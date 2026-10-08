use std::collections::HashMap;
use std::fs::read_to_string;
use std::sync::Arc;

use rcgen::{Certificate, CertificateParams, KeyPair};
use tokio_rustls::rustls::ServerConfig;

use super::CryptoBuildError;

/* Description:
 *      CA certificates generate self-signed TLS certificates for domains.
 *
 * There are two CAs:
 *      1. Trusted      : user generated and trusted
 *      2. Untrusted    : per session generated
 */

pub struct CA {
    cert: Certificate,
    store: HashMap<Vec<u8>, Arc<ServerConfig>>,
}

impl CA {
    pub fn trusted(key_pair: &KeyPair) -> Result<CA, CryptoBuildError> {
        let mut cert_path = std::env::current_dir()?;
        cert_path.push("./examples/ca/ca.crt");
        let cert_str = read_to_string(cert_path)?;
        let cert_params = CertificateParams::from_ca_cert_pem(&cert_str)?;
        let cert = cert_params.self_signed(key_pair)?;
        Ok(CA {
            cert,
            store: HashMap::new(),
        })
    }

    // Per session CA Certificate
    pub fn untrusted(key_pair: &KeyPair) -> Result<CA, CryptoBuildError> {
        let cert_params = CertificateParams::default();
        let cert = cert_params.self_signed(key_pair)?;
        Ok(CA {
            cert,
            store: HashMap::new(),
        })
    }

    pub fn cert(&self) -> &Certificate {
        &self.cert
    }

    pub fn store(&self) -> &HashMap<Vec<u8>, Arc<ServerConfig>> {
        &self.store
    }

    pub fn add_config(&mut self, digest: Vec<u8>, config: Arc<ServerConfig>) {
        self.store
            .entry(digest)
            .or_insert(config);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca() -> CA {
        let key_pair = KeyPair::generate().expect("generate test CA key pair");
        let cert = CertificateParams::default()
            .self_signed(&key_pair)
            .expect("generate test CA certificate");
        CA {
            cert,
            store: HashMap::new(),
        }
    }

    fn config() -> Arc<ServerConfig> {
        let key_pair =
            KeyPair::generate().expect("generate test server key pair");
        let cert = CertificateParams::default()
            .self_signed(&key_pair)
            .expect("generate test server certificate");
        let private_key =
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                key_pair.serialize_der().into(),
            );
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], private_key)
            .expect("build test server config");
        Arc::new(config)
    }

    #[test]
    fn config_store_looks_up_digest_and_returns_none_for_miss() {
        let mut ca = ca();
        let config = config();
        ca.add_config(b"known".to_vec(), config.clone());

        assert!(Arc::ptr_eq(
            ca.store()
                .get(b"known".as_slice())
                .expect("stored config"),
            &config
        ));
        assert!(
            ca.store()
                .get(b"missing".as_slice())
                .is_none()
        );
    }

    #[test]
    fn duplicate_digest_keeps_first_config() {
        let mut ca = ca();
        let first = config();
        let second = config();
        ca.add_config(b"same".to_vec(), first.clone());
        ca.add_config(b"same".to_vec(), second);

        assert_eq!(ca.store().len(), 1);
        assert!(Arc::ptr_eq(
            ca.store()
                .get(b"same".as_slice())
                .expect("stored config"),
            &first
        ));
    }

    #[test]
    fn separate_ca_stores_do_not_share_configs() {
        let mut trusted = ca();
        let mut untrusted = ca();
        let trusted_config = config();
        trusted.add_config(b"digest".to_vec(), trusted_config.clone());

        assert!(Arc::ptr_eq(
            trusted
                .store()
                .get(b"digest".as_slice())
                .expect("trusted config"),
            &trusted_config
        ));
        assert!(
            untrusted
                .store()
                .get(b"digest".as_slice())
                .is_none()
        );
        untrusted.add_config(b"digest".to_vec(), config());
        assert_eq!(trusted.store().len(), 1);
        assert_eq!(untrusted.store().len(), 1);
        assert!(!Arc::ptr_eq(
            trusted
                .store()
                .get(b"digest".as_slice())
                .expect("trusted config"),
            untrusted
                .store()
                .get(b"digest".as_slice())
                .expect("untrusted config")
        ));
    }
}
