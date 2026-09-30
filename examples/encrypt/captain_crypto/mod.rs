mod ca;
pub mod error;
mod verifier;
use std::sync::Arc;

use ca::*;
use error::*;
use rcgen::{CertificateParams, KeyPair};
use rustls::KeyLogFile;
use rustls_pki_types::PrivateKeyDer;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::client::WebPkiServerVerifier;
use tokio_rustls::rustls::client::danger::ServerCertVerifier;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::{
    ClientConfig, RootCertStore, ServerConfig, {self},
};
use tracing::trace;
use verifier::*;

mod private_key;
use private_key::{read_private, str_to_private};
use webpki_roots::TLS_SERVER_ROOTS;

const ALPN_H1: &[u8] = b"http/1.1";
const ALPN_H2: &[u8] = b"h2";

pub struct CaptainCrypto {
    connector: Arc<TlsConnector>,
    key_pair: KeyPair,
    private_key: PrivateKeyDer<'static>,
    trusted_ca: CA,
    untrusted_ca: CA,
    web_pki: Arc<WebPkiServerVerifier>,
}

impl CaptainCrypto {
    pub fn new() -> Result<Self, CryptoBuildError> {
        // 1
        let root_cert_store =
            RootCertStore::from_iter(TLS_SERVER_ROOTS.iter().cloned());
        let web_pki =
            WebPkiServerVerifier::builder(root_cert_store.into()).build()?;

        // 2
        let supported_verify_schemes = web_pki.supported_verify_schemes();
        let verifier = CertVerifier::new(supported_verify_schemes);
        let mut client_config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        client_config.alpn_protocols =
            vec![ALPN_H1.to_vec(), ALPN_H2.to_vec()];
        client_config.key_log = Arc::new(KeyLogFile::new());
        let tls_connector = TlsConnector::from(Arc::new(client_config));
        let connector = Arc::new(tls_connector);

        let pk_str = read_private()?;
        let key_pair = KeyPair::from_pem(&pk_str)?;
        let trusted_ca = CA::trusted(&key_pair)?;
        let untrusted_ca = CA::untrusted(&key_pair)?;
        let private_key = str_to_private(&pk_str)?;
        Ok(CaptainCrypto {
            connector,
            key_pair,
            private_key,
            trusted_ca,
            untrusted_ca,
            web_pki,
        })
    }

    pub fn get_connector(&self) -> Arc<TlsConnector> {
        self.connector.clone()
    }

    pub fn get_verifier(&self) -> Arc<WebPkiServerVerifier> {
        self.web_pki.clone()
    }

    /* Description:
     *      Check if a certificate already exists in the selected store.
     *
     * Steps:
     *      Select the Cert Store and Search,
     *          verified == true =>  Trusted
     *          verified == false => Untrusted
     */

    pub fn check_serial(
        &self,
        verified: bool,
        digest_to_check: &[u8],
    ) -> Option<Arc<ServerConfig>> {
        let cert_store = if verified {
            trace!("trusted");
            &self.trusted_ca.store()
        } else {
            trace!("untrusted");
            &self.untrusted_ca.store()
        };
        cert_store
            .iter()
            .find_map(|(digest, config)| {
                if digest.as_slice() == digest_to_check {
                    Some(config.clone())
                } else {
                    None
                }
            })
    }

    /* Description:
     *      Generate new certificate based on verification result from web_pki.
     *
     * Steps:
     *      1. Select CA based on verification result
     *          verified == true =>  Trusted
     *          verified == false => Untrusted
     *      2. Generate new domain cert using server cert and CA cert.
     *      3. Generate Server Config using generated cert and private key
     *      4. Push to the selected store
     *
     * Returns:
     *      Result<Arc<ServerConfig>, CertError>
     *
     * Error:
     *      CertError::Rcgen    [2]
     *      CertError::Rustls   [4]
     */

    pub fn generate_new_cert(
        &mut self,
        verified: bool,
        cert: Vec<CertificateDer<'static>>,
    ) -> Result<Arc<ServerConfig>, CertError> {
        // 1. Select CA based on verification result
        let ca = if verified {
            trace!("new cert| ca| Y");
            &mut self.trusted_ca
        } else {
            trace!("new cert| ca| N");
            &mut self.untrusted_ca
        };
        // 2. Generate new domain cert using server cert and CA cert.
        let gen_cert = generate_domain_cert(&self.key_pair, cert, ca.cert())?;

        // 3. Generate Server Config
        let config =
            generate_server_config(gen_cert, self.private_key.clone_key())?;
        trace!("server config| Y");

        // 4. Push to the selected store
        let arc_config = Arc::new(config);
        let tosend = arc_config.clone();

        Ok(tosend)
    }
}

pub fn generate_domain_cert(
    keypair: &KeyPair,
    cert: Vec<CertificateDer<'static>>,
    signer: &rcgen::Certificate,
) -> Result<CertificateDer<'static>, rcgen::Error> {
    let cert_params = CertificateParams::from_ca_cert_der(&cert[0])?;
    trace!("certificate params built");
    let certificate: CertificateDer<'static> = cert_params
        .signed_by(keypair, signer, keypair)?
        .into();
    Ok(certificate)
}

pub fn generate_server_config(
    cert: CertificateDer<'static>,
    private_key: PrivateKeyDer<'static>,
) -> Result<ServerConfig, rustls::Error> {
    let certs = vec![cert];
    let mut server_conf = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)?;
    server_conf.alpn_protocols = vec![ALPN_H2.to_vec(), ALPN_H1.to_vec()];
    Ok(server_conf)
}
