use futures_util::SinkExt;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName,
};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;
use support::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::protocol::{Message, Role},
};
use two_plz::Tunnel;

struct Driver(tokio::task::JoinHandle<()>);

impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn tls_configs() -> (TlsAcceptor, TlsConnector) {
    let certified =
        generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate: CertificateDer<'static> = certified.cert.der().clone();
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified.key_pair.serialize_der(),
    ));

    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], private_key)
        .unwrap();
    server_config.alpn_protocols = vec![b"h2".to_vec()];

    let mut roots = RootCertStore::empty();
    roots.add(certificate).unwrap();
    let mut client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![b"h2".to_vec()];

    (
        TlsAcceptor::from(Arc::new(server_config)),
        TlsConnector::from(Arc::new(client_config)),
    )
}

fn websocket_request() -> Request {
    let mut headers = HeaderMap::new();
    headers.insert("sec-websocket-version", "13");
    Request::builder()
        .method(Method::CONNECT)
        .extension("websocket".into())
        .uri(Uri::try_from("https://localhost/chat").unwrap())
        .headers(headers)
        .build()
}

#[tokio::test]
async fn websocket_messages_flow_over_tls_http2_tunnel() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (acceptor, connector) = tls_configs();

        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(tcp).await.unwrap();
            assert_eq!(
                tls.get_ref().1.alpn_protocol(),
                Some(b"h2".as_slice())
            );

            let mut server = ServerBuilder::new()
                .enable_connect_protocol()
                .handshake(tls)
                .await
                .unwrap();
            let (request, recv, mut respond) = server
                .accept_streaming()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.method(), &Method::CONNECT);
            let send = respond
                .send_response_streaming(build_test_response(), false)
                .unwrap();
            let tunnel = Tunnel::new(recv, send).unwrap();
            let mut websocket =
                WebSocketStream::from_raw_socket(tunnel, Role::Server, None)
                    .await;
            let _driver = Driver(tokio::spawn(async move {
                let _ = poll_fn(|cx| server.poll_closed(cx)).await;
            }));

            assert_eq!(
                websocket.next().await.unwrap().unwrap(),
                Message::text("hello over h2")
            );
            assert_eq!(
                websocket.next().await.unwrap().unwrap(),
                Message::Ping(Bytes::from_static(b"check"))
            );
            websocket
                .send(Message::Binary(Bytes::from_static(b"binary reply")))
                .await
                .unwrap();

            assert!(matches!(
                websocket.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            websocket.flush().await.unwrap();

            let mut tunnel = websocket.into_inner();
            tunnel.shutdown().await.unwrap();
            let mut trailing_data = Vec::new();
            tunnel
                .read_to_end(&mut trailing_data)
                .await
                .unwrap();
            assert!(trailing_data.is_empty());
        });

        let tcp = TcpStream::connect(address)
            .await
            .unwrap();
        let tls = connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));

        let (mut connection, mut sender) = ClientBuilder::new()
            .handshake(tls)
            .await
            .unwrap();
        poll_fn(|cx| {
            let result = Pin::new(&mut connection).poll(cx);
            if connection.is_extended_connect_protocol_enabled() {
                Poll::Ready(())
            } else {
                assert!(result.is_pending());
                Poll::Pending
            }
        })
        .await;
        let _driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));

        let (response, send) = sender
            .send_request_streaming(websocket_request(), false)
            .unwrap();
        let (response, recv) = response.await.unwrap();
        assert_eq!(response.status(), &StatusCode::OK);
        let tunnel = Tunnel::new(recv, send).unwrap();
        let mut websocket =
            WebSocketStream::from_raw_socket(tunnel, Role::Client, None).await;

        websocket
            .send(Message::text("hello over h2"))
            .await
            .unwrap();
        websocket
            .send(Message::Ping(Bytes::from_static(b"check")))
            .await
            .unwrap();
        let mut saw_pong = false;
        let mut saw_binary = false;
        for _ in 0..2 {
            match websocket.next().await.unwrap().unwrap() {
                Message::Pong(payload) => {
                    assert_eq!(payload, Bytes::from_static(b"check"));
                    saw_pong = true;
                }
                Message::Binary(payload) => {
                    assert_eq!(payload, Bytes::from_static(b"binary reply"));
                    saw_binary = true;
                }
                message => panic!("unexpected WebSocket message: {message:?}"),
            }
        }
        assert!(saw_pong);
        assert!(saw_binary);

        websocket
            .send(Message::Close(None))
            .await
            .unwrap();
        assert!(matches!(
            websocket.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));

        let mut tunnel = websocket.into_inner();
        tunnel.shutdown().await.unwrap();
        let mut trailing_data = Vec::new();
        tunnel
            .read_to_end(&mut trailing_data)
            .await
            .unwrap();
        assert!(trailing_data.is_empty());

        server_task.await.unwrap();
    })
    .await
    .expect("TLS WebSocket over HTTP/2 stalled");
}
