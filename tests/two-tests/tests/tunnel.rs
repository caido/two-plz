use support::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use two_plz::Tunnel;

struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn pair() -> (Tunnel, Tunnel, Driver, Driver) {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (client, server) = tokio::join!(
        ClientBuilder::new().handshake(client_io),
        ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(server_io),
    );
    let (mut connection, mut sender) = client.unwrap();
    let mut server = server.unwrap();
    poll_fn(|cx| {
        assert!(
            Pin::new(&mut connection)
                .poll(cx)
                .is_pending()
        );
        if connection.is_extended_connect_protocol_enabled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    let client_driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = Request::builder()
        .method(Method::CONNECT)
        .extension("websocket".into())
        .uri(build_test_uri())
        .build();
    let (response, send) = sender
        .send_request_streaming(request, false)
        .unwrap();
    let (request, recv, mut respond) = server
        .accept_streaming()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.method(), &Method::CONNECT);
    let server_send = respond
        .send_response_streaming(build_test_response(), false)
        .unwrap();
    let server_tunnel = Tunnel::new(recv, server_send).unwrap();
    let server_driver = Driver(tokio::spawn(async move {
        let _ = poll_fn(|cx| server.poll_closed(cx)).await;
    }));
    let (response, recv) = response.await.unwrap();
    assert_eq!(response.status(), &StatusCode::OK);
    (
        Tunnel::new(recv, send).unwrap(),
        server_tunnel,
        client_driver,
        server_driver,
    )
}

#[tokio::test]
async fn bidirectional_data_and_half_close() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, server, _client_driver, _server_driver) = pair().await;
        let (mut client_read, mut client_write) = tokio::io::split(client);
        let (mut server_read, mut server_write) = tokio::io::split(server);
        let request = vec![0x31; 256 * 1024];
        let response = vec![0x72; 256 * 1024];
        let send_request = async {
            client_write
                .write_all(&request)
                .await
                .unwrap();
            client_write.shutdown().await.unwrap();
            assert!(
                client_write
                    .write_all(b"after shutdown")
                    .await
                    .is_err()
            );
        };
        let receive_request = async {
            let mut received = Vec::new();
            server_read
                .read_to_end(&mut received)
                .await
                .unwrap();
            assert_eq!(received, request);
            // The other direction remains usable after receiving END_STREAM.
            server_write
                .write_all(&response)
                .await
                .unwrap();
            server_write.shutdown().await.unwrap();
        };
        let receive_response = async {
            let mut received = Vec::new();
            client_read
                .read_to_end(&mut received)
                .await
                .unwrap();
            assert_eq!(received, response);
        };
        tokio::join!(send_request, receive_request, receive_response);
    })
    .await
    .expect("tunnel stalled");
}

#[tokio::test]
async fn reset_wakes_pending_read_and_write() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client, mut server, _client_driver, _server_driver) =
            pair().await;
        let (mut read, mut write) = tokio::io::split(client);
        let reader = async {
            let mut byte = [0];
            assert!(read.read(&mut byte).await.is_err());
        };
        let writer = async {
            assert!(
                write
                    .write_all(&vec![1; 1024 * 1024])
                    .await
                    .is_err()
            );
        };
        let reset = async {
            tokio::task::yield_now().await;
            server.send_reset(Reason::CANCEL);
        };
        tokio::join!(reader, writer, reset);
    })
    .await
    .expect("reset did not wake tunnel");
}

#[tokio::test]
async fn small_reads_preserve_data_and_empty_reads_do_not_consume() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut client, mut server, _client_driver, _server_driver) =
            pair().await;
        server
            .write_all(b"abcdef")
            .await
            .unwrap();
        server.shutdown().await.unwrap();
        assert_eq!(client.read(&mut []).await.unwrap(), 0);
        let mut received = Vec::new();
        let mut small = [0; 2];
        loop {
            let len = client.read(&mut small).await.unwrap();
            if len == 0 {
                break;
            }
            received.extend_from_slice(&small[..len]);
        }
        assert_eq!(received, b"abcdef");
        client.shutdown().await.unwrap();
    })
    .await
    .expect("small reads stalled");
}

#[tokio::test]
async fn dropping_tunnel_cancels_peer() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut client, server, _client_driver, _server_driver) =
            pair().await;
        drop(server);
        let mut byte = [0];
        assert!(client.read(&mut byte).await.is_err());
        assert!(
            client
                .write_all(b"closed")
                .await
                .is_err()
        );
    })
    .await
    .expect("drop did not cancel tunnel");
}
