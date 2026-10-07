use support::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use two_plz::Tunnel;

struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn request() -> Request {
    Request::builder()
        .method(Method::CONNECT)
        .extension("websocket".into())
        .uri(build_test_uri())
        .build()
}

#[tokio::test]
async fn reset_tunnel_does_not_interrupt_sibling_tunnel_or_http_request() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (client, server) = tokio::join!(
            ClientBuilder::new().handshake(client_io),
            ServerBuilder::new()
                .enable_connect_protocol()
                .handshake(server_io),
        );
        let (mut connection, mut sender) = client.unwrap();
        let mut server = server.unwrap();
        let (established, ready_to_reset) = tokio::sync::oneshot::channel();
        let (finished, client_finished) = tokio::sync::oneshot::channel();
        // Run acceptance concurrently with negotiation: the server must flush its settings.
        let server_task = tokio::spawn(async move {
            let (_, recv1, mut respond1) = server
                .accept_streaming()
                .await
                .unwrap()
                .unwrap();
            let send1 = respond1
                .send_response_streaming(build_test_response(), false)
                .unwrap();
            let tunnel1 = Tunnel::new(recv1, send1).unwrap();
            let (_, recv2, mut respond2) = server
                .accept_streaming()
                .await
                .unwrap()
                .unwrap();
            let send2 = respond2
                .send_response_streaming(build_test_response(), false)
                .unwrap();
            let mut tunnel2 = Tunnel::new(recv2, send2).unwrap();
            let (ordinary, mut ordinary_body, mut respond3) = server
                .accept_streaming()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ordinary.method(), &Method::GET);
            assert!(ordinary_body.frame().await.is_none());
            let _driver = Driver(tokio::spawn(async move {
                let _ = poll_fn(|cx| server.poll_closed(cx)).await;
            }));
            ready_to_reset.await.unwrap();
            drop(tunnel1);
            // An ordinary response must complete while the surviving tunnel remains open.
            let mut response = build_test_response();
            response.set_body(BytesMut::from(&b"ordinary response"[..]));
            respond3
                .send_response(response)
                .unwrap();
            let mut data = Vec::new();
            tunnel2
                .read_to_end(&mut data)
                .await
                .unwrap();
            assert_eq!(data, b"surviving tunnel");
            tunnel2
                .write_all(b"reply")
                .await
                .unwrap();
            tunnel2.shutdown().await.unwrap();
            client_finished.await.unwrap();
        });
        poll_fn(|cx| {
            assert!(
                std::pin::Pin::new(&mut connection)
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
        let _client_driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let (response1, send1) = sender
            .send_request_streaming(request(), false)
            .unwrap();
        let (response2, send2) = sender
            .send_request_streaming(request(), false)
            .unwrap();
        let ordinary = sender
            .send_request(build_test_request())
            .unwrap();
        let (_, recv1) = response1.await.unwrap();
        let mut tunnel1 = Tunnel::new(recv1, send1).unwrap();
        let (_, recv2) = response2.await.unwrap();
        let mut tunnel2 = Tunnel::new(recv2, send2).unwrap();
        established.send(()).unwrap();
        let head = ordinary.await.unwrap();
        assert_eq!(head.body_as_ref().unwrap().as_ref(), b"ordinary response");
        let mut byte = [0];
        assert!(tunnel1.read(&mut byte).await.is_err());
        tunnel2
            .write_all(b"surviving tunnel")
            .await
            .unwrap();
        tunnel2.shutdown().await.unwrap();
        let mut reply = Vec::new();
        tunnel2
            .read_to_end(&mut reply)
            .await
            .unwrap();
        assert_eq!(reply, b"reply");
        finished.send(()).unwrap();
        server_task.await.unwrap();
    })
    .await
    .expect("multiplexed CONNECT streams stalled");
}
