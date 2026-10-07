use support::prelude::*;
use tokio::sync::oneshot;

#[tokio::test]
async fn recv_single_ping() {
    support::trace_init!();
    let (io, mut handle) = mock::new();

    let h2 = async move {
        let (conn, _hand) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();
        let _: () = conn.await.unwrap();
    };

    let mock = async move {
        let _ = handle.assert_client_handshake().await;
        let frame = frame::Ping::new(Default::default());
        handle.send(frame.into()).await.unwrap();
        let frame = handle.next().await.unwrap();
        let pong = assert_ping!(frame.unwrap());
        // Payload is correct
        assert_eq!(*pong.payload(), <[u8; 8]>::default());
        // Is ACK
        assert!(pong.is_ack());
    };

    join(mock, h2).await;
}

#[tokio::test]
async fn recv_multiple_pings() {
    support::trace_init!();
    let (mock, mut handle) = mock::new();

    let client = async move {
        let settings = handle.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        handle
            .send_frame(frames::ping([1; 8]))
            .await;
        handle
            .send_frame(frames::ping([2; 8]))
            .await;
        handle
            .recv_frame(frames::ping([1; 8]).pong())
            .await;
        handle
            .recv_frame(frames::ping([2; 8]).pong())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn pong_has_highest_priority() {
    support::trace_init!();
    let (io, mut client) = mock::new();

    let data = Bytes::from(vec![0; 16_384]);
    let data_clone = data.clone();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(frames::headers(1).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        client
            .send_frame(frames::data(1, data_clone).eos())
            .await;
        client
            .send_frame(frames::ping([1; 8]))
            .await;
        client
            .recv_frame(frames::ping([1; 8]).pong())
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");
        let (req, mut stream) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method().as_str(), "POST");
        assert_eq!(req.body_as_ref().unwrap().len(), data.len());

        let res = Response::builder()
            .status(200)
            .build()
            .unwrap();
        stream
            .send_response(res)
            .expect("response");
        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn user_notifies_when_connection_closes() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        srv
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let resp_fut = client
            .send_request(build_test_request())
            .unwrap();
        poll_once(&mut conn).await.unwrap();
        (conn, resp_fut)
    };

    let (srv_handle, (conn, resp_fut)) = join(srv_fut, client_fut).await;

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let res = resp_fut.await;
        tx.send(res).unwrap();
    });

    idle_ms(50).await;
    drop(conn);
    drop(srv_handle);

    let result = rx.await.unwrap();
    assert!(result.is_err());
}
