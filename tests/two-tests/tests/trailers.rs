use support::prelude::*;

#[tokio::test]
async fn recv_trailers_only() {
    support::trace_init!();

    let mock = mock_io::Builder::new()
        .client_handshake()
        // Write GET /
        .write(&[
            0, 0, 0x10, 1, 5, 0, 0, 0, 1, 0x82, 0x87, 0x41, 0x8B, 0x9D, 0x29,
            0xAC, 0x4B, 0x8F, 0xA8, 0xE9, 0x19, 0x97, 0x21, 0xE9, 0x84,
        ])
        // Read response
        .read(&[
            0, 0, 1, 1, 4, 0, 0, 0, 1, 0x88, 0, 0, 9, 1, 5, 0, 0, 0, 1, 0x40,
            0x84, 0x42, 0x46, 0x9B, 0x51, 0x82, 0x3F, 0x5F,
        ])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .unwrap();

    // Send the request
    let request = build_test_request();
    let resp_fut = client.send_request(request).unwrap();
    let resp: Response = conn.run(resp_fut).await.unwrap();
    let trailers = resp.trailers().unwrap();
    assert_eq!(trailers.iter().len(), 1);
    let mut map = HeaderMap::new();
    map.insert("status", "ok");
    assert_eq!(trailers, &map);
    conn.await.unwrap();
}

#[tokio::test]
async fn send_trailers_immediately() {
    support::trace_init!();

    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request();

        let mut trailers = HeaderMap::new();
        trailers.insert("zomg", "hello");
        request.set_trailers(trailers);

        tracing::info!("sending request with trailers");
        let response_fut = client.send_request(request).unwrap();

        let response = conn.drive(response_fut).await.unwrap();
        assert_eq!(response.status(), &StatusCode::OK);
        assert_eq!(response.body_as_ref(), Some(&"hello world"[..].into()));
        assert!(response.trailers().is_none());
        idle_ms(100).await;
        conn.await.unwrap();
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(frames::headers(1).request(
            "GET",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        srv.recv_frame(
            frames::headers(1)
                .field("zomg", "hello") // expected trailer
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, b"hello world").eos())
            .await;
        idle_ms(100).await;
    };

    join(srv_fut, client_fut).await;
}
