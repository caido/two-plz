use support::prelude::*;

#[tokio::test]
async fn single_stream_send_large_body() {
    support::trace_init!();
    let payload = vec![0; 1024];

    let mock = mock_io::Builder::new()
        .client_handshake()
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75,
            143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(&[
            // DATA
            0, 4, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[..])
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .expect("handshake");

    let mut request = build_test_request_post("http2.akamai.com");
    request.set_body(BytesMut::from(&payload[..]));
    let resp_fut = client.send_request(request).unwrap();
    let resp = conn.drive(resp_fut).await.unwrap();
    assert_eq!(resp.status(), &StatusCode::NO_CONTENT);

    conn.await.unwrap();
}

#[tokio::test]
async fn multiple_streams_with_payload_greater_than_default_window() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let payload = vec![0u8; 16384 * 5 - 1]; // 81,919 bytes

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        // Receive headers for all 3 streams
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;
        srv.recv_frame(
            frames::headers(3)
                .request("POST", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.recv_frame(
            frames::headers(5)
                .request("POST", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        // Receive stream 1 data in chunks (exhausts connection window)
        srv.recv_frame(frames::data(1, &payload[0..16_384]))
            .await;
        srv.recv_frame(frames::data(1, &payload[16_384..(16_384 * 2)]))
            .await;
        srv.recv_frame(frames::data(1, &payload[(16_384 * 2)..(16_384 * 3)]))
            .await;
        srv.recv_frame(frames::data(
            1,
            &payload[(16_384 * 3)..(16_384 * 4 - 1)],
        ))
        .await;

        // Send settings (can be empty)
        srv.send_frame(frames::settings()).await;
        srv.recv_frame(frames::settings_ack())
            .await;

        // Send responses for all streams
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
        srv.send_frame(frames::headers(3).response(200).eos())
            .await;
        srv.send_frame(frames::headers(5).response(200).eos())
            .await;
        idle_ms(0).await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // Create request 1 with large body
        let mut request1 = build_test_request_post("http2.akamai.com");
        request1.set_body(BytesMut::from(&vec![0u8; 16384 * 5 - 1][..]));

        // Create requests 2 and 3 with no body
        let mut request2 = build_test_request_post("http2.akamai.com");
        let _ = request2.take_body();

        let mut request3 = build_test_request_post("http2.akamai.com");
        let _ = request3.take_body();

        let resp_fut1 = client.send_request(request1).unwrap();
        let resp_fut2 = client.send_request(request2).unwrap();
        let resp_fut3 = client.send_request(request3).unwrap();

        let (_, r1, r2, r3) = join4(
            async move { conn.await.expect("client") },
            async move {
                let resp = resp_fut1.await.expect("response1");
                assert_eq!(resp.status(), &StatusCode::OK);
                resp
            },
            async move {
                let resp = resp_fut2.await.expect("response2");
                assert_eq!(resp.status(), &StatusCode::OK);
                resp
            },
            async move {
                let resp = resp_fut3.await.expect("response3");
                assert_eq!(resp.status(), &StatusCode::OK);
                resp
            },
        )
        .await;

        (r1, r2, r3)
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn single_stream_send_extra_large_body_multi_frames_one_buffer() {
    support::trace_init!();
    let payload = vec![0; 32_768];

    let mock = mock_io::Builder::new()
        .client_handshake()
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75,
            143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(&[
            // DATA
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[0..16_384])
        .write(&[
            // DATA
            0, 64, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[16_384..])
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .expect("handshake");

    let mut request = build_test_request_post("http2.akamai.com");
    request.set_body(BytesMut::from(&vec![0u8; 32_768][..]));
    let resp_fut = client.send_request(request).unwrap();
    let resp = conn.drive(resp_fut).await.unwrap();
    assert_eq!(resp.status(), &StatusCode::NO_CONTENT);

    conn.await.unwrap();
}

#[tokio::test]
async fn single_stream_send_body_greater_than_default_window() {
    support::trace_init!();

    let payload = vec![0u8; 16384 * 5 - 1]; // 81,919 bytes

    let mock = mock_io::Builder::new()
        .client_handshake()
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75,
            143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(&[
            // DATA frame 1 (16,384 bytes)
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[0..16_384])
        .write(&[
            // DATA frame 2 (16,384 bytes)
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[16_384..(16_384 * 2)])
        .write(&[
            // DATA frame 3 (16,384 bytes)
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[(16_384 * 2)..(16_384 * 3)])
        .write(&[
            // DATA frame 4 (16,383 bytes - exhausts 65,535 byte window)
            0, 63, 255, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[(16_384 * 3)..(16_384 * 4 - 1)])
        // Read WINDOW_UPDATE for connection (stream 0)
        .read(&[0, 0, 4, 8, 0, 0, 0, 0, 0, 0, 0, 64, 0])
        // Read WINDOW_UPDATE for stream 1
        .read(&[0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 64, 0])
        .write(&[
            // DATA frame 5 (16,384 bytes with END_STREAM)
            0, 64, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[(16_384 * 4 - 1)..(16_384 * 5 - 1)])
        // Read response: 204 NO_CONTENT with END_STREAM
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .expect("handshake");

    let mut request = build_test_request_post("http2.akamai.com");
    request.set_body(BytesMut::from(&payload[..]));

    let resp_fut = client.send_request(request).unwrap();
    let resp = conn.drive(resp_fut).await.unwrap();

    assert_eq!(resp.status(), &StatusCode::NO_CONTENT);

    conn.await.unwrap();
}

#[tokio::test]
async fn single_stream_send_extra_large_body_multi_frames_multi_buffer() {
    support::trace_init!();
    let payload = vec![0u8; 32_768];

    let mock = mock_io::Builder::new()
        .client_handshake()
        .wait(Duration::from_millis(10))
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75,
            143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(&[
            // DATA frame 1
            0, 64, 0, 0, 0, 0, 0, 0, 1,
        ])
        .write(&payload[0..16_384])
        .wait(Duration::from_millis(10))
        .write(&[
            // DATA frame 2 with END_STREAM
            0, 64, 0, 0, 1, 0, 0, 0, 1,
        ])
        .write(&payload[16_384..])
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .expect("handshake");

    let mut request = build_test_request_post("http2.akamai.com");
    request.set_body(BytesMut::from(&payload[..]));

    let resp_fut = client.send_request(request).unwrap();
    let resp = conn.drive(resp_fut).await.unwrap();

    assert_eq!(resp.status(), &StatusCode::NO_CONTENT);

    conn.await.unwrap();
}

#[tokio::test]
async fn send_data_receive_window_update() {
    support::trace_init!();
    let (m, mut mock) = mock::new();

    let srv_fut = async move {
        let settings = mock.assert_client_handshake().await;
        assert_default_settings!(settings);

        let frame = mock.next().await.unwrap();
        let request = assert_headers!(frame.unwrap());
        assert!(!request.is_end_stream());
        let frame = mock.next().await.unwrap();
        let data = assert_data!(frame.unwrap());

        // Update the windows
        let len = data.payload().len();
        let f = frame::WindowUpdate::new(StreamId::zero(), len as u32);
        mock.send(f.into()).await.unwrap();

        let f = frame::WindowUpdate::new(data.stream_id(), len as u32);
        mock.send(f.into()).await.unwrap();

        //

        for _ in 0..2usize {
            let frame = mock.next().await.unwrap();
            let data = assert_data!(frame.unwrap());
            assert_eq!(
                data.payload().len(),
                (frame::DEFAULT_MAX_FRAME_SIZE) as usize
            );
        }

        //
        let frame = mock.next().await.unwrap();
        let data = assert_data!(frame.unwrap());
        assert_eq!(
            data.payload().len(),
            (frame::DEFAULT_MAX_FRAME_SIZE - 1) as usize
        );

        //
        let frame = mock.next().await.unwrap();
        let data = assert_data!(frame.unwrap());
        assert_eq!(data.payload().len(), 5,);
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(m)
            .await
            .expect("handshake");

        // Send initial small body
        let mut request1 = build_test_request_post("http2.akamai.com");
        let mut body = BytesMut::from(&b"hello"[..]);
        let payload = vec![0; frame::DEFAULT_INITIAL_WINDOW_SIZE as usize];
        body.extend_from_slice(&payload[..]);
        request1.set_body(body);
        let resp1 = client.send_request(request1).unwrap();

        std::mem::forget(resp1);
        conn.await.expect("client");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn stream_count_over_max_stream_limit_does_not_starve_capacity() {
    use tokio::sync::oneshot;
    support::trace_init!();

    let (io, mut srv) = mock::new();
    let (tx, rx) = oneshot::channel();

    let srv_fut = async move {
        let _ = srv
            .assert_client_handshake_with_settings(
                frames::settings().max_concurrent_streams(1),
            )
            .await;

        // Stream 1: Receive full body
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;
        srv.recv_frame(frames::data(1, vec![0; 16384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0; 16384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0; 16384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0; 16383]).eos())
            .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        // Grant connection capacity - should go to stream 3
        srv.send_frame(frames::window_update(0, 16384))
            .await;
        srv.send_frame(frames::window_update(0, 16384))
            .await;
        srv.send_frame(frames::window_update(0, 16384))
            .await;
        srv.send_frame(frames::window_update(0, 16383))
            .await;

        // Stream 3 should immediately send with the new capacity
        srv.recv_frame(frames::headers(3).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;
        srv.recv_frame(frames::data(3, vec![0; 16384]))
            .await;
        srv.recv_frame(frames::data(3, vec![0; 16384]))
            .await;
        srv.recv_frame(frames::data(3, vec![0; 16384]))
            .await;
        srv.recv_frame(frames::data(3, vec![0; 16383]).eos())
            .await;
        srv.send_frame(frames::headers(3).response(200).eos())
            .await;

        tx.send(()).unwrap();
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // Stream 1: Exhaust connection window
        let mut req1 = build_test_request_post("example.com");
        req1.set_body(BytesMut::from(&vec![0u8; 65535][..]));
        let resp1_fut = client.send_request(req1).unwrap();

        // Stream 3: Queue up for connection window
        let mut req2 = build_test_request_post("example.com");
        req2.set_body(BytesMut::from(&vec![0u8; 65535][..]));
        let resp2_fut = client.send_request(req2).unwrap();

        // Queue 5 more streams waiting to open
        for _ in 0..5 {
            let mut req = build_test_request_post("example.com");
            req.set_body(BytesMut::from(&vec![0u8; 65535][..]));
            client.send_request(req).unwrap();
        }

        let resp1 = conn.drive(resp1_fut).await.unwrap();
        assert_eq!(resp1.status(), &StatusCode::OK);

        let resp2 = conn.drive(resp2_fut).await.unwrap();
        assert_eq!(resp2.status(), &StatusCode::OK);

        rx.await.unwrap();
    };

    tokio::time::timeout(Duration::from_secs(5), join(srv_fut, client_fut))
        .await
        .expect("test should not timeout");
}
