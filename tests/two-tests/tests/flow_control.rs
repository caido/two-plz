use support::prelude::*;
use tokio::sync::oneshot;

// skipped
// recv_data_overflows_connection_window
// recv_data_overflows_stream_window
// stream_close_by_data_frame_releases_capacity
// stream_close_by_trailers_frame_releases_capacity
// settings_lowered_capacity_returns_capacity_to_connection
// client_increase_target_window_size
// increase_target_window_size_after_using_some
// decrease_target_window_size
// client_update_initial_window_size
// client_decrease_initial_window_size
// server_target_window_size
// reserve_capacity_after_peer_closes
// poll_capacity_after_send_data_and_reserve
// poll_capacity_after_send_data_and_reserve_with_max_send_buffer_size
// max_send_buffer_size_overflow
// max_send_buffer_size_poll_capacity_wakes_task
// poll_capacity_wakeup_after_window_update
// reclaim_reserved_capacity

#[tokio::test]
async fn send_data_without_requesting_capacity() {
    support::trace_init!();

    let payload = vec![0; 1024];

    let mock = mock_io::Builder::new()
        .handshake()
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
        .unwrap();

    let mut request = build_test_request_post("http2.akamai.com");
    request.set_body(BytesMut::zeroed(1024));
    let resp = client.send_request(request).unwrap();

    let resp = conn.run(resp).await.unwrap();

    assert_eq!(resp.status(), &StatusCode::NO_CONTENT);
    conn.await.unwrap();
}

#[tokio::test]
async fn release_capacity_sends_window_update() {
    support::trace_init!();
    let payload = vec![0u8; 16_384];
    let payload_len = payload.len();

    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let resp = conn.drive(resp).await.unwrap();
        assert_eq!(resp.body_as_ref().unwrap().len(), payload_len * 4)
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, &payload[..]))
            .await;
        srv.send_frame(frames::data(1, &payload[..]))
            .await;
        srv.send_frame(frames::data(1, &payload[..]))
            .await;
        srv.recv_frame(frames::window_update(0, 32_768))
            .await;
        srv.recv_frame(frames::window_update(1, 32_768))
            .await;
        srv.send_frame(frames::data(1, &payload[..]).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn release_capacity_of_small_amount_does_not_send_window_update() {
    support::trace_init!();
    let payload = [0u8; 16];

    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let resp = conn.drive(resp).await.unwrap();
        assert_eq!(resp.body_as_ref().unwrap().len(), 16)
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, &payload[..]).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn stream_error_release_connection_capacity() {
    support::trace_init!();

    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let resp = conn.drive(resp).await;
        assert!(resp.is_err());
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        // we're sending the wrong content-length
        srv.send_frame(
            frames::headers(1)
                .response(200)
                .field("content-length", &*(16_384 * 3).to_string()),
        )
        .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]))
            .await;
        srv.send_frame(frames::data(1, vec![0; 10]).eos())
            .await;
        srv.recv_frame(frames::window_update(0, 16_384 * 2))
            .await;
        srv.recv_frame(frames::window_update(1, 32768))
            .await;
        // mismatched content-length is a protocol error
        srv.recv_frame(frames::reset(1).protocol_error())
            .await;
        // but then the capacity should be released automatically
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn stream_close_by_send_reset_frame_releases_capacity() {
    support::trace_init!();

    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let resp = conn.drive(resp).await;
        assert!(resp.is_err());
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        // we're sending the wrong content-length
        srv.send_frame(
            frames::headers(1)
                .response(200)
                .field("content-length", &*(16_384 * 3).to_string()),
        )
        .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]))
            .await;
        srv.send_frame(frames::data(1, vec![0; 10]).eos())
            .await;
        srv.recv_frame(frames::window_update(0, 16_384 * 2))
            .await;
        srv.recv_frame(frames::window_update(1, 32768))
            .await;
        // mismatched content-length is a protocol error
        srv.recv_frame(frames::reset(1).protocol_error())
            .await;
        // but then the capacity should be released automatically
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_window_update_on_stream_closed_by_data_frame() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::from("hello"));

        let resp_fut = client.send_request(request).unwrap();

        // Wait for response
        let response = conn.drive(resp_fut).await.unwrap();
        assert_eq!(response.status(), &StatusCode::OK);

        // Keep response alive until after WINDOW_UPDATE is received
        // Poll connection to receive the WINDOW_UPDATE frame
        poll_once(&mut conn).await.unwrap();

        drop(response);
        drop(client);

        assert!(conn.await.is_ok());
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        // Receive request headers
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        srv.recv_frame(frames::data(1, "hello").eos())
            .await;

        // send window update after stream is closed
        srv.send_frame(frames::window_update(1, 5))
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn capacity_assigned_in_multi_window_updates() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("http2.akamai.com");
        let mut body =
            BytesMut::zeroed(frame::DEFAULT_INITIAL_WINDOW_SIZE as usize);
        body.extend_from_slice(b"helloworld");
        request.set_body(body);

        let resp_fut = client.send_request(request).unwrap();

        // Wait for response
        let response = conn.drive(resp_fut).await.unwrap();
        assert_eq!(response.status(), &StatusCode::OK);

        // Keep response alive until after WINDOW_UPDATE is received
        // Poll connection to receive the WINDOW_UPDATE frame
        poll_once(&mut conn).await.unwrap();

        drop(response);
        drop(client);

        assert!(conn.await.is_ok());
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        // Receive request headers
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_383]))
            .await;

        // Increase the connection window
        srv.send_frame(frames::window_update(0, 10))
            .await;
        // Incrementally increase the stream window
        srv.send_frame(frames::window_update(1, 4))
            .await;

        srv.send_frame(frames::window_update(1, 1))
            .await;
        // Receive first chunk
        srv.recv_frame(frames::data(1, "hello"))
            .await;
        srv.send_frame(frames::window_update(1, 5))
            .await;
        // Receive second chunk
        srv.recv_frame(frames::data(1, "world").eos())
            .await;

        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn connection_notified_on_released_capacity() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let resp_fut1 = client.send_request(request).unwrap();

        let request = build_test_request();
        let resp_fut2 = client.send_request(request).unwrap();

        let resp1 = conn.drive(resp_fut1).await.unwrap();
        assert_eq!(resp1.status(), &StatusCode::OK);

        let resp2 = conn.drive(resp_fut2).await.unwrap();
        assert_eq!(resp2.status(), &StatusCode::OK);

        assert_eq!(
            resp1.body_as_ref(),
            Some(BytesMut::zeroed(16_384)).as_ref()
        );
        assert_eq!(
            resp2.body_as_ref(),
            Some(BytesMut::zeroed(16_384)).as_ref()
        );

        drop(resp1);
        drop(resp2);

        poll_once(&mut conn).await.unwrap();

        drop(client);
        conn.await.unwrap();
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        // Receive request headers
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        // Send the first response
        srv.send_frame(frames::headers(1).response(200))
            .await;
        // Send the second response
        srv.send_frame(frames::headers(3).response(200))
            .await;

        // Fill the connection window
        srv.send_frame(frames::data(1, vec![0u8; 16_384]).eos())
            .await;
        idle_ms(100).await;
        srv.send_frame(frames::data(3, vec![0u8; 16_384]).eos())
            .await;

        // The window update is sent
        srv.recv_frame(frames::window_update(0, 32768))
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_settings_removes_available_capacity() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::from("hello world"));

        let resp_fut = client.send_request(request).unwrap();

        let response = conn.drive(resp_fut).await.unwrap();
        assert_eq!(response.status(), &StatusCode::NO_CONTENT);

        conn.await.unwrap();
    };

    let srv_fut = async move {
        let mut settings = frame::Settings::default();
        // Streams start with 0 capacity
        settings.set_initial_window_size(Some(0));

        let settings = srv
            .assert_client_handshake_with_settings(settings)
            .await;
        assert_default_settings!(settings);

        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        // send window update
        srv.send_frame(frames::window_update(0, 11))
            .await;
        srv.send_frame(frames::window_update(1, 11))
            .await;

        // recv data
        srv.recv_frame(frames::data(1, "hello world").eos())
            .await;

        srv.send_frame(frames::headers(1).response(204).eos())
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_settings_keeps_assigned_capacity() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let (sent_settings_tx, sent_settings_rx) = oneshot::channel();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::from("hello world"));

        let resp_fut = client.send_request(request).unwrap();

        // Poll once to send HEADERS
        poll_once(&mut conn).await.unwrap();

        // Wait for server to send SETTINGS
        sent_settings_rx.await.unwrap();

        // Poll to receive SETTINGS and send ACK
        poll_once(&mut conn).await.unwrap();

        let response = conn.drive(resp_fut).await.unwrap();
        assert_eq!(response.status(), &StatusCode::NO_CONTENT);
        conn.await.unwrap();
    };

    let srv_fut = async move {
        let mut initial_settings = frame::Settings::default();
        // intitial window size is set to 0
        initial_settings.set_initial_window_size(Some(0));

        let settings = srv
            .assert_client_handshake_with_settings(initial_settings)
            .await;
        assert_default_settings!(settings);

        // Receive request headers (no DATA yet, window is 0)
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        // Send SETTINGS increasing window to 64 bytes (from 0)
        srv.send_frame(frames::settings().initial_window_size(64))
            .await;
        sent_settings_tx.send(()).unwrap();
        srv.recv_frame(frames::settings_ack())
            .await;

        srv.recv_frame(frames::data(1, "hello world").eos())
            .await;

        srv.send_frame(frames::headers(1).response(204).eos())
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_no_init_window_then_receive_some_init_window() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        // Create request with 11 byte body
        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::from("hello world"));

        let resp_fut = client.send_request(request).unwrap();

        // Poll to send HEADERS (no DATA yet, window is 0)
        poll_once(&mut conn).await.unwrap();
        // Poll to receive SETTINGS(10) and send 10 bytes
        poll_once(&mut conn).await.unwrap();

        // Poll to receive SETTINGS(11) and send last 1 byte
        poll_once(&mut conn).await.unwrap();

        // Drive to completion
        let response = conn.drive(resp_fut).await.unwrap();
        assert_eq!(response.status(), &StatusCode::NO_CONTENT);

        conn.await.unwrap();
    };

    let srv_fut = async move {
        // Start with 0 window
        let mut initial_settings = frame::Settings::default();
        initial_settings.set_initial_window_size(Some(0));

        let settings = srv
            .assert_client_handshake_with_settings(initial_settings)
            .await;
        assert_default_settings!(settings);

        // Receive HEADERS (no DATA yet)
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        // Increase window to 10 bytes
        srv.send_frame(frames::settings().initial_window_size(10))
            .await;
        srv.recv_frame(frames::settings_ack())
            .await;

        // Client sends 10 bytes
        srv.recv_frame(frames::data(1, "hello worl"))
            .await;

        // Increase window to 11 bytes
        srv.send_frame(frames::settings().initial_window_size(11))
            .await;
        srv.recv_frame(frames::settings_ack())
            .await;

        // Client sends last byte
        srv.recv_frame(frames::data(1, "d").eos())
            .await;

        srv.send_frame(frames::headers(1).response(204).eos())
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_settings_increase_window_size_after_using_some() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let size = 16_384 * 4; // 65,536 bytes

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(size));
        let resp_fut = client.send_request(request).unwrap();
        let response = conn.drive(resp_fut).await.unwrap();
        assert_eq!(response.status(), &StatusCode::OK);
        conn.await.unwrap();
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        // Receive data 65,535
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_383]))
            .await;

        // Send SETTINGS to increase window
        srv.send_frame(frames::settings().initial_window_size(size as u32))
            .await;
        srv.recv_frame(frames::settings_ack())
            .await;

        // Grant connection capacity
        srv.send_frame(frames::window_update(0, 1))
            .await;

        // Client should send last byte without panicking
        srv.recv_frame(frames::data(1, vec![0u8; 1]).eos())
            .await;

        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn reset_stream_waiting_for_capacity() {
    // Tests that receiving a reset on a stream that has some available
    // connection-level window reassigns that window to another stream.
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request1 = build_test_request_post("example.com");
        request1.set_body(BytesMut::zeroed(65_535));
        let resp_fut1 = client.send_request(request1).unwrap();

        let mut request2 = build_test_request_post("example.com");
        request2.set_body(BytesMut::zeroed(1));
        let resp_fut2 = client.send_request(request2).unwrap();

        let mut request3 = build_test_request_post("example.com");
        request3.set_body(BytesMut::zeroed(1));
        let resp_fut3 = client.send_request(request3).unwrap();

        // Drive connection and all requests
        let (_, r1, r2, r3) = join4(
            async { conn.await.expect("conn") },
            async { resp_fut1.await.expect("req1") },
            resp_fut2,
            async { resp_fut3.await.expect("req3") },
        )
        .await;

        assert_eq!(r1.status(), &StatusCode::OK);
        assert_eq!(r3.status(), &StatusCode::OK);
        assert!(r2.is_err());
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        // Receive headers for all 3 streams
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;
        srv.recv_frame(frames::headers(3).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;
        srv.recv_frame(frames::headers(5).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;

        // Receive all data from stream 1 (exhausts 65535 byte connection window)
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16_383]).eos())
            .await;

        // Send response to stream 1
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        // Assign 1 byte of connection window (initially for stream 3)
        srv.send_frame(frames::window_update(0, 1))
            .await;

        // But then reset stream 3
        srv.send_frame(frames::reset(3).cancel())
            .await;

        // Stream 5 should use that window instead
        srv.recv_frame(frames::data(5, vec![0u8; 1]).eos())
            .await;

        // Send response to stream 5
        srv.send_frame(frames::headers(5).response(200).eos())
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn data_padding() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let mut body = Vec::new();
    body.push(5); // pad length
    body.extend_from_slice(&[b'z'; 100][..]); // actual data
    body.extend_from_slice(&[b'0'; 5][..]); // padding

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        srv.send_frame(
            frames::headers(1)
                .response(200)
                .field("content-length", "100"),
        )
        .await;

        srv.send_frame(frames::data(1, body).padded().eos())
            .await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let request = build_test_request();
        let resp_fut = client.send_request(request).unwrap();

        join(async move { conn.await.expect("client") }, async move {
            let resp = resp_fut.await.expect("response");
            assert_eq!(resp.status(), &StatusCode::OK);

            let body = resp.body_as_ref().unwrap();

            // Verify padding was stripped - body should be exactly 100 bytes
            assert_eq!(body.len(), 100);
            // Verify it's all 'z' characters (no padding '0' bytes)
            assert!(body.iter().all(|&b| b == b'z'));
        })
        .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn window_size_does_not_underflow() {
    support::trace_init!();
    let (io, mut client) = mock::new();

    let client_fut = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frame::Settings::default());

        // invalid HEADERS frame (missing mandatory fields).
        client
            .send_bytes(&[0, 0, 0, 1, 5, 0, 0, 0, 1])
            .await;

        // Send multiple SETTINGS with wildly varying window sizes
        client
            .send_frame(frames::settings().initial_window_size(1329018135))
            .await;

        client
            .send_frame(frames::settings().initial_window_size(3809661))
            .await;

        client
            .send_frame(frames::settings().initial_window_size(1467177332))
            .await;

        client
            .send_frame(frames::settings().initial_window_size(3844989))
            .await;
    };

    let srv_fut = async move {
        let mut srv = ServerBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        poll_fn(move |cx| srv.poll_closed(cx))
            .await
            .unwrap();
    };

    join(client_fut, srv_fut).await;
}

#[tokio::test]
async fn too_many_window_update_resets_causes_go_away() {
    support::trace_init!();
    let (io, mut client) = mock::new();

    let client_fut = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frame::Settings::default());

        // Send 10 streams with invalid window updates
        for s in (1..21).step_by(2) {
            client
                .send_frame(
                    frames::headers(s)
                        .request("GET", "https", "example.com", "/")
                        .eos(),
                )
                .await;

            // Send invalid WINDOW_UPDATE that will cause flow control error
            client
                .send_frame(frames::window_update(s, u32::MAX - 2))
                .await;

            // Expect server to reset this stream
            client
                .recv_frame(frames::reset(s).flow_control())
                .await;
        }

        // 11th stream - should trigger GOAWAY
        client
            .send_frame(
                frames::headers(21)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;

        client
            .send_frame(frames::window_update(21, u32::MAX - 2))
            .await;

        // Expect GOAWAY instead of reset
        client
            .recv_frame(
                frames::go_away(21)
                    .calm()
                    .data("too_many_internal_resets"),
            )
            .await;
    };

    let srv_fut = async move {
        let mut srv = ServerBuilder::new()
            .max_local_error_reset_streams(Some(10))
            .handshake(io)
            .await
            .expect("handshake");

        // Process the first 10 streams that get reset
        for _ in (1..21).step_by(2) {
            let result = srv.accept().await;
            assert!(result.is_some()); // Stream accepted but will be reset
        }

        // 11th stream should cause connection-level GOAWAY
        let err = poll_fn(move |cx| srv.poll_closed(cx)).await;
        assert!(err.is_err());

        let err = err.unwrap_err();
        assert_eq!(err.reason(), Some(Reason::ENHANCE_YOUR_CALM));
    };

    join(client_fut, srv_fut).await;
}
