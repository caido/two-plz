use support::prelude::*;

// skipped
// send_reset_notifies_recv_stream
// http_2_request_without_scheme_or_authority
// connection_close_notifies_client_poll_ready

trait MockH2 {
    fn local_handshake(&mut self) -> &mut Self;
}

impl MockH2 for mock_io::Builder {
    fn local_handshake(&mut self) -> &mut Self {
        self.write(MAGIC_PREFACE)
            // Settings frame
            .write(frames::NEW_SETTINGS)
            .read(frames::NEW_SETTINGS)
            .write(frames::SETTINGS_ACK)
            .read(frames::SETTINGS_ACK)
    }
}

#[tokio::test]
async fn client_handshake() {
    support::trace_init!();

    let mock = mock_io::Builder::new()
        .local_handshake()
        .build();

    let (conn, _client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .unwrap();
    conn.await.unwrap();
}

#[tokio::test]
async fn client_sends_request_before_peer_settings() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        srv.read_preface().await.unwrap();

        let settings = assert_settings!(srv.recv_frame_raw().await);
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        srv.send_frame(frame::Settings::default())
            .await;
        srv.send_frame(frame::Settings::ack())
            .await;
        srv.recv_frame(frame::Settings::ack())
            .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("write-ready handshake");
        let response = client
            .send_request(build_test_request())
            .unwrap();
        let response = conn.drive(response).await.unwrap();
        assert_eq!(response.status(), &StatusCode::OK);
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn client_rejects_settings_ack_as_first_peer_frame() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        srv.read_preface().await.unwrap();
        let settings = assert_settings!(srv.recv_frame_raw().await);
        assert_default_settings!(settings);
        srv.send_frame(frame::Settings::ack())
            .await;
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.recv_frame(frames::go_away(0).protocol_error())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("write-ready handshake");
        let response = client
            .send_request(build_test_request())
            .expect("queue request");
        let partial = conn
            .drive(response)
            .await
            .expect_err("initial SETTINGS ACK must fail queued requests");
        assert_eq!(partial.err().reason(), Some(Reason::PROTOCOL_ERROR));
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn client_rejects_non_settings_first_peer_frame() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        srv.read_preface().await.unwrap();
        let settings = assert_settings!(srv.recv_frame_raw().await);
        assert_default_settings!(settings);
        srv.send_frame(frames::ping([0; 8]))
            .await;
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.recv_frame(frames::go_away(0).protocol_error())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("write-ready handshake");
        let response = client
            .send_request(build_test_request())
            .expect("queue request");
        let partial = conn
            .drive(response)
            .await
            .expect_err("initial PING must fail queued requests");
        assert_eq!(partial.err().reason(), Some(Reason::PROTOCOL_ERROR));
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn client_rejects_eof_before_peer_settings() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        srv.read_preface().await.unwrap();
        let settings = assert_settings!(srv.recv_frame_raw().await);
        assert_default_settings!(settings);
    };

    let client_fut = async move {
        let (conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("write-ready handshake");
        let err = conn
            .await
            .expect_err("EOF before initial peer SETTINGS must fail");
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn client_other_thread() {
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
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();
        tokio::spawn(async move {
            let request = build_test_request();
            let _ = client
                .send_request(request)
                .unwrap()
                .await
                .expect("request");
        });
        conn.await.expect("h2");
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_invalid_server_stream_id() {
    support::trace_init!();

    let io = mock_io::Builder::new()
        .local_handshake()
        // Write GET /
        .write(&[
            0, 0, 0x10, 1, 5, 0, 0, 0, 1, 0x82, 0x87, 0x41, 0x8B, 0x9D, 0x29,
            0xAC, 0x4B, 0x8F, 0xA8, 0xE9, 0x19, 0x97, 0x21, 0xE9, 0x84,
        ])
        ////// Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 2, 137])
        ////// Write GO_AWAY
        .write(&[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        .build();

    let (conn, mut client) = ClientBuilder::new()
        .handshake(io)
        .await
        .unwrap();

    let request = build_test_request();
    let response = client.send_request(request).unwrap();

    // The connection errors
    assert!(conn.await.is_err());

    //// The stream errors
    assert!(response.await.is_err());
}

/*
// TODO(init): fix - client - initial_stream_id
#[ignore]
#[tokio::test]
async fn request_stream_id_overflows() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            //.initial_stream_id(u32::MAX >> 1)
            .handshake(io)
            .await
            .unwrap();
        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let _r = conn.drive(resp).await.unwrap();

        // second cannot use the next stream id, it's over
        // let poll_err = poll_fn(|cx| client.poll_ready(cx)).await.unwrap_err();
        // assert_eq!(poll_err.to_string(), "user error: stream ID overflowed");

        let request = build_test_request();
        let err = client
            .send_request(request)
            .unwrap()
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "user error: stream ID overflowed");
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(u32::MAX >> 1)
                .request("GET", "https", "example.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(
            frames::headers(u32::MAX >> 1)
                .response(200)
                .eos(),
        )
        .await;
        idle_ms(10).await;
    };

    join(client_fut, srv_fut).await;
}
*/

#[tokio::test]
async fn client_builder_max_concurrent_streams() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let mut settings = frame::Settings::default();
    settings.set_enable_push(false);
    settings.set_max_concurrent_streams(Some(1));

    let srv_fut = async move {
        let rcvd_settings = srv.assert_client_handshake().await;
        assert_frame_eq(settings, rcvd_settings);

        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let builder = ClientBuilder::new().max_concurrent_streams(1);

    let client_fut = async move {
        let (mut conn, mut client) = builder.handshake(io).await.unwrap();
        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let _r = conn.drive(resp).await.unwrap();
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn request_over_max_concurrent_streams_errors() {
    support::trace_init!();

    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let _ = srv
            .assert_client_handshake_with_settings(
                frames::settings()
                    .max_concurrent_streams(1)
                    // Tiny window to immediately block on capacity
                    .initial_window_size(1),
            )
            .await;

        // Stream 1: Receive 1 byte (fills window), then blocked
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;
        srv.recv_frame(frames::data(1, vec![0; 1]))
            .await;

        // Grant 1 more byte to stream 1
        srv.send_frame(frames::window_update(1, 1))
            .await;
        srv.recv_frame(frames::data(1, vec![0; 1]).eos())
            .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        // Stream 3 should immediately send with the new capacity
        srv.recv_frame(frames::headers(3).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;

        // Grant connection capacity -
        // should be allocated to stream 3 (not pending streams 5-13)
        srv.send_frame(frames::window_update(0, 2))
            .await;

        // Grant stream 3 capacity as well
        srv.send_frame(frames::window_update(3, 1))
            .await;

        srv.recv_frame(frames::data(3, vec![0; 1]))
            .await;
        srv.send_frame(frames::window_update(3, 1))
            .await;
        srv.recv_frame(frames::data(3, vec![0; 1]).eos())
            .await;
        srv.send_frame(frames::headers(3).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // Stream 1: Will block after 1 byte due to window size
        let mut req1 = build_test_request_post("example.com");
        req1.set_body(BytesMut::from(&vec![0u8; 2][..]));
        let resp1_fut = client.send_request(req1).unwrap();

        // Stream 3: Queue up waiting for connection window
        let mut req2 = build_test_request_post("example.com");
        req2.set_body(BytesMut::from(&vec![0u8; 2][..]));
        let resp2_fut = client.send_request(req2).unwrap();

        // Queue 5 more streams that will be pending (waiting to open due to
        // MAX_CONCURRENT_STREAMS)
        for _ in 0..5 {
            let mut req = build_test_request_post("example.com");
            req.set_body(BytesMut::from(&vec![0u8; 2][..]));
            client.send_request(req).unwrap();
        }

        let resp1 = conn.drive(resp1_fut).await.unwrap();
        assert_eq!(resp1.status(), &StatusCode::OK);

        let resp2 = conn.drive(resp2_fut).await.unwrap();
        assert_eq!(resp2.status(), &StatusCode::OK);
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_decrement_max_concurrent_streams_when_requests_queued() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        // Stream 1: Complete before settings change
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        srv.ping_pong([0; 8]).await;

        // Reduce concurrent streams limit to 1
        srv.send_frame(frames::settings().max_concurrent_streams(1))
            .await;
        srv.recv_frame(frames::settings_ack())
            .await;

        // Stream 3: Should be allowed (within new limit of 1)
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.ping_pong([1; 8]).await;
        srv.send_frame(frames::headers(3).response(200).eos())
            .await;

        // Stream 5: Should now be allowed (stream 3 completed)
        srv.recv_frame(
            frames::headers(5)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(5).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // Stream 1: Before settings change
        let req1 = build_test_request();
        let resp1_fut = client.send_request(req1).unwrap();
        let resp1 = conn.drive(resp1_fut).await.unwrap();
        assert_eq!(resp1.status(), &StatusCode::OK);

        // Stream 3: After settings change (allowed)
        let req2 = build_test_request();
        let resp2_fut = client.send_request(req2).unwrap();

        // Stream 5: Should be queued in pending_open
        let req3 = build_test_request();
        let resp3_fut = client.send_request(req3).unwrap();

        let resp2 = conn.drive(resp2_fut).await.unwrap();
        assert_eq!(resp2.status(), &StatusCode::OK);

        let resp3 = conn.drive(resp3_fut).await.unwrap();
        assert_eq!(resp3.status(), &StatusCode::OK);

        conn.await.unwrap();
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn send_request_poll_ready_when_connection_error() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().max_concurrent_streams(1),
            )
            .await;
        assert_default_settings!(settings);

        // Stream 1: Complete successfully
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        // Stream 3: Receive request
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        // PROTOCOL ERROR: Send response with wrong stream ID (8 instead of 3)
        // Even stream IDs are reserved for server-initiated streams
        srv.send_frame(frames::headers(8).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // first request is allowed
        let req1 = build_test_request();
        let resp1_fut = client.send_request(req1).unwrap();
        // as long as we let the connection internals tick
        let resp1 = conn.drive(resp1_fut).await.unwrap();
        assert_eq!(resp1.status(), &StatusCode::OK);

        // second request is put into pending_open
        let req2 = build_test_request();
        let resp2_fut = client.send_request(req2).unwrap();

        // third stream is over max concurrent
        let req3 = build_test_request();
        let resp3_fut = client.send_request(req3).unwrap();

        // a FuturesUnordered is used on purpose!
        //
        // We don't want a join, since any of the other futures notifying
        // will make the until_ready future polled again, but we are
        // specifically testing that until_ready gets notified on its own.
        let mut unordered = futures::stream::FuturesUnordered::new();
        unordered.push(Box::pin(async move {
            conn.await
                .expect_err("connection should error");
        }) as Pin<Box<dyn Future<Output = ()>>>);

        // Both pending responses should error
        unordered.push(Box::pin(async move {
            resp2_fut
                .await
                .expect_err("resp2 should error");
        }));
        unordered.push(Box::pin(async move {
            resp3_fut
                .await
                .expect_err("resp3 should error");
        }));

        while unordered.next().await.is_some() {}
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn http_11_request_without_scheme_or_authority() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .request("GET", "http", "", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let uri = Uri::builder()
            .path("/")
            .scheme(Scheme::HTTP)
            .build()
            .unwrap();
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .build();

        let resp_fut = client.send_request(req).unwrap();
        let resp = conn.drive(resp_fut).await.unwrap();
        assert_eq!(resp.status(), &StatusCode::OK);

        conn.await.unwrap();
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn http_2_connect_request_omit_scheme_and_path_fields() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        // CONNECT request should only have :method and :authority
        // (no :scheme or :path)
        srv.recv_frame(
            frames::headers(1)
                .pseudo(Pseudo {
                    method: Some(Method::CONNECT),
                    authority: util::byte_str("tunnel.example.com:8443")
                        .into(),
                    ..Default::default()
                })
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // In HTTP_2 CONNECT request the ":scheme" and ":path" pseudo-header
        // fields MUST be omitted.
        let uri = Uri::builder()
            .scheme(Scheme::HTTPS)
            .authority("tunnel.example.com:8443")
            .path("/")
            .build()
            .unwrap();

        let req = Request::builder()
            .method(Method::CONNECT)
            .uri(uri)
            .build();

        let resp_fut = client.send_request(req).unwrap();
        let resp = conn.drive(resp_fut).await.unwrap();
        assert_eq!(resp.status(), &StatusCode::OK);

        conn.await.unwrap();
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn request_with_connection_headers() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        srv.send_frame(frames::new_settings())
            .await;
        srv.read_preface().await.unwrap();
        srv.recv_frame(frames::new_settings())
            .await;
        srv.send_frame(frames::settings_ack())
            .await;
        srv.recv_frame(frames::settings_ack())
            .await;
        // goaway is required to make sure the connection closes because
        // of no active streams
        srv.recv_frame(frames::go_away(0)).await;
    };

    let forbidden_headers = vec![
        ("connection", "foo"),
        ("keep-alive", "5"),
        ("proxy-connection", "bar"),
        ("transfer-encoding", "chunked"),
        ("upgrade", "HTTP/2"),
        ("te", "boom"),
    ];

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        for (name, val) in forbidden_headers {
            let uri = Uri::builder()
                .authority("http2.akamai.com")
                .scheme(Scheme::HTTPS)
                .build()
                .unwrap();

            let mut builder = Request::builder()
                .method(Method::GET)
                .uri(uri);

            let mut header_map = HeaderMap::new();
            // Try to add forbidden header
            header_map.insert(name, val);
            builder = builder.headers(header_map);

            let err = client
                .send_request(builder.build())
                .expect_err(name);
            assert_eq!(err.to_string(), "user error: malformed headers");
        }

        poll_once(&mut conn).await.unwrap();
        drop(client);
        conn.await.unwrap();
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn connection_close_notifies_response_future() {
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
        // don't send any response, just close
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let req = build_test_request();
        let resp_fut = client.send_request(req).unwrap();

        let req_task = async move {
            let partial = resp_fut.await.expect_err("response");
            assert_eq!(
                partial.err().to_string(),
                "stream closed because of a broken pipe"
            );
        };

        join(async move { conn.await.expect("conn") }, req_task).await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn sending_request_on_closed_connection() {
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
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        // Protocol error: HEADERS frame on stream 0 (invalid)
        srv.send_frame(frames::headers(0).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // First request succeeds
        let req1 = build_test_request();
        let resp1_fut = client.send_request(req1).unwrap();
        let resp1 = conn
            .drive(resp1_fut)
            .await
            .expect("response1");
        assert_eq!(resp1.status(), &StatusCode::OK);

        // Connection should error due to protocol violation
        let conn_err = conn
            .await
            .expect_err("connection should error");
        let msg =
            "connection error detected: unspecific protocol error detected";
        assert_eq!(conn_err.to_string(), msg);

        // Attempting to send another request should fail
        let req2 = build_test_request();
        let send_err = client.send_request(req2).unwrap_err();
        assert_eq!(send_err.to_string(), msg);
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_too_big_headers() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().max_header_list_size(10),
            )
            .await;
        assert_frame_eq(
            settings,
            frames::settings()
                .max_header_list_size(10)
                .disable_push(),
        );

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

        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
        // No END_STREAM
        srv.send_frame(frames::headers(3).response(200))
            .await;

        // no reset for 1, since it's closed anyway
        // but reset for 3, since server hasn't closed stream
        srv.recv_frame(frames::reset(3).protocol_error())
            .await;
        idle_ms(10).await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .max_header_list_size(10)
            .handshake(io)
            .await
            .expect("handshake");

        let req1 = build_test_request();
        let resp1_fut = client.send_request(req1).unwrap();

        let req2 = build_test_request();
        let resp2_fut = client.send_request(req2).unwrap();

        // Spawn tasks to ensure that the error wakes up tasks that are blocked
        // waiting for a response.
        let task1 = async move {
            let partial = resp1_fut
                .await
                .expect_err("response1 should error");
            assert_eq!(partial.err().reason(), Some(Reason::PROTOCOL_ERROR));
        };

        let task2 = async move {
            let partial = resp2_fut
                .await
                .expect_err("response2 should error");
            assert_eq!(partial.err().reason(), Some(Reason::PROTOCOL_ERROR));
        };

        conn.drive(join(task1, task2)).await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn pending_send_request_gets_reset_by_peer_properly() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let payload = vec![0u8; (frame::DEFAULT_INITIAL_WINDOW_SIZE * 2) as usize];
    let max_frame_size = frame::DEFAULT_MAX_FRAME_SIZE as usize;

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

        // Receive initial DATA frames (up to window limit)
        srv.recv_frame(frames::data(1, &payload[0..max_frame_size]))
            .await;
        srv.recv_frame(frames::data(
            1,
            &payload[max_frame_size..(max_frame_size * 2)],
        ))
        .await;
        srv.recv_frame(frames::data(
            1,
            &payload[(max_frame_size * 2)..(max_frame_size * 3)],
        ))
        .await;
        srv.recv_frame(frames::data(
            1,
            &payload[(max_frame_size * 3)..(max_frame_size * 4 - 1)],
        ))
        .await;

        idle_ms(100).await;

        // Reset the stream without allowing more data
        srv.send_frame(frames::reset(1).refused())
            .await;

        // Because all active requests are finished, connection should shutdown
        // and send a GO_AWAY frame. If the reset stream is bugged (and doesn't
        // count towards concurrency limit), then connection will not send
        // a GO_AWAY and this test will fail.
        srv.recv_frame(frames::go_away(0)).await;
        drop(srv);
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let mut req = build_test_request();
        req.set_body(BytesMut::zeroed(
            frame::DEFAULT_INITIAL_WINDOW_SIZE as usize * 2,
        ));
        let resp_fut = client.send_request(req).unwrap();

        let response_task = async move {
            let partial = resp_fut
                .await
                .expect_err("response should error");
            assert_eq!(partial.err().reason(), Some(Reason::REFUSED_STREAM));
        };

        conn.drive(response_task).await;
        drop(client);
        conn.await.expect("client");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn request_without_path() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .request("GET", "http", "example.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // URI without explicit path - should default to "/"
        let uri = Uri::builder()
            .scheme(Scheme::HTTP)
            .authority("example.com")
            .build()
            .unwrap();
        // Note: No path set explicitly

        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .build();

        let resp_fut = client.send_request(req).unwrap();
        let resp = conn.drive(resp_fut).await.unwrap();
        assert_eq!(resp.status(), &StatusCode::OK);

        conn.await.unwrap();
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn request_options_with_star() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        // OPTIONS * should have :path = "*"
        srv.recv_frame(
            frames::headers(1)
                .request("OPTIONS", "http", "example.com", "*")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // OPTIONS request with asterisk path
        let uri = Uri::builder()
            .scheme(Scheme::HTTP)
            .authority("example.com")
            .path("*")
            .build()
            .unwrap();

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri(uri)
            .build();

        let resp_fut = client.send_request(req).unwrap();
        let resp = conn.drive(resp_fut).await.unwrap();
        assert_eq!(resp.status(), &StatusCode::OK);

        conn.await.unwrap();
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn notify_on_send_capacity() {
    use tokio::sync::oneshot;
    support::trace_init!();

    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel();
    let (tx, rx) = oneshot::channel();

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().max_concurrent_streams(1),
            )
            .await;
        assert_default_settings!(settings);

        tx.send(()).unwrap();

        // Receive and respond to 3 sequential requests
        for stream_id in [1, 3, 5] {
            srv.recv_frame(
                frames::headers(stream_id)
                    .request("GET", "https", "http2.akamai.com", "/")
                    .eos(),
            )
            .await;
            srv.send_frame(
                frames::headers(stream_id)
                    .response(200)
                    .eos(),
            )
            .await;
        }

        done_rx.await.unwrap();
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        tokio::spawn(async move {
            rx.await.unwrap();

            let mut response_futs = vec![];

            // Send 3 requests - they should be serialized due to
            // MAX_CONCURRENT_STREAMS=1
            for _ in 0..3 {
                let req = build_test_request();
                let resp_fut = client.send_request(req).unwrap();
                response_futs.push(resp_fut);
            }

            // All responses should complete successfully
            for resp_fut in response_futs {
                let resp = resp_fut.await.unwrap();
                assert_eq!(resp.status(), &StatusCode::OK);
            }

            done_tx.send(()).unwrap();
        });

        conn.await.expect("connection");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn send_stream_poll_reset() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .request("POST", "https", "example.com", "/")
                .eos(),
        )
        .await;

        // Server refuses the stream
        srv.send_frame(frames::reset(1).refused())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let mut req = build_test_request_post("example.com");
        req.take_body(); // Keep stream open for sending data
        let resp_fut = client.send_request(req).unwrap();

        // Drive the response future - should receive reset error
        let partial = conn
            .drive(resp_fut)
            .await
            .expect_err("should error");
        assert_eq!(partial.err().reason(), Some(Reason::REFUSED_STREAM));

        conn.await.expect("connection");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn drop_pending_open() {
    // This test checks that a stream queued for pending open behaves correctly
    // when its client drops.
    use tokio::sync::oneshot;
    support::trace_init!();

    let (io, mut srv) = mock::new();
    let (init_tx, init_rx) = oneshot::channel();
    let (trigger_go_away_tx, trigger_go_away_rx) = oneshot::channel();
    let (sent_go_away_tx, sent_go_away_rx) = oneshot::channel();
    let (drop_tx, drop_rx) = oneshot::channel();

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().max_concurrent_streams(2),
            )
            .await;
        assert_default_settings!(settings);
        init_tx.send(()).unwrap();

        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;

        trigger_go_away_rx.await.unwrap();
        srv.send_frame(frames::go_away(3)).await;
        sent_go_away_tx.send(()).unwrap();
        drop_rx.await.unwrap();

        srv.recv_frame(frames::data(1, vec![0; 10]).eos())
            .await;

        srv.send_frame(frames::headers(3).response(200).eos())
            .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .max_concurrent_reset_streams(0)
            .handshake(io)
            .await
            .expect("handshake");

        let f = async move {
            init_rx.await.expect("init_rx");

            // Fill up the concurrent stream limit.
            let mut req1 = build_test_request_post("http2.akamai.com");
            req1.set_body(BytesMut::zeroed(10));
            let resp1_fut = client.send_request(req1).unwrap();

            let req2 = build_test_request();
            let resp2_fut = client.send_request(req2).unwrap();

            let req3 = build_test_request();
            let resp3_fut = client.send_request(req3).unwrap();

            // Trigger a GOAWAY frame to invalidate our third request.
            trigger_go_away_tx.send(()).unwrap();
            sent_go_away_rx
                .await
                .expect("sent_go_away_rx");

            // Now drop all the references to that stream.
            drop(resp3_fut);
            drop(client);
            drop_tx.send(()).unwrap();

            // Complete remaining streams
            resp2_fut.await.expect("resp2");
            resp1_fut.await.expect("resp1")
        };

        join(async move { conn.await.expect("connection") }, f).await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn malformed_response_headers_dont_unlink_stream() {
    // This test checks that receiving malformed headers frame on a stream with
    // no remaining references correctly resets the stream, without prematurely
    // unlinking it.
    use tokio::sync::oneshot;
    support::trace_init!();

    let (io, mut srv) = mock::new();
    let (drop_tx, drop_rx) = oneshot::channel();
    let (queued_tx, queued_rx) = oneshot::channel();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

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

        drop_tx.send(()).unwrap();
        queued_rx.await.unwrap();

        // Send malformed HEADERS frame to stream 3
        srv.send_bytes(&[
            0, 0, 2, // 2 byte frame
            1, // type: HEADERS
            5, // flags: END_STREAM | END_HEADERS
            0, 0, 0, 3, // stream identifier: 3
            144, 135, // invalid HPACK data (pseudo not at end of block)
        ])
        .await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let mut req1 = build_test_request_post("example.com");
        // Use up most of the connection window.
        req1.set_body(BytesMut::zeroed(65534));
        let _resp1_fut = client.send_request(req1).unwrap();

        let mut req2 = build_test_request_post("example.com");
        // Use up the remainder of the connection window.
        req2.set_body(BytesMut::zeroed(2));
        let resp2_fut = client.send_request(req2).unwrap();

        let mut req3 = build_test_request_post("example.com");
        // Queue up for more connection window.
        req3.set_body(BytesMut::zeroed(1));
        let resp3_fut = client.send_request(req3).unwrap();

        let f = async move {
            drop_rx.await.unwrap();
            queued_tx.send(()).unwrap();
            // Drop references before malformed frame arrives
            drop((resp2_fut, resp3_fut));
        };

        join(async move { conn.await.expect("h2") }, f).await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn allow_empty_data_for_head() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .request("HEAD", "https", "example.com", "/")
                .eos(),
        )
        .await;

        srv.send_frame(
            frames::headers(1)
                .response(200)
                .field("content-length", "100"),
        )
        .await;

        // Send empty DATA frame with END_STREAM
        srv.send_frame(frames::data(1, "").eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let uri = Uri::builder()
            .authority("example.com")
            .scheme(Scheme::HTTPS)
            .build()
            .unwrap();
        let req = Request::builder()
            .method(Method::HEAD)
            .uri(uri)
            .build();

        let resp_fut = client.send_request(req).unwrap();
        let resp = conn.drive(resp_fut).await.unwrap();
        assert_eq!(resp.body_as_ref().unwrap(), "");

        conn.await.expect("connection");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn reject_non_zero_content_length_header_with_end_stream() {
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

        // Protocol violation: Content-Length: 100 but END_STREAM set (no body)
        srv.send_frame(
            frames::headers(1)
                .response(200)
                .field("content-length", "100")
                .eos(), // END_STREAM with no DATA frames
        )
        .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let req = build_test_request();
        let resp_fut = client.send_request(req).unwrap();

        let (response, err) = conn
            .drive(resp_fut)
            .await
            .unwrap_err()
            .into_parts();
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
        let response = response.expect("valid response headers are retained");
        assert_eq!(response.status(), &StatusCode::OK);
        assert!(response.body_as_ref().is_none());

        conn.await.expect("connection");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn content_length_mismatch_retains_partial_body() {
    support::trace_init!();
    // Short final DATA, an excessive DATA frame, and an untrusted large length.
    for (length, final_data, expected_body) in [
        ("5", "cd", "abcd"),
        ("3", "cd", "ab"),
        ("18446744073709551615", "cd", "abcd"),
    ] {
        let (io, mut srv) = mock::new();
        let srv_fut = async move {
            srv.assert_client_handshake().await;
            srv.recv_frame(
                frames::headers(1)
                    .request("GET", "https", "http2.akamai.com", "/")
                    .eos(),
            )
            .await;
            srv.send_frame(
                frames::headers(1)
                    .response(200)
                    .field("content-length", length),
            )
            .await;
            srv.send_frame(frames::data(1, "ab"))
                .await;
            srv.send_frame(frames::data(1, final_data).eos())
                .await;
            srv.recv_frame(frames::reset(1).reason(Reason::PROTOCOL_ERROR))
                .await;
        };
        let client_fut = async move {
            let (mut conn, mut client) = ClientBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let response = client
                .send_request(build_test_request())
                .unwrap();
            let (partial, err) = conn
                .drive(response)
                .await
                .unwrap_err()
                .into_parts();
            assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
            let partial = partial.expect("response headers");
            assert_eq!(partial.status(), &StatusCode::OK);
            assert_eq!(
                partial.body_as_ref().unwrap().as_ref(),
                expected_body.as_bytes()
            );
            conn.await.expect("connection");
        };
        join(srv_fut, client_fut).await;
    }
}

#[tokio::test]
async fn early_hints() {
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

        // Send 103 Early Hints informational response
        srv.send_frame(frames::headers(1).response(103))
            .await;

        // Send final 200 OK response
        srv.send_frame(
            frames::headers(1)
                .response(200)
                .field("content-length", "2"),
        )
        .await;

        // Send body data
        srv.send_frame(frames::data(1, "ok").eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let req = build_test_request();
        let resp_fut = client.send_request(req).unwrap();
        let resp = conn.drive(resp_fut).await.unwrap();

        assert_eq!(resp.status(), &StatusCode::OK);
        assert_eq!(resp.body_as_ref().unwrap().as_ref(), b"ok");

        conn.await.expect("connection");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn informational_while_local_streaming() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);

        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;

        // Send 103 while request body is pending
        srv.send_frame(frames::headers(1).response(103))
            .await;

        // Send final 200 response headers
        srv.send_frame(
            frames::headers(1)
                .response(200)
                .field("content-length", "2"),
        )
        .await;

        // Receive request body
        srv.recv_frame(frames::data(1, "hello").eos())
            .await;

        // Send response body
        srv.send_frame(frames::data(1, "ok").eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        // Send POST without body initially
        let mut req = build_test_request_post("example.com");
        req.set_body(BytesMut::from(&b"hello"[..]));
        let resp_fut = client.send_request(req).unwrap();

        // Await response (should get 200, not 103)
        let resp = conn
            .drive(resp_fut)
            .await
            .expect("response");
        assert_eq!(resp.status(), &StatusCode::OK);

        assert_eq!(resp.body_as_ref().unwrap().as_ref(), b"ok");
        conn.await.expect("connection");
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn extended_connect_protocol_disabled_by_default() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
    };

    let client_fut = async move {
        let (mut conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");
        poll_once(&mut conn).await.unwrap();
        assert!(!conn.is_extended_connect_protocol_enabled());
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn handshake_apply_enable_connect_protocol_settings() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().enable_connect_protocol(1),
            )
            .await;
        assert_default_settings!(settings);
    };

    let client_fut = async move {
        let (mut conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");
        assert!(!conn.is_extended_connect_protocol_enabled());
        poll_once(&mut conn).await.unwrap();
        assert!(conn.is_extended_connect_protocol_enabled());
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn invalid_connect_protocol_enabled_setting() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        srv.send(
            frames::settings()
                .enable_connect_protocol(2)
                .into(),
        )
        .await
        .unwrap();

        srv.read_preface().await.unwrap();

        let settings = assert_settings!(
            srv.next()
                .await
                .expect("unexpected EOF")
                .unwrap()
        );
        assert_default_settings!(settings);

        // Send the ACK
        let ack = frame::Settings::ack();
        srv.send(ack.into()).await.unwrap();

        let frame = srv.next().await.unwrap().unwrap();
        let go_away = assert_go_away!(frame);
        assert_eq!(go_away.reason(), Reason::PROTOCOL_ERROR);
    };

    let client_fut = async move {
        let (conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("write-ready handshake");
        let err = conn
            .await
            .expect_err("invalid peer settings must fail");
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn extended_connect_request() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().enable_connect_protocol(1),
            )
            .await;
        assert_default_settings!(settings);

        srv.recv_frame(
            frames::headers(1)
                .pseudo(Pseudo {
                    method: Method::CONNECT.into(),
                    scheme: util::byte_str("http").into(),
                    authority: util::byte_str("bread").into(),
                    path: util::byte_str("/baguette").into(),
                    protocol: Protocol::from_static("the-bread-protocol")
                        .into(),
                    ..Default::default()
                })
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        tokio::time::timeout(
            Duration::from_secs(5),
            poll_fn(|cx| {
                let result = Pin::new(&mut conn).poll(cx);
                assert!(result.is_pending());
                if conn.is_extended_connect_protocol_enabled() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("peer did not enable extended CONNECT");
        let uri = Uri::builder()
            .authority("bread")
            .scheme(Scheme::HTTP)
            .path("/baguette")
            .build()
            .unwrap();
        let req = Request::builder()
            .method(Method::CONNECT)
            .extension("the-bread-protocol".into())
            .uri(uri)
            .build();
        let resp = client.send_request(req).unwrap();
        conn.drive(resp).await.unwrap();
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn rogue_server_odd_headers() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.send_frame(frames::headers(1)).await;
        srv.recv_frame(frames::go_away(0).protocol_error())
            .await;
    };

    let client_fut = async move {
        let (conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let err = conn.await.unwrap_err();
        assert!(err.is_go_away());
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn rogue_server_even_headers() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.send_frame(frames::headers(2)).await;
        srv.recv_frame(frames::go_away(0).protocol_error())
            .await;
    };

    let client_fut = async move {
        let (conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let err = conn.await.unwrap_err();
        assert!(err.is_go_away());
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn rogue_server_reused_headers() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
        srv.send_frame(frames::headers(1)).await;
        srv.recv_frame(frames::reset(1).stream_closed())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();
        let req = build_test_request();
        let resp_fut = client.send_request(req).unwrap();
        let _resp = conn.drive(resp_fut).await.unwrap();
        conn.await.unwrap()
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn client_builder_header_table_size() {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let mut settings = frame::Settings::default();
    settings.set_header_table_size(Some(10000));
    settings.set_enable_push(false);

    let srv = async move {
        let recv_settings = srv.assert_client_handshake().await;
        assert_frame_eq(recv_settings, settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
        srv.send_frame(frames::headers(1)).await;
        srv.recv_frame(frames::reset(1).stream_closed())
            .await;
    };

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .header_table_size(10000)
            .handshake(io)
            .await
            .unwrap();
        let req = build_test_request();
        let resp_fut = client.send_request(req).unwrap();
        let _resp = conn.drive(resp_fut).await.unwrap();
        conn.await.unwrap()
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn configured_max_concurrent_send_streams_and_update_it_based_on_empty_settings_frame()
 {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        // Send empty SETTINGS frame (no MAX_CONCURRENT_STREAMS is provided)
        srv.send_frame(frames::settings()).await;
    };

    let client_fut = async move {
        let (conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        assert_eq!(conn.max_concurrent_send_streams(), usize::MAX);
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn configured_max_concurrent_send_streams_and_update_it_based_on_non_empty_settings_frame()
 {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        // Send SETTINGS frame with MAX_CONCURRENT_STREAMS set to 42
        srv.send_frame(frames::settings().max_concurrent_streams(42))
            .await;
    };

    let client_fut = async move {
        let (mut conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        assert_eq!(conn.max_concurrent_send_streams(), usize::MAX);
        poll_once(&mut conn).await.unwrap();
        assert_eq!(conn.max_concurrent_send_streams(), 42);
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn receive_settings_frame_twice_with_second_one_empty() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        // Send the initial SETTINGS frame with MAX_CONCURRENT_STREAMS set to 42
        srv.send_frame(frames::settings().max_concurrent_streams(42))
            .await;

        // Handle the client's connection preface
        srv.read_preface().await.unwrap();
        match srv.next().await {
            Some(frame) => match frame.unwrap() {
                frame::Frame::Settings(_) => {
                    let ack = frame::Settings::ack();
                    srv.send(ack.into()).await.unwrap();
                }
                frame => {
                    panic!("unexpected frame: {:?}", frame);
                }
            },
            None => {
                panic!("unexpected EOF");
            }
        }

        // Should receive the ack for the server's initial SETTINGS frame
        let frame = assert_settings!(srv.next().await.unwrap().unwrap());
        assert!(frame.is_ack());

        // Send another SETTINGS frame with no MAX_CONCURRENT_STREAMS
        // This should not update the max_concurrent_send_streams value that
        // the client manages.
        srv.send_frame(frames::settings()).await;
    };

    let client_fut = async move {
        let (conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut conn = std::pin::pin!(conn);
        conn.as_mut().await.unwrap();
        assert_eq!(conn.max_concurrent_send_streams(), 42);
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn receive_settings_frame_twice_with_second_one_non_empty() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        // Send the initial SETTINGS frame with MAX_CONCURRENT_STREAMS set to 42
        srv.send_frame(frames::settings().max_concurrent_streams(42))
            .await;

        // Handle the client's connection preface
        srv.read_preface().await.unwrap();
        match srv.next().await {
            Some(frame) => match frame.unwrap() {
                frame::Frame::Settings(_) => {
                    let ack = frame::Settings::ack();
                    srv.send(ack.into()).await.unwrap();
                }
                frame => {
                    panic!("unexpected frame: {:?}", frame);
                }
            },
            None => {
                panic!("unexpected EOF");
            }
        }

        // Should receive the ack for the server's initial SETTINGS frame
        let frame = assert_settings!(srv.next().await.unwrap().unwrap());
        assert!(frame.is_ack());

        // Send another SETTINGS frame with no MAX_CONCURRENT_STREAMS
        // This should not update the max_concurrent_send_streams value that
        // the client manages.
        srv.send_frame(frames::settings().max_concurrent_streams(2024))
            .await;
    };

    let client_fut = async move {
        let (conn, _client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut conn = std::pin::pin!(conn);
        conn.as_mut().await.unwrap();
        // The most-recently advertised value should be used
        assert_eq!(conn.max_concurrent_send_streams(), 2024);
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn server_drop_connection_unexpectedly_return_unexpected_eof_err() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.close_without_notify();
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let _resp = client.send_request(request).unwrap();

        let err = conn
            .await
            .expect_err("should receive UnexpectedEof");
        assert_eq!(
            err.get_io()
                .expect("should be UnexpectedEof")
                .kind(),
            std::io::ErrorKind::UnexpectedEof,
        );
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn server_drop_connection_after_go_away() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::go_away(1)).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        srv.close_without_notify();
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let _resp = client.send_request(request).unwrap();
        conn.await.unwrap();
    };

    join(srv, client_fut).await;
}

#[tokio::test]
async fn client_request_empty_body_new() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

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
        srv.recv_frame(frames::data(1, vec![]).eos())
            .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let conn_handle = tokio::spawn(async move {
            conn.await.expect("h2 driver crashed");
        });

        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::new());

        let response = client
            .send_request(request)
            .unwrap()
            .await
            .expect("request failed");

        assert_eq!(*response.status(), 200);

        drop(client);
        let _ = conn_handle.await;
    };

    join(srv_fut, client_fut).await;
}
