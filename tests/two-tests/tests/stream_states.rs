use support::{prelude::*, util::yield_once};
use tokio::sync::oneshot;

// Skipped
// recv_next_stream_id_updated_by_malformed_headers

#[tokio::test]
async fn send_recv_headers_only() {
    support::trace_init!();

    let mock = mock_io::Builder::new()
        .handshake()
        // Write GET /
        .write(&[
            0, 0, 0x10, 1, 5, 0, 0, 0, 1, 0x82, 0x87, 0x41, 0x8B, 0x9D, 0x29,
            0xAC, 0x4B, 0x8F, 0xA8, 0xE9, 0x19, 0x97, 0x21, 0xE9, 0x84,
        ])
        // Read response
        .read(&[0, 0, 1, 1, 5, 0, 0, 0, 1, 0x89])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .unwrap();

    // Send the request
    let request = build_test_request();

    tracing::info!("sending request");
    let response = client.send_request(request).unwrap();
    let resp = conn.run(response).await.unwrap();
    assert_eq!(resp.status(), &StatusCode::NO_CONTENT);
    conn.await.unwrap();
}

#[tokio::test]
async fn send_recv_data() {
    support::trace_init!();

    let mock = mock_io::Builder::new()
        .handshake()
        .write(&[
            // POST /
            0, 0, 16, 1, 4, 0, 0, 0, 1, 131, 135, 65, 139, 157, 41, 172, 75,
            143, 168, 233, 25, 151, 33, 233, 132,
        ])
        .write(&[
            // DATA
            0, 0, 5, 0, 1, 0, 0, 0, 1, 104, 101, 108, 108, 111,
        ])
        // Read response
        .read(&[
            // HEADERS
            0, 0, 1, 1, 4, 0, 0, 0, 1, 136, // DATA
            0, 0, 5, 0, 1, 0, 0, 0, 1, 119, 111, 114, 108, 100,
        ])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .unwrap();

    let request = build_test_request_post("http2.akamai.com");
    let response = client.send_request(request).unwrap();

    let resp = conn.run(response).await.unwrap();
    assert_eq!(resp.status(), &StatusCode::OK);
    assert_eq!(resp.body_as_ref().unwrap(), "world");
    conn.await.unwrap();
}

#[tokio::test]
async fn send_headers_recv_data_single_frame() {
    support::trace_init!();

    let mock = mock_io::Builder::new()
        .handshake()
        // Write GET /
        .write(&[
            0, 0, 16, 1, 5, 0, 0, 0, 1, 130, 135, 65, 139, 157, 41, 172, 75,
            143, 168, 233, 25, 151, 33, 233, 132,
        ])
        // Read response
        .read(&[
            0, 0, 1, 1, 4, 0, 0, 0, 1, 136, 0, 0, 5, 0, 0, 0, 0, 0, 1, 104,
            101, 108, 108, 111, 0, 0, 5, 0, 1, 0, 0, 0, 1, 119, 111, 114, 108,
            100,
        ])
        .build();

    let (mut conn, mut client) = ClientBuilder::new()
        .handshake(mock)
        .await
        .unwrap();

    let request = build_test_request();
    let response = client.send_request(request).unwrap();
    let resp = conn.run(response).await.unwrap();
    assert_eq!(resp.status(), &StatusCode::OK);
    assert_eq!(resp.body_as_ref().unwrap(), "helloworld");
    conn.await.unwrap();
}

#[tokio::test]
async fn closed_streams_are_released() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        // send request
        let request = build_test_request();
        let response = client.send_request(request).unwrap();
        // there is one active stream
        assert_eq!(1, client.num_active_streams());
        let resp = conn.drive(response).await.unwrap();
        assert_eq!(resp.status(), &StatusCode::NO_CONTENT);
        // There are no active streams
        assert_eq!(0, client.num_active_streams());
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
        srv.send_frame(frames::headers(1).response(204).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn reset_streams_dont_grow_memory_continuously() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    const N: u32 = 50;
    const MAX: usize = 20;

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());

        for n in (1..(N * 2)).step_by(2) {
            client
                .send_frame(
                    frames::headers(n)
                        .request("GET", "https", "a.b", "/")
                        .eos(),
                )
                .await;
            client
                .send_frame(frames::reset(n).protocol_error())
                .await;
        }

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.recv_frame(
                frames::go_away((MAX * 2 + 1) as u32)
                    .data("too_many_resets")
                    .calm(),
            ),
        )
        .await
        .expect("client goaway");
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_pending_accept_reset_streams(MAX)
            .handshake(mock)
            .await
            .unwrap();
        poll_fn(|cx| s.poll_closed(cx))
            .await
            .expect_err("server should error");
        // specifically, not 50;
        assert_eq!(21, s.num_wired_streams());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn go_away_with_pending_accepting() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let (sent_go_away_tx, sent_go_away_rx) = oneshot::channel();
    let (recv_go_away_tx, recv_go_away_rx) = oneshot::channel();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());

        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "baguette", "/")
                    .eos(),
            )
            .await;

        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "campag", "/")
                    .eos(),
            )
            .await;
        client
            .send_frame(frames::go_away(1).protocol_error())
            .await;
        sent_go_away_tx.send(()).unwrap();
        recv_go_away_rx.await.unwrap();
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_pending_accept_reset_streams(1)
            .handshake(mock)
            .await
            .unwrap();
        let (_req_1, _send_response_1) = s.accept().await.unwrap().unwrap();

        poll_fn(|cx| s.poll_closed(cx))
            .drive(sent_go_away_rx)
            .await
            .unwrap();

        let (_req_2, _send_response_2) = s.accept().await.unwrap().unwrap();

        recv_go_away_tx.send(()).unwrap();
    };

    join(client, srv).await;
}

#[tokio::test]
async fn pending_accept_reset_streams_decrement_too() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    // If it didn't decrement internally, this would eventually get
    // the count over MAX.
    const M: usize = 2;
    const N: usize = 5;
    const MAX: usize = 6;

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());

        let mut id = 1;
        for _ in 0..M {
            for _ in 0..N {
                client
                    .send_frame(
                        frames::headers(id)
                            .request("GET", "https", "a.b", "/")
                            .eos(),
                    )
                    .await;
                client
                    .send_frame(frames::reset(id).protocol_error())
                    .await;
                id += 2;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_pending_accept_reset_streams(MAX)
            .handshake(mock)
            .await
            .unwrap();
        while let Some(Ok(_)) = s.accept().await {}

        poll_fn(|cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn errors_if_recv_frame_exceeds_max_frame_size() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let req = async move {
            let request = build_test_request();
            let partial = client
                .send_request(request)
                .unwrap()
                .await
                .unwrap_err();
            assert_eq!(
                partial.err().to_string(),
                "connection error detected: frame with invalid size"
            );
        };

        // client should see a conn error
        let conn = async move {
            let err = conn.await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "connection error detected: frame with invalid size"
            );
        };
        join(conn, req).await;
    };

    // a bad peer
    srv.codec_mut()
        .set_max_send_frame_size(16_384 * 4);

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
        srv.send_frame(frames::data(1, vec![0; 16_385]).eos())
            .await;
        srv.recv_frame(frames::go_away(0).frame_size())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn configure_max_frame_size() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .max_frame_size(16_384 * 2)
            .handshake(io)
            .await
            .unwrap();

        let req = async move {
            let request = build_test_request();
            let resp = client
                .send_request(request)
                .unwrap()
                .await
                .unwrap();
            assert_eq!(resp.status(), &StatusCode::OK);
            assert_eq!(resp.body_as_ref().map(|b| b.len()), Some(16385));
        };

        join(async move { conn.await.expect("client") }, req).await;
    };

    // a good peer
    srv.codec_mut()
        .set_max_send_frame_size(16_384 * 2);

    let srv_fut = async move {
        let _ = srv.assert_client_handshake().await;
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16_385]).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_goaway_finishes_processed_streams() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut client_clone = client.clone();

        let req1 = async move {
            let request = build_test_request();
            let resp = client
                .send_request(request)
                .unwrap()
                .await
                .unwrap();
            assert_eq!(resp.status(), &StatusCode::OK);
            assert_eq!(resp.body_as_ref().map(|b| b.len()), Some(16384));
        };

        let req2 = async move {
            let request = build_test_request();
            let err = client_clone
                .send_request(request)
                .unwrap()
                .await
                .unwrap_err()
                .err()
                .to_string();
            assert_eq!(
                err,
                "connection error received: not a result of an error"
            );
        };

        join3(async move { conn.await.expect("client") }, req1, req2).await;
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
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::go_away(1)).await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]).eos())
            .await;
        srv.recv_frame(frames::go_away(0)).await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn recv_goaway_with_higher_last_processed_id() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let partial = conn.drive(resp).await.unwrap_err();
        let err = partial.err();
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
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
        srv.send_frame(frames::go_away(1)).await;
        // a bigger goaway? kaboom
        srv.send_frame(frames::go_away(3)).await;
        // expecting a goaway of 0, since server never initiated a stream
        srv.recv_frame(frames::go_away(0).protocol_error())
            .await;
    };
    join(srv_fut, client_fut).await;
}

// TODO(init): initial_stream_id
#[ignore]
#[tokio::test]
async fn skipped_stream_ids_are_implicitly_closed() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            //.initial_stream_id(5)
            .handshake(io)
            .await
            .unwrap();

        let req = async move {
            let request = build_test_request();
            let resp = client
                .send_request(request)
                .unwrap()
                .await
                .expect("response");
            assert_eq!(resp.status(), &StatusCode::OK);
        };
        conn.drive(req).await;
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
        // send the response on a lower-numbered stream, which should be
        // implicitly closed.
        srv.send_frame(frames::headers(3).response(299))
            .await;
        // however, our client choose to send a RST_STREAM because it
        // can't tell if it had previously reset '3'.
        srv.recv_frame(frames::reset(3).stream_closed())
            .await;
        srv.send_frame(frames::headers(5).response(200).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn send_rst_stream_allows_recv_data() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (con, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let req = async {
            let request = build_test_request();
            let resp = client.send_request(request).unwrap();
            drop(resp);
        };

        let mut conn = Box::pin(async move {
            con.await.expect("client");
        });
        conn.drive(req).await;
        conn.await;
        drop(client);
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(frames::reset(1).cancel())
            .await;
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
        // sending frames after canceled!
        //   note: sending 2 to consume 50% of connection window
        srv.send_frame(frames::data(1, vec![0; 16_384]))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]).eos())
            .await;
        // make sure we automatically free the connection window
        srv.recv_frame(frames::window_update(0, 16_384 * 2))
            .await;
        // do a pingpong to ensure no other frames were sent
        srv.ping_pong([1; 8]).await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn send_rst_stream_allows_recv_trailers() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(frames::reset(1).cancel())
            .await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]))
            .await;
        // sending frames after canceled!
        srv.send_frame(
            frames::headers(1)
                .field("foo", "bar")
                .eos(),
        )
        .await;
        // do a pingpong to ensure no other frames were sent
        srv.ping_pong([1; 8]).await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        drop(resp);

        conn.await.unwrap();
        drop(client);
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn rst_stream_expires() {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .reset_stream_duration(Duration::from_millis(10))
            .handshake(io)
            .await
            .unwrap();
        let req = async {
            let request = build_test_request();
            let resp = client.send_request(request).unwrap();
            drop(resp);
        };

        let mut conn = Box::pin(async move { conn.await.expect("client") });
        conn.drive(req).await;
        conn.await;
        drop(client);
    };

    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        //srv.recv_frame(
        //    frames::headers(1)
        //        .request("GET", "https", "http2.akamai.com", "/")
        //        .eos(),
        //)
        //.await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16_384]))
            .await;
        srv.recv_frame(frames::reset(1).cancel())
            .await;
        // wait till after the configured duration
        idle_ms(15).await;
        srv.ping_pong([1; 8]).await;
        // sending frame after canceled!
        srv.send_frame(frames::data(1, vec![0; 16_384]).eos())
            .await;
        // window capacity is returned
        srv.recv_frame(frames::window_update(0, 16_384 * 2))
            .await;
        // and then stream error
        srv.recv_frame(frames::reset(1).stream_closed())
            .await;
    };

    join(srv_fut, client_fut).await;
}

/*
// TODO(support) - max_concurrent_reset_streams ?
// #[tokio::test]
async fn rst_stream_max() {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .max_concurrent_reset_streams(1)
            .handshake(io)
            .await
            .unwrap();

        let mut client_clone = client.clone();
        let req1 = async move {
            let request = build_test_request();
            let resp = client_clone
                .send_request(request)
                .unwrap();
            drop(resp);
        };

        let req2 = async move {
            let request = build_test_request();
            let resp = client.send_request(request).unwrap();
            drop(resp);
        };

        let mut conn = Box::pin(async move {
            conn.await.expect("client");
        });
        conn.drive(join(req1, req2)).await;
        conn.await;
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
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::data(1, vec![0; 16]))
            .await;
        srv.send_frame(frames::headers(3).response(200))
            .await;
        srv.send_frame(frames::data(3, vec![0; 16]))
            .await;
        srv.recv_frame(frames::reset(1).cancel())
            .await;
        srv.recv_frame(frames::reset(3).cancel())
            .await;
        // sending frame after canceled!
        // olders streams trump newer streams
        // 1 is still being ignored
        srv.send_frame(frames::data(1, vec![0; 16]).eos())
            .await;
        // ping pong to be sure of no goaway
        srv.ping_pong([1; 8]).await;
        // 3 has been evicted, will get a reset
        srv.send_frame(frames::data(3, vec![0; 16]).eos())
            .await;
        srv.recv_frame(frames::reset(3).stream_closed())
            .await;
    };

    join(srv_fut, client_fut).await;
}
*/

#[tokio::test]
async fn rst_while_closing() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    // Rendezvous when we've queued a trailers frame
    let (tx, rx) = oneshot::channel();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("example.com");
        request.set_body(BytesMut::zeroed(100_000));
        request.set_trailers(HeaderMap::new());
        let resp = client
            .send_request(request)
            .expect("send_request");
        let _resp = conn.drive(resp).await;
        // on receipt of an EOS response from the server, transition
        // the stream Open => Half Closed (remote).
        //let resp = conn.drive(resp).await;
        let _: () = tx.send(()).unwrap();
        yield_once().await;
        // yield once to allow the server mock to be polled
        // before the conn flushes its buffer
        drop(client);
        conn.await.expect("client");
    };

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
        srv.recv_frame(frames::data(1, vec![0u8; 16384]))
            .await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::headers(1).eos())
            .await;
        // Idling for a moment here is necessary to ensure that the client
        // enqueues its TRAILERS frame *before* we send the RST_STREAM frame
        // which causes the panic.
        // Send the RST_STREAM frame which causes the client to panic.
        // data frames
        srv.recv_frame(frames::data(1, vec![0u8; 16384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16384]))
            .await;
        srv.recv_frame(frames::data(1, vec![0u8; 16383]))
            .await;
        rx.await.unwrap();
        srv.send_frame(frames::reset(1).cancel())
            .await;
        srv.ping_pong([1; 8]).await;
        srv.recv_frame(frames::go_away(0).no_error())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn rst_with_buffered_data() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    // Rendezvous when we've queued a trailers frame
    let (tx, rx) = oneshot::channel();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("example.com");
        // A large body
        let body =
            BytesMut::zeroed(2 * frame::DEFAULT_INITIAL_WINDOW_SIZE as usize);
        request.set_body(body);
        let resp = client
            .send_request(request)
            .expect("send_request");
        let _resp = conn.drive(resp).await;
        let _: () = tx.send(()).unwrap();
        conn.await.expect("client");
    };

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
        srv.recv_frame(frames::data(1, vec![0; 16_384]))
            .await;
        srv.send_frame(frames::headers(1).response(204).eos())
            .await;
        srv.send_frame(frames::reset(1).cancel())
            .await;
        rx.await.unwrap();
        srv.unbounded_bytes().await;
        srv.recv_frame(frames::data(1, vec![0; 16_384]))
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn err_with_buffered_data() {
    support::trace_init!();

    let (io, mut srv) = mock::new_with_write_capacity(73);
    let (tx, rx) = oneshot::channel();
    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        tokio::spawn(async move {
            let _ = conn.await;
        });

        let mut request = build_test_request_post("example.com");
        let body =
            BytesMut::zeroed(2 * frame::DEFAULT_INITIAL_WINDOW_SIZE as usize);
        request.set_body(body);

        let resp = client
            .send_request(request)
            .expect("send_request");
        drop(client);

        let res = resp.await;
        assert!(res.is_err());
        tx.send(()).unwrap();
    };

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
        srv.buffer_bytes(128).await;
        srv.send_frame(frames::headers(1).response(204).eos())
            .await;
        srv.send_bytes(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00")
            .await;

        rx.await.unwrap();
        srv.unbounded_bytes().await;
        srv.recv_frame(frames::data(1, vec![0; 16_384]))
            .await;
    };

    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn send_err_with_buffered_data() {
    // Data is buffered in `FramedWrite` and the stream is reset locally before
    // the data is fully flushed. Given that resetting a stream requires
    // clearing all associated state for that stream, this test ensures that the
    // buffered up frame is correctly handled.
    //support::trace_init!();
    let (io, mut srv) = mock::new();

    // Rendezvous when we've queued a trailers frame
    let (tx, rx) = oneshot::channel();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        let mut request = build_test_request_post("example.com");
        // A large body
        let body = BytesMut::zeroed(10);
        request.set_body(body);
        let resp = client
            .send_request(request)
            .expect("send_request");
        // Hack to drive the connection, trying to flush data
        futures::future::lazy(|cx| {
            if let Poll::Ready(v) = conn.poll_unpin(cx) {
                v.unwrap();
            }
        })
        .await;
        drop(resp);
        drop(client);
        tx.send(()).unwrap();
        conn.await.unwrap();
    };

    let srv_fut = async move {
        let mut settings = frame::Settings::default();
        settings.set_initial_window_size(Some(1));
        let _ = srv
            .assert_client_handshake_with_settings(settings)
            .await;
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "example.com",
            "/",
        ))
        .await;
        srv.recv_frame(frames::data(1, vec![0; 1]))
            .await;
        rx.await.unwrap();
        srv.recv_frame(frames::reset(1).cancel())
            .await;
        srv.recv_frame(frames::go_away(0).no_error())
            .await;
    };
    join(srv_fut, client_fut).await;
}

// TODO(pp)
#[ignore]
#[tokio::test]
async fn srv_window_update_on_lower_stream_id() {}

#[tokio::test]
async fn reset_new_stream_before_send() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        // req 1
        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let resp = conn.drive(resp).await.unwrap();
        assert_eq!(resp.status(), &StatusCode::OK);

        // req 2
        let request = build_test_request();
        let resp = client.send_request(request).unwrap();
        let resp = conn.drive(resp).await.unwrap();
        assert_eq!(resp.status(), &StatusCode::OK);

        conn.await.expect("client");
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
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;
        // Send unexpected headers, that depends on itself, causing a framing
        // error.
        srv.send_bytes(&[
            0, 0, 0x6,  // len
            0x1,  // type (headers)
            0x25, // flags (eos, eoh, pri)
            0, 0, 0, 0x3, // stream id
            0, 0, 0, 0x3,  // dependency
            2,    // weight
            0x88, // HPACK :status=200
        ])
        .await;
        srv.recv_frame(frames::reset(3).protocol_error())
            .await;
        srv.recv_frame(
            frames::headers(5)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(5).response(200).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn explicit_reset_with_max_concurrent_stream() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        {
            // req 1
            let mut request = build_test_request_post("http2.akamai.com");
            request.set_body(BytesMut::zeroed(10));
            let resp = client.send_request(request).unwrap();
            let resp = conn.drive(resp).await.unwrap();
            drop(resp);
            poll_once(&mut conn).await.unwrap();
        }

        // req 2
        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(1));
        let resp = client.send_request(request).unwrap();
        let _resp = conn.drive(resp).await;
        conn.await.expect("client");
    };

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings()
                    .max_concurrent_streams(1)
                    .initial_window_size(1),
            )
            .await;
        assert_default_settings!(settings);

        // Receive request 1 headers
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        // Send response to stream 1
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        // Receive partial data (only 1 byte due to window size)
        srv.recv_frame(frames::data(1, vec![0; 1]))
            .await;

        // Receive reset for stream 1
        srv.recv_frame(frames::reset(1).cancel())
            .await;

        // Now stream 3 can be received (slot freed)
        srv.recv_frame(frames::headers(3).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        srv.recv_frame(frames::data(3, vec![0; 1]).eos())
            .await;

        // Respond to stream 3
        srv.send_frame(frames::headers(3).response(200).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn implicit_cancel_with_max_concurrent_stream() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        {
            let request = build_test_request_post("http2.akamai.com");
            //request.set_body(None);

            let resp = client.send_request(request).unwrap();
            drop(resp);

            poll_once(&mut conn).await.unwrap();
        }

        {
            let request = build_test_request_post("http2.akamai.com");
            //request.set_body(None);
            let resp_fut = client.send_request(request).unwrap();
            let resp = conn.drive(resp_fut).await.unwrap();
            assert_eq!(resp.status(), &StatusCode::OK);
        }

        conn.await.expect("client");
    };

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().max_concurrent_streams(1),
            )
            .await;
        assert_default_settings!(settings);

        // Receive request 1 headers with EOS (no body)
        srv.recv_frame(frames::reset(1).cancel())
            .await;

        // Send response to stream 1
        srv.send_frame(frames::headers(1).response(200).eos())
            .await;

        // Now stream 3 can be received (slot freed by reset)
        srv.recv_frame(frames::headers(3).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;

        // Send response to stream 3
        srv.send_frame(frames::headers(3).response(200).eos())
            .await;
    };

    join(srv_fut, client_fut).await;
}
