use futures::stream::FuturesUnordered;
use rstest::rstest;
use support::prelude::{frame::Frame, spa::Mode, *};
use tokio::sync::oneshot;

#[rstest]
#[case::basic(Mode::default())]
#[case::standard(Mode::ping())]
#[tokio::test]
async fn spa_initial_window_increase_with_pending_requests(
    #[case] mode: Mode,
) {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let n = 4;

    let (conn, mut client) = ClientBuilder::new()
        .single_packet_attack_mode(mode.clone())
        .handshake(io)
        .await
        .expect("handshake");

    // Queue every request before polling the driver, so the peer's initial
    // SETTINGS increases the send window while the streams are pending_open.
    let requests = (0..n)
        .map(|i| {
            if i % 2 == 0 {
                build_test_request_post("http2.akamai.com")
            } else {
                build_test_request()
            }
        })
        .collect();
    let responses = client
        .spa(requests)
        .into_iter()
        .map(|result| result.expect("queue SPA request"))
        .collect::<FuturesUnordered<_>>();
    assert_eq!(responses.len(), n as usize);

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().initial_window_size(1_048_576),
            )
            .await;
        assert_default_settings!(settings);

        for i in 0..n {
            let method = if i % 2 == 0 {
                "POST"
            } else {
                "GET"
            };
            srv.recv_frame(frames::headers(i * 2 + 1).request(
                method,
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        }
        for i in (0..n).step_by(2) {
            srv.recv_frame(frames::data(i * 2 + 1, "hell"))
                .await;
        }

        if matches!(mode, Mode::Ping(_)) {
            let Frame::Ping(ping) = srv.recv_frame_raw().await else {
                panic!("expected SPA ping before final DATA frames");
            };
            assert!(!ping.is_ack());
            srv.send_frame(frames::ping(ping.into_payload()).pong())
                .await;
        }

        for i in 0..n {
            let payload = if i % 2 == 0 {
                "o"
            } else {
                ""
            };
            srv.recv_frame(frames::data(i * 2 + 1, payload).eos())
                .await;
        }
        for i in 0..n {
            srv.send_frame(
                frames::headers(i * 2 + 1)
                    .response(200)
                    .eos(),
            )
            .await;
        }
        done_rx
            .await
            .expect("client received all responses");
    };

    let client_fut = async move {
        let conn_handle = tokio::spawn(async move {
            conn.await.expect("h2 driver crashed");
        });
        let results: Vec<_> = responses.collect().await;
        assert_eq!(results.len(), n as usize);
        for result in results {
            assert_eq!(*result.expect("SPA response").status(), 200);
        }
        done_tx
            .send(())
            .expect("server still connected");
        drop(client);
        conn_handle
            .await
            .expect("h2 driver task panicked");
    };

    join(srv_fut, client_fut).await;
}

#[rstest]
#[case::basic(Mode::default())]
#[case::standard(Mode::ping())]
#[case::enhanced(Mode::enhanced())]
#[tokio::test]
async fn spa_post(#[case] mode: Mode) {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let n = 30;
    let mode_clone = mode.clone();
    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        // enhanced ping mode recv ping
        if let Mode::EnhancedPing(..) = mode_clone {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }

        // recv headers
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::headers(i).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        }
        // recv part body
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::data(i, "hell"))
                .await;
        }
        // if ping mode recv ping
        if matches!(mode_clone, Mode::Ping(_) | Mode::EnhancedPing(..)) {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // spa
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::data(i, "o").eos())
                .await;
        }
        // send response
        for i in (1..2 * n).step_by(2) {
            srv.send_frame(frames::headers(i).response(200).eos())
                .await;
        }
        let _ = done_rx.await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode.clone())
            .handshake(io)
            .await
            .unwrap();

        let conn_handle = tokio::spawn(async move {
            conn.await.expect("h2 driver crashed");
        });

        let request = build_test_request_post("http2.akamai.com");
        let mut requests = Vec::with_capacity(n as usize);
        for _ in 0..n {
            requests.push(request.clone());
        }

        let results: Vec<_> = client
            .spa(requests)
            .into_iter()
            .filter_map(Result::ok)
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;
        assert_eq!(results.len(), n as usize);
        let _ = done_tx.send(());
        drop(client);
        for res in results {
            assert_eq!(*res.unwrap().status(), 200);
        }
        let _ = conn_handle.await;
    };

    join(srv_fut, client_fut).await;
}

#[rstest]
#[case::basic(Mode::default())]
#[case::standard(Mode::ping())]
#[case::enhanced(Mode::enhanced())]
#[tokio::test]
async fn spa_get(#[case] mode: Mode) {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let n = 30;
    let mode_clone = mode.clone();
    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        // enhanced ping mode recv ping
        if let Mode::EnhancedPing(..) = mode_clone {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }

        // recv headers
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::headers(i).request(
                "GET",
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        }
        // if ping mode recv ping
        if matches!(mode_clone, Mode::Ping(_) | Mode::EnhancedPing(..)) {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // spa
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::data(i, "").eos())
                .await;
        }
        // send response
        for i in (1..2 * n).step_by(2) {
            srv.send_frame(frames::headers(i).response(200).eos())
                .await;
        }
        let _ = done_rx.await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode)
            .handshake(io)
            .await
            .unwrap();

        let conn_handle = tokio::spawn(async move {
            conn.await.expect("h2 driver crashed");
        });

        let request = build_test_request();
        let mut requests = Vec::with_capacity(n as usize);
        for _ in 0..n {
            requests.push(request.clone());
        }

        let results: Vec<_> = client
            .spa(requests)
            .into_iter()
            .filter_map(Result::ok)
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;
        assert_eq!(results.len(), n as usize);
        let _ = done_tx.send(());
        drop(client);
        for res in results {
            assert_eq!(*res.unwrap().status(), 200);
        }
        let _ = conn_handle.await;
    };

    join(srv_fut, client_fut).await;
}

#[rstest]
#[case::basic(Mode::default())]
#[case::standard(Mode::ping())]
#[case::enhanced(Mode::enhanced())]
#[tokio::test]
async fn spa_basic_mixed(#[case] mode: Mode) {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let n = 30;

    let mode_clone = mode.clone();
    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        // enhanced ping mode recv ping
        if let Mode::EnhancedPing(..) = mode_clone {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // headers
        for i in 0..n {
            let stream_id = (i * 2) + 1;
            let method = if i % 2 == 0 {
                "POST"
            } else {
                "GET"
            };
            srv.recv_frame(frames::headers(stream_id).request(
                method,
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        }
        // POST body
        for i in 0..n {
            let stream_id = (i * 2) + 1;
            if i % 2 == 0 {
                srv.recv_frame(frames::data(stream_id, "hell"))
                    .await;
            }
        }
        // if ping mode recv ping
        if matches!(mode_clone, Mode::Ping(_) | Mode::EnhancedPing(..)) {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send ping
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // spa [ POST + GET remaining ]
        for i in 0..n {
            let stream_id = (i * 2) + 1;
            let payload = if i % 2 == 0 {
                "o"
            } else {
                ""
            };
            srv.recv_frame(frames::data(stream_id, payload).eos())
                .await;
        }
        // Responses
        for i in (1..2 * n).step_by(2) {
            srv.send_frame(frames::headers(i).response(200).eos())
                .await;
        }

        let _ = done_rx.await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode)
            .handshake(io)
            .await
            .expect("handshake");

        let conn_task = tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("Connection ended: {:?}", e);
            }
        });

        let post_req = build_test_request_post("http2.akamai.com");
        let get_req = build_test_request();

        let mut requests = Vec::with_capacity(n as usize);
        for i in 0..n {
            if i % 2 == 0 {
                requests.push(post_req.clone());
            } else {
                requests.push(get_req.clone());
            }
        }

        let results: Vec<_> = client
            .spa(requests)
            .into_iter()
            .filter_map(Result::ok)
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;

        assert_eq!(results.len(), n as usize);
        for res in results {
            assert_eq!(*res.unwrap().status(), 200);
        }

        let _ = done_tx.send(());
        drop(client);
        let _ = conn_task.await;
    };

    tokio::join!(srv_fut, client_fut);
}

#[rstest]
#[case::basic(Mode::default())]
#[case::standard(Mode::ping())]
#[case::enhanced(Mode::enhanced())]
#[tokio::test]
async fn spa_basic_post_stream_window_exhaust(#[case] mode: Mode) {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let n = 30;
    let mode_clone = mode.clone();
    let srv_fut = async move {
        let mut settings = frame::Settings::default();
        // Streams start with 10 capacity
        settings.set_initial_window_size(Some(3));
        let settings = srv
            .assert_client_handshake_with_settings(settings)
            .await;
        assert_default_settings!(settings);
        // enhanced ping mode recv ping
        if let Mode::EnhancedPing(..) = mode_clone {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // headers
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::headers(i).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        }
        // body - till capacity
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::data(i, "hel"))
                .await;
        }
        // release capacity
        for i in (1..2 * n).step_by(2) {
            srv.send_frame(frames::window_update(i, 5))
                .await;
        }
        // remaining body except last byte
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::data(i, "l"))
                .await;
        }
        // if ping mode recv ping
        if matches!(mode_clone, Mode::Ping(_) | Mode::EnhancedPing(..)) {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send ping
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }

        // spa
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::data(i, "o").eos())
                .await;
        }
        // response
        for i in (1..2 * n).step_by(2) {
            srv.send_frame(frames::headers(i).response(200).eos())
                .await;
        }
        let _ = done_rx.await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode)
            .handshake(io)
            .await
            .unwrap();

        let conn_handle = tokio::spawn(async move {
            conn.await.expect("h2 driver crashed");
        });

        let request = build_test_request_post("http2.akamai.com");
        let mut requests = Vec::with_capacity(n as usize);
        for _ in 0..n {
            requests.push(request.clone());
        }

        let results: Vec<_> = client
            .spa(requests)
            .into_iter()
            .filter_map(Result::ok)
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;
        assert_eq!(results.len(), n as usize);
        let _ = done_tx.send(());
        drop(client);
        for res in results {
            assert_eq!(*res.unwrap().status(), 200);
        }
        let _ = conn_handle.await;
    };

    join(srv_fut, client_fut).await;
}

#[rstest]
#[case::basic(Mode::default())]
#[case::standard(Mode::ping())]
#[case::enhanced(Mode::enhanced())]
#[tokio::test]
async fn spa_basic_post_stream_window_exhaust_mixed(#[case] mode: Mode) {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let n = 30;
    let mode_clone = mode.clone();
    let srv_fut = async move {
        let mut settings = frame::Settings::default();
        // Streams start with 10 capacity
        settings.set_initial_window_size(Some(3));
        let settings = srv
            .assert_client_handshake_with_settings(settings)
            .await;
        assert_default_settings!(settings);
        // enhanced ping mode recv ping
        if let Mode::EnhancedPing(..) = mode_clone {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // headers
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::headers(i).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        }
        // recv part body
        for i in 0..n {
            let stream_id = (i * 2) + 1;
            if i % 2 != 0 {
                srv.recv_frame(frames::data(stream_id, "hel"))
                    .await;
            }
        }
        // send window update
        for i in 0..n {
            let stream_id = (i * 2) + 1;
            if i % 2 != 0 {
                srv.send_frame(frames::window_update(stream_id, 5))
                    .await;
            }
        }
        // recv rem body
        for i in 0..n {
            let stream_id = (i * 2) + 1;
            if i % 2 != 0 {
                srv.recv_frame(frames::data(stream_id, "l"))
                    .await;
            }
        }
        // if ping mode recv ping
        if matches!(mode_clone, Mode::Ping(_) | Mode::EnhancedPing(..)) {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send ping
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // spa
        for i in 0..n {
            let stream_id = (i * 2) + 1;
            let payload = if i % 2 != 0 {
                "o"
            } else {
                ""
            };
            srv.recv_frame(frames::data(stream_id, payload).eos())
                .await;
        }
        // response
        for i in (1..2 * n).step_by(2) {
            srv.send_frame(frames::headers(i).response(200).eos())
                .await;
        }
        let _ = done_rx.await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode)
            .handshake(io)
            .await
            .unwrap();

        let conn_handle = tokio::spawn(async move {
            conn.await.expect("h2 driver crashed");
        });

        let request = build_test_request_post("http2.akamai.com");
        let mut requests = Vec::with_capacity(n as usize);
        for i in 0..n {
            let mut clone = request.clone();
            if i % 2 == 0 {
                clone.take_body();
            }
            requests.push(clone);
        }

        let results: Vec<_> = client
            .spa(requests)
            .into_iter()
            .filter_map(Result::ok)
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;
        assert_eq!(results.len(), n as usize);
        let _ = done_tx.send(());
        drop(client);
        for res in results {
            assert_eq!(*res.unwrap().status(), 200);
        }
        let _ = conn_handle.await;
    };

    join(srv_fut, client_fut).await;
}

#[rstest]
#[case::basic(Mode::default())]
#[case::standard(Mode::ping())]
#[case::enhanced(Mode::enhanced())]
#[tokio::test]
async fn spa_basic_post_conn_window(#[case] mode: Mode) {
    support::trace_init!();
    let (io, mut srv) = mock::new();
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let n = 4;
    let mode_clone = mode.clone();
    let srv_fut = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        // enhanced ping mode recv ping
        if let Mode::EnhancedPing(..) = mode_clone {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send pong
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }
        // headers
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::headers(i).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
        }
        // except last request
        for i in (1..2 * n - 1).step_by(2) {
            srv.recv_frame(frames::data(i, vec![0u8; 16_384]))
                .await;
        }
        // last request
        srv.recv_frame(frames::data(7, vec![0u8; 16_383]))
            .await;

        // send window update
        srv.send_frame(frames::window_update(0, 10))
            .await;

        // remaining 1 byte from stream 7 before spa
        srv.recv_frame(frames::data(7, vec![0u8; 1]))
            .await;

        // if ping mode recv ping
        if matches!(mode_clone, Mode::Ping(_) | Mode::EnhancedPing(..)) {
            if let Frame::Ping(ping) = srv.recv_frame_raw().await {
                let payload = ping.into_payload();
                // send ping
                srv.send_frame(frames::ping(payload).pong())
                    .await;
            } else {
                panic!()
            }
        }

        // spa
        for i in (1..2 * n).step_by(2) {
            srv.recv_frame(frames::data(i, vec![0; 1]).eos())
                .await;
        }
        // response
        for i in (1..2 * n).step_by(2) {
            srv.send_frame(frames::headers(i).response(200).eos())
                .await;
        }
        let _ = done_rx.await;
    };

    let client_fut = async move {
        let (conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode)
            .handshake(io)
            .await
            .unwrap();

        let conn_handle = tokio::spawn(async move {
            conn.await.expect("h2 driver crashed");
        });

        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(16385));
        let mut requests = Vec::with_capacity(n as usize);
        for _ in 0..n {
            requests.push(request.clone());
        }

        let results: Vec<_> = client
            .spa(requests)
            .into_iter()
            .filter_map(Result::ok)
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;
        assert_eq!(results.len(), n as usize);
        let _ = done_tx.send(());
        drop(client);
        for res in results {
            assert_eq!(*res.unwrap().status(), 200);
        }
        let _ = conn_handle.await;
    };

    join(srv_fut, client_fut).await;
}
