use support::prelude::*;

#[tokio::test]
async fn client_out_of_order_complete() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        // req 1
        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(10));
        let resp1 = client.send_request(request).unwrap();

        // req 2
        let request = build_test_request();
        let resp2 = client.send_request(request).unwrap();
        let result = conn
            .drive(async {
                futures::future::select(Box::pin(resp1), Box::pin(resp2)).await
            })
            .await;

        match result {
            Either::Left((_resp1, _resp2_futt)) => {
                panic!("Stream 1 completed first");
            }
            Either::Right((resp2, _)) => {
                let resp = resp2.unwrap();
                assert_eq!(resp.status(), &StatusCode::OK);
                assert_eq!(
                    resp.body_as_ref(),
                    Some(&BytesMut::zeroed(16_200))
                );
            }
        }

        // drive resp2
        conn.await.expect("client");
    };

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(frames::settings())
            .await;
        assert_default_settings!(settings);
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
        srv.recv_frame(frames::data(1, vec![0; 10]).eos())
            .await;
        srv.send_frame(frames::headers(3).response(200))
            .await;
        srv.send_frame(frames::data(3, vec![0; 16_200]).eos())
            .await;
        idle_ms(0).await;
        srv.send_frame(frames::headers(1).response(204).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn server_out_of_order_complete() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req1, mut responder1) = s.accept().await.unwrap().unwrap();
        let (req2, mut responder2) = s.accept().await.unwrap().unwrap();

        assert_eq!(req1.method(), &Method::POST);
        assert_eq!(req2.method(), &Method::GET);

        responder2
            .send_response(build_test_response())
            .unwrap();
        responder1
            .send_response(build_test_response())
            .unwrap();
        poll_fn(|cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frame::Settings::default());
        client
            .send_frame(
                frames::headers(1).request("POST", "https", "a.b", "/"),
            )
            .await;

        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;

        client
            .send_frame(frames::data(1, vec![0; 10]).eos())
            .await;
        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    join(client, srv).await;
}

#[tokio::test]
async fn client_reset_get() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        // req 1
        let request = build_test_request();
        let resp1 = client.send_request(request).unwrap();

        // req 2
        let request = build_test_request();
        let resp2 = client.send_request(request).unwrap();
        let (resp1, resp2) = conn
            .drive(async { join(Box::pin(resp1), Box::pin(resp2)).await })
            .await;

        assert!(resp1.is_err());

        let resp2 = resp2.unwrap();
        assert_eq!(resp2.status(), &StatusCode::NO_CONTENT);

        // drive resp2
        conn.await.expect("client");
    };

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(frames::settings())
            .await;
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
        srv.send_frame(frames::reset(1).stream_closed())
            .await;
        srv.send_frame(frames::headers(3).response(204).eos())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn server_reset_get() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req1, mut responder1) = s.accept().await.unwrap().unwrap();
        let (req2, mut responder2) = s.accept().await.unwrap().unwrap();
        assert_eq!(req1.method(), &Method::GET);
        assert_eq!(req2.method(), &Method::GET);

        let result = responder1.send_response(build_test_response());
        assert!(result.is_err());
        let _ = responder2.send_response(build_test_response());

        poll_fn(|cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frame::Settings::default());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;

        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;

        // send reset for stream 1
        client
            .send_frame(frames::reset(1).stream_closed())
            .await;

        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
    };

    join(client, srv).await;
}

#[tokio::test]
async fn client_reset_pending_send() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let client_fut = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();

        // req 1
        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(10));
        let resp1 = client.send_request(request).unwrap();

        // req 2
        let request = build_test_request();
        let resp2 = client.send_request(request).unwrap();
        let (resp1, resp2) = conn
            .drive(async { join(Box::pin(resp1), Box::pin(resp2)).await })
            .await;

        assert!(resp1.is_err());

        let resp2 = resp2.unwrap();
        assert_eq!(resp2.status(), &StatusCode::NO_CONTENT);

        // drive resp2
        conn.await.expect("client");
    };

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(
                frames::settings().initial_window_size(1),
            )
            .await;
        assert_default_settings!(settings);
        srv.recv_frame(frames::headers(1).request(
            "POST",
            "https",
            "http2.akamai.com",
            "/",
        ))
        .await;
        srv.send_frame(frames::reset(1).stream_closed())
            .await;
        srv.recv_frame(
            frames::headers(3)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(3).response(204).eos())
            .await;
        // only initial data frame is received, remaining is dropped
        srv.recv_frame(frames::data(1, vec![0; 1]))
            .await;
        // do a pingpong to ensure no other frames were sent
        srv.ping_pong([1; 8]).await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn server_reset_pending_send() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let srv = async move {
        let mut s = ServerBuilder::new()
            //.initial_window_size(1)
            .handshake(mock)
            .await
            .unwrap();
        let (req1, mut responder1) = s.accept().await.unwrap().unwrap();
        let (req2, mut responder2) = s.accept().await.unwrap().unwrap();
        assert_eq!(req1.method(), &Method::GET);
        assert_eq!(req2.method(), &Method::GET);

        let mut response1 = build_test_response();
        response1.set_body(BytesMut::zeroed(10));

        let mut response2 = build_test_response();
        response2.set_body(BytesMut::zeroed(10));

        let result = responder1.send_response(response1);
        assert!(result.is_err());

        let result = responder2.send_response(response2);
        assert!(result.is_ok());

        poll_fn(|cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frame::Settings::default());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;

        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;

        // send reset for stream 1
        client
            .send_frame(frames::reset(1).stream_closed())
            .await;

        // only response for stream 3 is recvd
        client
            .recv_frame(frames::headers(3).response(200))
            .await;
        client
            .recv_frame(frames::data(3, vec![0; 10]).eos())
            .await;
    };

    join(client, srv).await;
}

#[tokio::test]
async fn client_reset_pending_recv() {
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
        let result = conn.drive(resp).await;
        assert!(result.is_err());
        let resp = result
            .unwrap_err()
            .take_partial_response()
            .unwrap();
        assert_eq!(resp.status(), &StatusCode::OK);
        conn.await.unwrap();
    };

    let srv_fut = async move {
        let settings = srv
            .assert_client_handshake_with_settings(frames::settings())
            .await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frames::headers(1).response(200))
            .await;
        srv.send_frame(frames::reset(1).stream_closed())
            .await;
    };
    join(srv_fut, client_fut).await;
}

#[tokio::test]
async fn server_reset_pending_recv() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut responder) = s.accept().await.unwrap().unwrap();
        dbg!("done");
        assert_eq!(req.method(), &Method::GET);
        responder
            .send_response(build_test_response())
            .unwrap();
        poll_fn(|cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frame::Settings::default());
        client
            .send_frame(
                frames::headers(1).request("POST", "https", "a.b", "/"),
            )
            .await;

        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;

        client
            .send_frame(frames::reset(1).stream_closed())
            .await;
        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
    };

    join(client, srv).await;
}
