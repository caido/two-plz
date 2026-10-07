use support::prelude::*;

fn push_uri(path: &str) -> Uri {
    Uri::builder()
        .scheme(Scheme::HTTPS)
        .authority("example.com")
        .path(path)
        .build()
        .unwrap()
}

fn push_request(method: Method, path: &str) -> Request {
    Request::builder()
        .method(method)
        .uri(push_uri(path))
        .build()
}

fn promise(
    parent: u32,
    promised: u32,
    method: Method,
    path: &str,
) -> frame::PushPromise {
    frame::PushPromise::new(
        parent.into(),
        promised.into(),
        Pseudo::request(method, push_uri(path), None),
        HeaderMap::new(),
    )
}

async fn bounded(future: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("push test stalled");
}

// A raw client is intentional: the library client rejects inbound PUSH_PROMISE by default.
#[tokio::test]
async fn omitted_enable_push_allows_even_ids_and_promise_before_response() {
    support::trace_init!();
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            peer.assert_server_handshake_with_settings(
                frame::Settings::default(),
            )
            .await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(promise(1, 2, Method::GET, "/first"))
                .await;
            peer.recv_frame(promise(1, 4, Method::HEAD, "/second"))
                .await;
            let mut seen = [false; 4];
            for _ in 0..4 {
                let frame = peer.next().await.unwrap().unwrap();
                let slot = match frame {
                    frame::Frame::Headers(headers) => {
                        match u32::from(headers.stream_id()) {
                            2 => {
                                assert_frame_eq(
                                    headers,
                                    frames::headers(2).response(200),
                                );
                                0
                            }
                            4 => {
                                assert_frame_eq(
                                    headers,
                                    frames::headers(4).response(200).eos(),
                                );
                                1
                            }
                            1 => {
                                assert_frame_eq(
                                    headers,
                                    frames::headers(1).response(200).eos(),
                                );
                                2
                            }
                            id => panic!("unexpected response stream {id}"),
                        }
                    }
                    frame::Frame::Data(data) => {
                        assert!(seen[0], "pushed data preceded its headers");
                        assert_frame_eq(
                            data,
                            frames::data(2, b"pushed body").eos(),
                        );
                        3
                    }
                    other => {
                        panic!("unexpected frame after promises: {other:?}")
                    }
                };
                assert!(!seen[slot], "duplicate response frame");
                seen[slot] = true;
            }
            assert!(seen.into_iter().all(|seen| seen));
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            let mut first = parent
                .push_request(push_request(Method::GET, "/first"))
                .unwrap();
            let mut second = parent
                .push_request(push_request(Method::HEAD, "/second"))
                .unwrap();
            assert_eq!(first.stream_id(), StreamId::from(2));
            assert_eq!(second.stream_id(), StreamId::from(4));
            let mut response = build_test_response();
            response.set_body(BytesMut::from(&b"pushed body"[..]));
            first.send_response(response).unwrap();
            second
                .send_response(build_test_response())
                .unwrap();
            parent
                .send_response(build_test_response())
                .unwrap();
            assert!(server.accept().await.is_none());
        };
        join(peer_task, server_task).await;
    })
    .await;
}

#[tokio::test]
async fn explicit_disable_rejects_push_without_emitting_promise() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            let mut settings = frame::Settings::default();
            settings.set_enable_push(false);
            peer.assert_server_handshake_with_settings(settings)
                .await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(frames::headers(1).response(200).eos())
                .await;
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            assert!(
                parent
                    .push_request(push_request(Method::GET, "/asset"))
                    .is_err()
            );
            parent
                .send_response(build_test_response())
                .unwrap();
            assert!(server.accept().await.is_none());
        };
        join(peer_task, server_task).await;
    })
    .await;
}

fn invalid_requests() -> Vec<(&'static str, Request)> {
    let mut body = push_request(Method::GET, "/body");
    body.set_body(BytesMut::from(&b"not allowed"[..]));
    let mut headers = HeaderMap::new();
    headers.insert("connection", "keep-alive");
    vec![
        ("unsafe method", push_request(Method::POST, "/post")),
        ("CONNECT", push_request(Method::CONNECT, "/connect")),
        ("body", body),
        (
            "relative URI",
            Request::builder()
                .method(Method::GET)
                .uri(
                    Uri::builder()
                        .path("/relative")
                        .build()
                        .unwrap(),
                )
                .build(),
        ),
        (
            "connection header",
            Request::builder()
                .method(Method::GET)
                .uri(push_uri("/headers"))
                .headers(headers)
                .build(),
        ),
        (
            "extended CONNECT",
            Request::builder()
                .method(Method::CONNECT)
                .uri(build_test_uri())
                .extension("websocket".into())
                .build(),
        ),
    ]
}

#[tokio::test]
async fn invalid_push_requests_do_not_emit_frames_or_consume_ids() {
    bounded(async {
        for (name, invalid) in invalid_requests() {
            let (io, mut peer) = mock::new();
            let peer_task = async move {
                peer.assert_server_handshake().await;
                peer.send_frame(
                    frames::headers(1)
                        .request("GET", "https", "example.com", "/")
                        .eos(),
                )
                .await;
                peer.recv_frame(promise(1, 2, Method::GET, "/valid"))
                    .await;
                peer.recv_frame(frames::headers(2).response(200).eos())
                    .await;
                peer.recv_frame(frames::headers(1).response(200).eos())
                    .await;
            };
            let server_task = async move {
                let mut server = ServerBuilder::new()
                    .handshake(io)
                    .await
                    .unwrap();
                let (_, mut parent) = server.accept().await.unwrap().unwrap();
                assert!(
                    parent.push_request(invalid).is_err(),
                    "accepted {name}"
                );
                let mut pushed = parent
                    .push_request(push_request(Method::GET, "/valid"))
                    .unwrap();
                assert_eq!(
                    pushed.stream_id(),
                    StreamId::from(2),
                    "rejection consumed ID: {name}"
                );
                pushed
                    .send_response(build_test_response())
                    .unwrap();
                parent
                    .send_response(build_test_response())
                    .unwrap();
                assert!(server.accept().await.is_none());
            };
            join(peer_task, server_task).await;
        }
    })
    .await;
}

#[tokio::test]
async fn completed_parent_rejects_push() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            peer.assert_server_handshake().await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(frames::headers(1).response(200).eos())
                .await;
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            parent
                .send_response(build_test_response())
                .unwrap();
            assert!(
                parent
                    .push_request(push_request(Method::GET, "/late"))
                    .is_err()
            );
            assert!(server.accept().await.is_none());
        };
        join(peer_task, server_task).await;
    })
    .await;
}

#[tokio::test]
async fn inbound_push_promise_returns_protocol_error_instead_of_panicking() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            peer.assert_client_handshake().await;
            peer.recv_frame(
                frames::headers(1)
                    .request("GET", "https", "http2.akamai.com", "/")
                    .eos(),
            )
            .await;
            peer.send_frame(promise(1, 2, Method::GET, "/unsolicited"))
                .await;
            peer.recv_frame(frames::go_away(0).protocol_error())
                .await;
        };
        let client_task = async move {
            let (mut conn, mut sender) = ClientBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let response = sender
                .send_request(build_test_request())
                .unwrap();
            let error = (&mut conn)
                .await
                .expect_err("inbound push must fail the connection");
            assert_eq!(error.reason(), Some(Reason::PROTOCOL_ERROR));
            drop(response);
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn zero_concurrency_reserves_push_until_settings_increase() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            let mut settings = frame::Settings::default();
            settings.set_max_concurrent_streams(Some(0));
            peer.assert_server_handshake_with_settings(settings)
                .await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(promise(1, 2, Method::GET, "/blocked"))
                .await;
            // Parent responses are not subject to the client's push concurrency limit.
            peer.recv_frame(frames::headers(1).response(200).eos())
                .await;
            let mut settings = frame::Settings::default();
            settings.set_max_concurrent_streams(Some(1));
            peer.send_frame(settings).await;
            peer.recv_frame(frames::settings_ack())
                .await;
            peer.recv_frame(frames::headers(2).response(200).eos())
                .await;
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            let mut pushed = parent
                .push_request(push_request(Method::GET, "/blocked"))
                .unwrap();
            pushed
                .send_response(build_test_response())
                .unwrap();
            parent
                .send_response(build_test_response())
                .unwrap();
            assert!(server.accept().await.is_none());
        };
        join(peer_task, server_task).await;
    })
    .await;
}

#[tokio::test]
async fn disabling_push_in_settings_update_rejects_new_promises() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            peer.assert_server_handshake().await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(promise(1, 2, Method::GET, "/before"))
                .await;
            peer.recv_frame(frames::headers(2).response(200).eos())
                .await;
            peer.recv_frame(frames::headers(1).response(200).eos())
                .await;
            let mut settings = frame::Settings::default();
            settings.set_enable_push(false);
            peer.send_frame(settings).await;
            peer.recv_frame(frames::settings_ack())
                .await;
            peer.send_frame(
                frames::headers(3)
                    .request("GET", "https", "example.com", "/next")
                    .eos(),
            )
            .await;
            peer.recv_frame(frames::headers(3).response(200).eos())
                .await;
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            let mut pushed = parent
                .push_request(push_request(Method::GET, "/before"))
                .unwrap();
            pushed
                .send_response(build_test_response())
                .unwrap();
            parent
                .send_response(build_test_response())
                .unwrap();
            let (_, mut next) = server.accept().await.unwrap().unwrap();
            assert!(
                next.push_request(push_request(Method::GET, "/after"))
                    .is_err()
            );
            next.send_response(build_test_response())
                .unwrap();
            assert!(server.accept().await.is_none());
        };
        join(peer_task, server_task).await;
    })
    .await;
}

#[tokio::test]
async fn promised_handle_cancellation_before_wire_follows_promise() {
    bounded(async {
        for (explicit_reset, zero_concurrency) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let (io, mut peer) = mock::new();
            let peer_task = async move {
                let mut settings = frame::Settings::default();
                if zero_concurrency {
                    settings.set_max_concurrent_streams(Some(0));
                }
                peer.assert_server_handshake_with_settings(settings)
                    .await;
                peer.send_frame(
                    frames::headers(1)
                        .request("GET", "https", "example.com", "/")
                        .eos(),
                )
                .await;
                peer.recv_frame(promise(1, 2, Method::GET, "/cancel"))
                    .await;
                // Both explicit reset and dropping an unanswered handle cancel the push.
                let mut reset_seen = false;
                let mut parent_seen = false;
                for _ in 0..2 {
                    let frame = peer.next().await.unwrap().unwrap();
                    match frame {
                        frame::Frame::Reset(reset) => {
                            assert!(!reset_seen);
                            assert_frame_eq(reset, frames::reset(2).cancel());
                            reset_seen = true;
                        }
                        frame::Frame::Headers(headers) => {
                            assert!(!parent_seen);
                            assert_frame_eq(
                                headers,
                                frames::headers(1).response(200).eos(),
                            );
                            parent_seen = true;
                        }
                        other => panic!(
                            "unexpected frame after canceled push: {other:?}"
                        ),
                    }
                }
                assert!(reset_seen && parent_seen);
            };
            let server_task = async move {
                let mut server = ServerBuilder::new()
                    .handshake(io)
                    .await
                    .unwrap();
                let (_, mut parent) = server.accept().await.unwrap().unwrap();
                let mut pushed = parent
                    .push_request(push_request(Method::GET, "/cancel"))
                    .unwrap();
                if explicit_reset {
                    pushed.send_reset(Reason::CANCEL);
                    assert!(
                        pushed
                            .send_response(build_test_response())
                            .is_err()
                    );
                }
                drop(pushed);
                parent
                    .send_response(build_test_response())
                    .unwrap();
                assert!(server.accept().await.is_none());
            };
            join(peer_task, server_task).await;
        }
    })
    .await;
}

#[tokio::test]
async fn parent_reset_after_queuing_promise_suppresses_unsent_push() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            peer.assert_server_handshake().await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(frames::reset(1).cancel())
                .await;
            peer.send_frame(
                frames::headers(3)
                    .request("GET", "https", "example.com", "/next")
                    .eos(),
            )
            .await;
            peer.recv_frame(frames::headers(3).response(200).eos())
                .await;
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            let mut pushed = parent
                .push_request(push_request(Method::GET, "/suppressed"))
                .unwrap();
            parent.send_reset(Reason::CANCEL);
            assert!(
                parent
                    .push_request(push_request(Method::GET, "/late"))
                    .is_err()
            );
            let (_, mut next) = server.accept().await.unwrap().unwrap();
            assert!(
                pushed
                    .send_response(build_test_response())
                    .is_err()
            );
            next.send_response(build_test_response())
                .unwrap();
            assert!(server.accept().await.is_none());
        };
        join(peer_task, server_task).await;
    })
    .await;
}

#[tokio::test]
async fn head_push_rejects_response_body_trailers_and_open_streaming_body() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            peer.assert_server_handshake().await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(promise(1, 2, Method::HEAD, "/head"))
                .await;
            peer.recv_frame(frames::headers(2).response(200).eos())
                .await;
            peer.recv_frame(frames::headers(1).response(200).eos())
                .await;
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            let mut pushed = parent
                .push_request(push_request(Method::HEAD, "/head"))
                .unwrap();
            let mut response = build_test_response();
            response.set_body(BytesMut::from(&b"forbidden"[..]));
            assert!(pushed.send_response(response).is_err());
            let mut response = build_test_response();
            let mut trailers = HeaderMap::new();
            trailers.insert("x-test", "forbidden");
            response.set_trailers(trailers);
            assert!(pushed.send_response(response).is_err());
            assert!(
                pushed
                    .send_response_streaming(build_test_response(), false)
                    .is_err()
            );
            pushed
                .send_response_streaming(build_test_response(), true)
                .unwrap();
            parent
                .send_response(build_test_response())
                .unwrap();
            assert!(server.accept().await.is_none());
        };
        join(peer_task, server_task).await;
    })
    .await;
}
