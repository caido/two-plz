use support::prelude::*;
use two_plz::message::BodyFrame;

async fn bounded(future: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("client push test stalled");
}

fn field(name: &str, value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(name, value);
    headers
}

fn push_uri(path: &str) -> Uri {
    Uri::builder()
        .scheme(Scheme::HTTPS)
        .authority("example.com")
        .path(path)
        .build()
        .unwrap()
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

// Abort transport drivers even when an assertion panics.
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn library_server_push_roundtrip() {
    bounded(async {
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (client, server) = tokio::join!(
            ClientBuilder::new().enable_push(true).handshake(client_io),
            ServerBuilder::new().handshake(server_io),
        );
        let (mut connection, mut sender) = client.unwrap();
        let mut server = server.unwrap();
        let parent_response = sender.send_request(build_test_request()).unwrap();
        let server_task = async move {
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            let mut pushed = parent.push_request(
                Request::builder().method(Method::GET).uri(push_uri("/asset")).build(),
            ).unwrap();
            assert_eq!(pushed.stream_id(), StreamId::from(2));
            let mut body = pushed.send_response_streaming(build_test_response(), false).unwrap();
            parent.send_response(build_test_response()).unwrap();
            let send = async move {
                body.send_data(Bytes::from_static(b"library push"), false).await.unwrap();
                let mut trailers = HeaderMap::new();
                trailers.insert("x-finished", "yes");
                body.send_trailers(trailers).unwrap();
            };
            tokio::select! {
                _ = poll_fn(|cx| server.poll_closed(cx)) => panic!("server closed before sending body"),
                _ = send => {}
            }
            let _ = poll_fn(|cx| server.poll_closed(cx)).await;
        };
        let client_task = async move {
            let (request, response) = connection.push().await.unwrap().unwrap();
            assert_eq!(request.method(), &Method::GET);
            assert!(request.body_as_ref().is_none());
            assert_eq!(request.into_message_head().0.uri(), &push_uri("/asset"));
            assert_eq!(response.stream_id(), StreamId::from(2));
            let _driver = Driver(tokio::spawn(async move { let _ = connection.await; }));
            let (head, mut body) = response.await.unwrap();
            assert_eq!(head.status(), &StatusCode::OK);
            assert!(head.body_as_ref().is_none());
            let mut data = Vec::new();
            let mut saw_trailers = false;
            while let Some(frame) = body.frame().await {
                match frame.unwrap() {
                    BodyFrame::Data(bytes) => {
                        assert!(!saw_trailers);
                        data.extend_from_slice(&bytes);
                    }
                    BodyFrame::Trailers(trailers) => {
                        assert!(!saw_trailers);
                        assert_eq!(trailers, field("x-finished", "yes"));
                        saw_trailers = true;
                    }
                }
            }
            assert_eq!(data, b"library push");
            assert!(saw_trailers);
            assert_eq!(parent_response.await.unwrap().status(), &StatusCode::OK);
        };
        join(server_task, client_task).await;
    }).await;
}

#[tokio::test]
async fn opt_in_advertises_enabled_and_exposes_multiple_get_and_head_promises()
{
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            let settings = peer.assert_client_handshake().await;
            assert_eq!(settings.is_push_enabled(), Some(true));
            peer.recv_frame(
                frames::headers(1)
                    .request("GET", "https", "http2.akamai.com", "/")
                    .eos(),
            )
            .await;
            peer.send_frame(promise(1, 2, Method::GET, "/get"))
                .await;
            peer.send_frame(promise(1, 4, Method::HEAD, "/head"))
                .await;
            // Responses deliberately arrive in the opposite order to the promises.
            peer.send_frame(frames::headers(4).response(200).eos())
                .await;
            peer.send_frame(
                frames::headers(2)
                    .response(200)
                    .field("x-pushed", "yes"),
            )
            .await;
            peer.send_frame(frames::data(2, b"raw push"))
                .await;
            peer.send_frame(
                frames::headers(2)
                    .field("x-finished", "yes")
                    .eos(),
            )
            .await;
            peer.send_frame(frames::headers(1).response(200).eos())
                .await;
            peer.recv_eof().await;
        };
        let client_task = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .enable_push(true)
                .handshake(io)
                .await
                .unwrap();
            let parent = sender
                .send_request(build_test_request())
                .unwrap();
            let mut pushes = Vec::new();
            for _ in 0..2 {
                pushes.push(
                    connection
                        .push()
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
            pushes
                .sort_by_key(|(_, response)| u32::from(response.stream_id()));
            let (head_request, head_response) = pushes.pop().unwrap();
            let (get_request, get_response) = pushes.pop().unwrap();
            assert_eq!(get_request.method(), &Method::GET);
            assert_eq!(
                get_request.into_message_head().0.uri(),
                &push_uri("/get")
            );
            assert_eq!(head_request.method(), &Method::HEAD);
            assert_eq!(
                head_request.into_message_head().0.uri(),
                &push_uri("/head")
            );
            assert_eq!(get_response.stream_id(), StreamId::from(2));
            assert_eq!(head_response.stream_id(), StreamId::from(4));
            let _driver = Driver(tokio::spawn(async move {
                let _ = connection.await;
            }));
            let ((get_head, mut body), (head_head, mut empty)) =
                tokio::join!(async { get_response.await.unwrap() }, async {
                    head_response.await.unwrap()
                },);
            assert_eq!(get_head.headers(), &field("x-pushed", "yes"));
            assert_eq!(head_head.status(), &StatusCode::OK);
            assert!(empty.frame().await.is_none());
            match body.frame().await.unwrap().unwrap() {
                BodyFrame::Data(bytes) => {
                    assert_eq!(bytes.as_ref(), b"raw push")
                }
                other => panic!("expected DATA, got {other:?}"),
            }
            match body.frame().await.unwrap().unwrap() {
                BodyFrame::Trailers(headers) => {
                    assert_eq!(headers, field("x-finished", "yes"))
                }
                other => panic!("expected trailers, got {other:?}"),
            }
            assert!(body.frame().await.is_none());
            parent.await.unwrap();
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn invalid_promised_or_parent_ids_are_connection_errors() {
    bounded(async {
        // Promised IDs must be unused even IDs; the parent must be an open request.
        for (parent_id, promised_id, reuse) in [
            (1, 3, false),
            (1, 2, true),
            (1, 2, false),
            (3, 2, false),
            (2, 4, false),
        ] {
            let (io, mut peer) = mock::new();
            let peer_task = async move {
                assert_eq!(
                    peer.assert_client_handshake()
                        .await
                        .is_push_enabled(),
                    Some(true)
                );
                peer.recv_frame(
                    frames::headers(1)
                        .request("GET", "https", "http2.akamai.com", "/")
                        .eos(),
                )
                .await;
                if reuse || (parent_id == 1 && promised_id == 2) {
                    let first_id = if reuse {
                        2
                    } else {
                        4
                    };
                    peer.send_frame(promise(
                        1,
                        first_id,
                        Method::GET,
                        "/first",
                    ))
                    .await;
                }
                peer.send_frame(promise(
                    parent_id,
                    promised_id,
                    Method::GET,
                    "/invalid",
                ))
                .await;
                peer.recv_frame(frames::go_away(0).protocol_error())
                    .await;
            };
            let client_task = async move {
                let (mut connection, mut sender) = ClientBuilder::new()
                    .enable_push(true)
                    .handshake(io)
                    .await
                    .unwrap();
                let _parent = sender
                    .send_request(build_test_request())
                    .unwrap();
                let mut delivered = Vec::new();
                loop {
                    match connection.push().await {
                        Some(Ok(push))
                            if (reuse
                                || (parent_id == 1 && promised_id == 2))
                                && delivered.is_empty() =>
                        {
                            delivered.push(push)
                        }
                        Some(Err(error)) => {
                            assert_eq!(
                                error.reason(),
                                Some(Reason::PROTOCOL_ERROR)
                            );
                            break;
                        }
                        other => panic!(
                            "invalid promise was not rejected: {other:?}"
                        ),
                    }
                }
            };
            join(peer_task, client_task).await;
        }
    })
    .await;
}

#[tokio::test]
async fn malformed_promised_request_is_stream_error() {
    bounded(async {
        for missing_scheme in [true, false] {
            let (io, mut peer) = mock::new();
            let peer_task = async move {
                peer.assert_client_handshake().await;
                peer.recv_frame(
                    frames::headers(1)
                        .request("GET", "https", "http2.akamai.com", "/")
                        .eos(),
                )
                .await;
                let mut pseudo =
                    Pseudo::request(Method::GET, push_uri("/malformed"), None);
                let mut fields = HeaderMap::new();
                if missing_scheme {
                    pseudo.scheme = None;
                } else {
                    fields.insert("connection", "keep-alive");
                }
                peer.send_frame(frame::PushPromise::new(
                    1.into(),
                    2.into(),
                    pseudo,
                    fields,
                ))
                .await;
                peer.recv_frame(frames::reset(2).protocol_error())
                    .await;
                peer.send_frame(frames::headers(1).response(200).eos())
                    .await;
                peer.recv_eof().await;
            };
            let client_task = async move {
                let (mut connection, mut sender) = ClientBuilder::new()
                    .enable_push(true)
                    .handshake(io)
                    .await
                    .unwrap();
                let parent = sender
                    .send_request(build_test_request())
                    .unwrap();
                // A malformed promise is not delivered, but must not poison its parent.
                assert_eq!(
                    connection
                        .drive(parent)
                        .await
                        .unwrap()
                        .status(),
                    &StatusCode::OK
                );
            };
            join(peer_task, client_task).await;
        }
    })
    .await;
}

#[tokio::test]
async fn reserved_promises_do_not_count_until_response_headers() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            let settings = peer.assert_client_handshake().await;
            assert_eq!(settings.max_concurrent_streams(), Some(1));
            peer.recv_frame(
                frames::headers(1)
                    .request("GET", "https", "http2.akamai.com", "/")
                    .eos(),
            )
            .await;
            peer.send_frame(promise(1, 2, Method::GET, "/active"))
                .await;
            peer.send_frame(promise(1, 4, Method::GET, "/excess"))
                .await;
            peer.send_frame(frames::headers(2).response(200))
                .await;
            peer.send_frame(frames::headers(4).response(200))
                .await;
            peer.recv_frame(frames::reset(4).refused())
                .await;
            peer.send_frame(frames::data(2, b"done").eos())
                .await;
            peer.send_frame(frames::headers(1).response(200).eos())
                .await;
            peer.recv_eof().await;
        };
        let client_task = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .enable_push(true)
                .max_concurrent_streams(1)
                .handshake(io)
                .await
                .unwrap();
            let parent = sender
                .send_request(build_test_request())
                .unwrap();
            let (_, first) = connection
                .push()
                .await
                .unwrap()
                .unwrap();
            let (_, second) = connection
                .push()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first.stream_id(), StreamId::from(2));
            assert_eq!(second.stream_id(), StreamId::from(4));
            let _driver = Driver(tokio::spawn(async move {
                let _ = connection.await;
            }));
            let (_, mut body) = first.await.unwrap();
            let error = second
                .await
                .err()
                .expect("excess pushed HEADERS should be refused");
            assert_eq!(error.reason(), Some(Reason::REFUSED_STREAM));
            match body.frame().await.unwrap().unwrap() {
                BodyFrame::Data(bytes) => assert_eq!(bytes.as_ref(), b"done"),
                other => panic!("expected DATA, got {other:?}"),
            }
            assert!(body.frame().await.is_none());
            parent.await.unwrap();
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn unsafe_promised_request_is_stream_error() {
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
            peer.send_frame(promise(1, 2, Method::POST, "/unsafe"))
                .await;
            peer.recv_frame(frames::reset(2).protocol_error())
                .await;
            peer.send_frame(frames::headers(1).response(200).eos())
                .await;
            peer.recv_eof().await;
        };
        let client_task = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .enable_push(true)
                .handshake(io)
                .await
                .unwrap();
            let parent = sender
                .send_request(build_test_request())
                .unwrap();
            assert_eq!(
                connection
                    .drive(parent)
                    .await
                    .unwrap()
                    .status(),
                &StatusCode::OK
            );
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn dropping_promised_response_cancels_reserved_stream() {
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
            peer.send_frame(promise(1, 2, Method::GET, "/cancel"))
                .await;
            peer.recv_frame(frames::reset(2).cancel())
                .await;
            peer.send_frame(frames::headers(1).response(200).eos())
                .await;
            peer.recv_eof().await;
        };
        let client_task = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .enable_push(true)
                .handshake(io)
                .await
                .unwrap();
            let parent = sender
                .send_request(build_test_request())
                .unwrap();
            let (_, response) = connection
                .push()
                .await
                .unwrap()
                .unwrap();
            drop(response);
            let _driver = Driver(tokio::spawn(async move {
                let _ = connection.await;
            }));
            parent.await.unwrap();
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn server_enable_push_setting_is_role_violation() {
    bounded(async {
        for initial in [true, false] {
            for enabled in [true, false] {
                let (io, mut peer) = mock::new();
                let peer_task = async move {
                    let mut settings = frame::Settings::default();
                    settings.set_enable_push(enabled);
                    if initial {
                        peer.send_frame(settings).await;
                        peer.read_preface().await.unwrap();
                        assert!(matches!(
                            peer.recv_frame_raw().await,
                            frame::Frame::Settings(_)
                        ));
                    } else {
                        peer.assert_client_handshake().await;
                        peer.send_frame(settings).await;
                    }
                    let first = peer.recv_frame_raw().await;
                    if matches!(&first, frame::Frame::Settings(settings) if settings.is_ack()) {
                        peer.recv_frame(frames::go_away(0).protocol_error()).await;
                    } else {
                        assert_frame_eq(first, frames::go_away(0).protocol_error());
                    }
                };
                let client_task = async move {
                    let (mut connection, _sender) = ClientBuilder::new()
                        .enable_push(true)
                        .handshake(io)
                        .await
                        .unwrap();
                    match connection.push().await {
                        Some(Err(error)) => assert_eq!(
                            error.reason(),
                            Some(Reason::PROTOCOL_ERROR)
                        ),
                        other => panic!(
                            "server ENABLE_PUSH was not rejected: {other:?}"
                        ),
                    }
                };
                join(peer_task, client_task).await;
            }
        }
    })
    .await;
}

#[tokio::test]
async fn late_promise_on_locally_reset_parent_is_cancelled() {
    bounded(async {
        for retained in [true, false] {
            let (io, mut peer) = mock::new();
            let peer_task = async move {
                peer.assert_client_handshake().await;
                peer.recv_frame(
                    frames::headers(1)
                        .request("GET", "https", "http2.akamai.com", "/")
                        .eos(),
                )
                .await;
                peer.recv_frame(frames::reset(1).cancel())
                    .await;
                peer.send_frame(promise(1, 2, Method::GET, "/late"))
                    .await;
                peer.recv_frame(frames::reset(2).cancel())
                    .await;
            };
            let client_task = async move {
                let builder = ClientBuilder::new().enable_push(true);
                let builder = if retained {
                    builder
                } else {
                    builder.max_concurrent_reset_streams(0)
                };
                let (mut connection, mut sender) =
                    builder.handshake(io).await.unwrap();
                let response = sender
                    .send_request(build_test_request())
                    .unwrap();
                // Drive the initial request before dropping its response.
                connection.run(future::ready(())).await;
                drop(response);
                let result = connection.push().await;
                assert!(result.is_none(), "late promise after local reset (retained={retained}) should be cancelled, got {result:?}");
            };
            join(peer_task, client_task).await;
        }
    })
    .await;
}

#[tokio::test]
async fn reserved_push_limit_covers_queued_and_handed_off_promises() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for handed_off in [false, true] {
            let (io, mut peer) = mock::new();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let peer_task = async move {
                peer.assert_client_handshake().await;
                peer.recv_frame(
                    frames::headers(1)
                        .request("GET", "https", "http2.akamai.com", "/")
                        .eos(),
                )
                .await;
                for index in 1..=1024 {
                    peer.send_frame(promise(
                        1,
                        index * 2,
                        Method::GET,
                        "/reserved",
                    ))
                    .await;
                }
                if handed_off {
                    ready_rx.await.unwrap();
                }
                peer.send_frame(promise(1, 2050, Method::GET, "/excess"))
                    .await;
                peer.recv_frame(frames::go_away(0).calm())
                    .await;
            };
            let client_task = async move {
                let (mut connection, mut sender) = ClientBuilder::new()
                    .enable_push(true)
                    .handshake(io)
                    .await
                    .unwrap();
                let _parent = sender
                    .send_request(build_test_request())
                    .unwrap();
                let mut held = Vec::new();
                if handed_off {
                    for index in 1..=1024 {
                        let push = connection
                            .push()
                            .await
                            .unwrap()
                            .unwrap();
                        assert_eq!(u32::from(push.1.stream_id()), index * 2);
                        held.push(push);
                    }
                    ready_tx.send(()).unwrap();
                    match connection.push().await {
                        Some(Err(error)) => assert_eq!(
                            error.reason(),
                            Some(Reason::ENHANCE_YOUR_CALM)
                        ),
                        other => {
                            panic!("reservation limit not enforced: {other:?}")
                        }
                    }
                } else {
                    let error = connection.await.err().expect(
                        "queued reservation limit must fail connection",
                    );
                    assert_eq!(
                        error.reason(),
                        Some(Reason::ENHANCE_YOUR_CALM)
                    );
                }
            };
            join(peer_task, client_task).await;
        }
    })
    .await
    .expect("reserved push limit test stalled");
}

#[tokio::test]
async fn zero_promised_id_is_connection_error() {
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
            // PUSH_PROMISE, END_HEADERS, parent 1, promised ID 0. ID validation
            // precedes decoding the empty header block.
            peer.send_bytes(&[0, 0, 4, 5, 4, 0, 0, 0, 1, 0, 0, 0, 0])
                .await;
            peer.recv_frame(frames::go_away(0).protocol_error())
                .await;
        };
        let client_task = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .enable_push(true)
                .handshake(io)
                .await
                .unwrap();
            let _parent = sender
                .send_request(build_test_request())
                .unwrap();
            match connection.push().await {
                Some(Err(error)) => {
                    assert_eq!(error.reason(), Some(Reason::PROTOCOL_ERROR))
                }
                other => {
                    panic!("zero promised ID was not rejected: {other:?}")
                }
            }
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn informational_push_headers_count_activation_once() {
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
            peer.send_frame(promise(1, 2, Method::GET, "/first"))
                .await;
            peer.send_frame(promise(1, 4, Method::GET, "/second"))
                .await;
            peer.send_frame(frames::headers(2).response(103))
                .await;
            peer.send_frame(frames::headers(2).response(100))
                .await;
            peer.send_frame(frames::headers(4).response(103))
                .await;
            peer.recv_frame(frames::reset(4).refused())
                .await;
            // The final head is not a second activation of stream 2.
            peer.send_frame(frames::headers(2).response(200).eos())
                .await;
            peer.send_frame(promise(1, 6, Method::GET, "/third"))
                .await;
            peer.send_frame(frames::headers(6).response(103))
                .await;
            peer.send_frame(frames::headers(6).response(200).eos())
                .await;
            peer.send_frame(frames::headers(1).response(200).eos())
                .await;
            peer.recv_eof().await;
        };
        let client_task = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .enable_push(true)
                .max_concurrent_streams(1)
                .handshake(io)
                .await
                .unwrap();
            let parent = sender
                .send_request(build_test_request())
                .unwrap();
            let (_, first) = connection
                .push()
                .await
                .unwrap()
                .unwrap();
            let (_, second) = connection
                .push()
                .await
                .unwrap()
                .unwrap();
            let (_, third) = connection
                .push()
                .await
                .unwrap()
                .unwrap();
            let _driver = Driver(tokio::spawn(async move {
                let _ = connection.await;
            }));
            assert_eq!(first.await.unwrap().0.status(), &StatusCode::OK);
            assert_eq!(
                second.await.err().unwrap().reason(),
                Some(Reason::REFUSED_STREAM)
            );
            assert_eq!(third.await.unwrap().0.status(), &StatusCode::OK);
            parent.await.unwrap();
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn server_goaway_odd_cutoff_preserves_existing_pushed_response() {
    bounded(async {
        let (io, mut peer) = mock::new();
        let (prefix_tx, prefix_rx) = tokio::sync::oneshot::channel();
        let peer_task = async move {
            peer.assert_client_handshake().await;
            peer.recv_frame(
                frames::headers(1)
                    .request("GET", "https", "http2.akamai.com", "/")
                    .eos(),
            )
            .await;
            peer.send_frame(promise(1, 2, Method::GET, "/survives-goaway"))
                .await;
            peer.send_frame(frames::headers(2).response(200))
                .await;
            peer.send_frame(frames::data(2, b"before "))
                .await;
            // Ensure the application has accepted the pushed response before GOAWAY.
            prefix_rx.await.unwrap();
            peer.send_frame(frames::go_away(1).no_error())
                .await;
            peer.send_frame(frames::data(2, b"after").eos())
                .await;
            peer.send_frame(frames::headers(1).response(200).eos())
                .await;
            // The client may send its own graceful GOAWAY as the last streams finish.
            while let Some(frame) = peer.next().await {
                match frame.unwrap() {
                    frame::Frame::GoAway(goaway) => {
                        assert_eq!(goaway.reason(), Reason::NO_ERROR)
                    }
                    other => {
                        panic!("unexpected frame while closing: {other:?}")
                    }
                }
            }
        };
        let client_task = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .enable_push(true)
                .handshake(io)
                .await
                .unwrap();
            let parent = sender
                .send_request(build_test_request())
                .unwrap();
            let (_, response) = connection
                .push()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.stream_id(), StreamId::from(2));
            let _driver = Driver(tokio::spawn(async move {
                let _ = connection.await;
            }));
            let (head, mut body) = response.await.unwrap();
            assert_eq!(head.status(), &StatusCode::OK);
            match body.frame().await.unwrap().unwrap() {
                BodyFrame::Data(bytes) => {
                    assert_eq!(bytes.as_ref(), b"before ")
                }
                other => panic!("expected body prefix, got {other:?}"),
            }
            prefix_tx.send(()).unwrap();
            match body.frame().await.unwrap().unwrap() {
                BodyFrame::Data(bytes) => assert_eq!(bytes.as_ref(), b"after"),
                other => panic!(
                    "expected remaining body after GOAWAY, got {other:?}"
                ),
            }
            assert!(body.frame().await.is_none());
            assert_eq!(parent.await.unwrap().status(), &StatusCode::OK);
        };
        join(peer_task, client_task).await;
    })
    .await;
}

#[tokio::test]
async fn push_waiter_ends_on_eof_and_reports_connection_error() {
    bounded(async {
        for protocol_error in [false, true] {
            let (io, mut peer) = mock::new();
            let peer_task = async move {
                peer.assert_client_handshake().await;
                if protocol_error {
                    peer.send_bytes(&[0, 0, 1, 0, 0, 0, 0, 0, 0, b'x'])
                        .await;
                    peer.recv_frame(frames::go_away(0).protocol_error())
                        .await;
                }
            };
            let client_task = async move {
                let (mut connection, _sender) = ClientBuilder::new()
                    .enable_push(true)
                    .handshake(io)
                    .await
                    .unwrap();
                let result = connection.push().await;
                if protocol_error {
                    match result {
                        Some(Err(error)) => assert_eq!(
                            error.reason(),
                            Some(Reason::PROTOCOL_ERROR)
                        ),
                        other => panic!(
                            "waiting push lost connection error: {other:?}"
                        ),
                    }
                } else {
                    assert!(result.is_none(), "EOF should end the push queue");
                }
            };
            join(peer_task, client_task).await;
        }
    })
    .await;
}
