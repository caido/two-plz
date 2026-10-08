use support::prelude::*;

#[tokio::test]
async fn reserved_remote_data_is_connection_error_but_reset_preserves_parent()
{
    for reset in [false, true] {
        let (io, mut peer) = mock::new();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let client = async move {
            let (mut conn, mut client) = ClientBuilder::new()
                .enable_push(true)
                .handshake(io)
                .await
                .unwrap();
            conn.drive(ready_rx).await.unwrap();
            let response = client
                .send_request(build_test_request())
                .unwrap();
            if reset {
                assert_eq!(
                    *conn
                        .drive(response)
                        .await
                        .unwrap()
                        .status(),
                    200
                );
                drop(client);
                conn.await.unwrap();
            } else {
                assert!(conn.drive(response).await.is_err());
                assert!(conn.await.is_err());
            }
        };
        let peer = async move {
            peer.assert_client_handshake_with_settings(frames::settings())
                .await;
            ready_tx.send(()).unwrap();
            peer.recv_frame(
                frames::headers(1)
                    .request("GET", "https", "http2.akamai.com", "/")
                    .eos(),
            )
            .await;
            let uri = Uri::builder()
                .scheme(Scheme::HTTPS)
                .authority("example.com")
                .path("/push")
                .build()
                .unwrap();
            peer.send_frame(frame::PushPromise::new(
                1.into(),
                2.into(),
                Pseudo::request(Method::GET, uri, None),
                HeaderMap::new(),
            ))
            .await;
            if reset {
                peer.send_frame(frames::reset(2).cancel())
                    .await;
                peer.send_frame(frames::ping(*b"reserve!"))
                    .await;
                peer.recv_frame(frames::ping(*b"reserve!").pong())
                    .await;
                peer.send_frame(frames::headers(1).response(200).eos())
                    .await;
            } else {
                peer.send_frame(frames::data(2, "invalid"))
                    .await;
                peer.recv_frame(frames::go_away(0).protocol_error())
                    .await;
            }
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            join(client, peer),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn reserved_local_data_is_connection_error_but_reset_preserves_parent() {
    for reset in [false, true] {
        let (io, mut peer) = mock::new();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let uri = Uri::builder()
            .scheme(Scheme::HTTPS)
            .authority("example.com")
            .path("/push")
            .build()
            .unwrap();
        let promise_uri = uri.clone();
        let server = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut parent) = server.accept().await.unwrap().unwrap();
            let mut pushed = parent
                .push_request(
                    Request::builder()
                        .method(Method::GET)
                        .uri(uri)
                        .build(),
                )
                .unwrap();
            if reset {
                poll_fn(|cx| server.poll_closed(cx))
                    .drive(done_rx)
                    .await
                    .unwrap();
                assert!(
                    pushed
                        .send_response(build_test_response())
                        .is_err()
                );
                parent
                    .send_response(build_test_response())
                    .unwrap();
                poll_fn(|cx| server.poll_closed(cx))
                    .await
                    .unwrap();
            } else {
                assert!(
                    poll_fn(|cx| server.poll_closed(cx))
                        .await
                        .is_err()
                );
            }
        };
        let peer = async move {
            peer.assert_server_handshake().await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(frame::PushPromise::new(
                1.into(),
                2.into(),
                Pseudo::request(Method::GET, promise_uri, None),
                HeaderMap::new(),
            ))
            .await;
            if reset {
                peer.send_frame(frames::reset(2).cancel())
                    .await;
                peer.send_frame(frames::ping(*b"reserve!"))
                    .await;
                peer.recv_frame(frames::ping(*b"reserve!").pong())
                    .await;
                done_tx.send(()).unwrap();
                peer.recv_frame(frames::headers(1).response(200).eos())
                    .await;
            } else {
                peer.send_frame(frames::data(2, "invalid"))
                    .await;
                peer.recv_frame(frames::go_away(1).protocol_error())
                    .await;
            }
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            join(server, peer),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn data_after_end_stream_is_stream_local_and_sibling_survives() {
    for removed in [false, true] {
        let (io, mut peer) = mock::new();
        let server = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let (_, mut first) = server.accept().await.unwrap().unwrap();
            if removed {
                first
                    .send_response(build_test_response())
                    .unwrap();
                drop(first);
            } else {
                // Hold the half-closed(remote) entry while driving rejection.
                let (_, mut sibling) = server.accept().await.unwrap().unwrap();
                sibling
                    .send_response(build_test_response())
                    .unwrap();
                assert!(
                    first
                        .send_response(build_test_response())
                        .is_err()
                );
                poll_fn(|cx| server.poll_closed(cx))
                    .await
                    .unwrap();
                return;
            }
            let (_, mut sibling) = server.accept().await.unwrap().unwrap();
            sibling
                .send_response(build_test_response())
                .unwrap();
            poll_fn(|cx| server.poll_closed(cx))
                .await
                .unwrap();
        };
        let peer = async move {
            peer.assert_server_handshake().await;
            peer.send_frame(
                frames::headers(1)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;
            if removed {
                peer.recv_frame(frames::headers(1).response(200).eos())
                    .await;
            }
            peer.send_frame(frames::data(1, "rejected"))
                .await;
            peer.recv_frame(frames::reset(1).stream_closed())
                .await;
            peer.send_frame(frames::ping(*b"credit!!"))
                .await;
            peer.recv_frame(frames::ping(*b"credit!!").pong())
                .await;
            peer.send_frame(
                frames::headers(3)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;
            peer.recv_frame(frames::headers(3).response(200).eos())
                .await;
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            join(server, peer),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn absent_reset_below_opened_remote_id_is_ignored() {
    let (io, mut peer) = mock::new();
    let server = async move {
        let mut server = ServerBuilder::new()
            .handshake(io)
            .await
            .unwrap();
        let (_, mut response) = server.accept().await.unwrap().unwrap();
        response
            .send_response(build_test_response())
            .unwrap();
        poll_fn(|cx| server.poll_closed(cx))
            .await
            .unwrap();
    };
    let peer = async move {
        peer.assert_server_handshake().await;
        peer.send_frame(
            frames::headers(3)
                .request("GET", "https", "a.b", "/")
                .eos(),
        )
        .await;
        peer.recv_frame(frames::headers(3).response(200).eos())
            .await;
        for id in [1, 3] {
            peer.send_frame(frames::reset(id).cancel())
                .await;
        }
        peer.send_frame(frames::ping(*b"history!"))
            .await;
        peer.recv_frame(frames::ping(*b"history!").pong())
            .await;
    };
    join(server, peer).await;
}

#[tokio::test]
async fn client_idle_data_reset_and_data_before_headers_are_connection_errors()
{
    for (id, reset, request) in [
        (1, false, false),
        (2, false, false),
        (5, false, false),
        (1, true, false),
        (2, true, false),
        (5, true, false),
        (1, false, true),
    ] {
        let (io, mut peer) = mock::new();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let client = async move {
            let (mut conn, mut client) = ClientBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            conn.drive(ready_rx).await.unwrap();
            let response = if request {
                Some(
                    client
                        .send_request(build_test_request())
                        .unwrap(),
                )
            } else {
                None
            };
            assert!(conn.await.is_err());
            if let Some(response) = response {
                assert!(response.await.is_err());
            }
        };
        let peer = async move {
            peer.assert_client_handshake_with_settings(frames::settings())
                .await;
            ready_tx.send(()).unwrap();
            if request {
                peer.recv_frame(
                    frames::headers(1)
                        .request("GET", "https", "http2.akamai.com", "/")
                        .eos(),
                )
                .await;
            }
            if reset {
                peer.send_frame(frames::reset(id).cancel())
                    .await;
            } else {
                peer.send_frame(frames::data(id, "x"))
                    .await;
            }
            peer.recv_frame(frames::go_away(0).protocol_error())
                .await;
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            join(client, peer),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn data_and_reset_on_idle_stream_are_connection_errors() {
    for reset in [false, true] {
        for id in [1, 2, 5] {
            let (io, mut peer) = mock::new();
            let server = async move {
                let mut server = ServerBuilder::new()
                    .handshake(io)
                    .await
                    .unwrap();
                assert!(
                    poll_fn(|cx| server.poll_closed(cx))
                        .await
                        .is_err()
                );
                assert_eq!(server.num_wired_streams(), 0);
            };
            let peer = async move {
                peer.assert_server_handshake().await;
                if reset {
                    peer.send_frame(frames::reset(id).cancel())
                        .await;
                } else {
                    peer.send_frame(frames::data(id, "x"))
                        .await;
                }
                peer.recv_frame(frames::go_away(0).protocol_error())
                    .await;
            };
            join(server, peer).await;
        }
    }
}
