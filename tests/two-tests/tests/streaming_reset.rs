use support::prelude::*;
use two_plz::message::BodyFrame;

#[derive(Clone, Copy)]
enum Completion {
    Data,
    Trailers,
    Headers,
    Incomplete,
}

async fn response_then_reset(
    completion: Completion,
    head_first: bool,
    reason: Reason,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (io, mut srv) = mock::new();
        let (head_tx, head_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let client_fut = async move {
            let (mut conn, mut client) = ClientBuilder::new().handshake(io).await.unwrap();
            conn.drive(ready_rx).await.unwrap();
            let (response, mut upload) = client
                .send_request_streaming(build_test_request(), false)
                .unwrap();
            let response = conn.drive(response).await;
            let succeeds = !matches!(completion, Completion::Incomplete) && reason == Reason::NO_ERROR;
            if succeeds || head_first {
                let (head, mut body) = response.unwrap();
                assert_eq!(head.status(), &StatusCode::OK);
                if head_first {
                    head_tx.send(()).unwrap();
                }
                if !succeeds {
                    let error = conn.drive(body.frame()).await.unwrap().unwrap_err();
                    assert_eq!(error.reason(), Some(reason));
                    assert!(body.frame().await.is_none());
                } else {
                    if !matches!(completion, Completion::Headers) {
                        match conn.drive(body.frame()).await.unwrap().unwrap() {
                            BodyFrame::Data(data) => assert_eq!(data, Bytes::from_static(b"response")),
                            BodyFrame::Trailers(_) => panic!("DATA must precede trailers"),
                        }
                    }
                    if matches!(completion, Completion::Trailers) {
                        let mut expected = HeaderMap::new();
                        expected.insert("x-finished", "yes");
                        match conn.drive(body.frame()).await.unwrap().unwrap() {
                            BodyFrame::Trailers(actual) => assert_eq!(actual, expected),
                            BodyFrame::Data(_) => panic!("expected trailers"),
                        }
                    }
                    assert!(conn.drive(body.frame()).await.is_none());
                    assert!(body.frame().await.is_none());
                }
            } else {
                assert_eq!(response.unwrap_err().reason(), Some(reason));
            }
            assert!(conn
                .drive(upload.send_data(Bytes::from_static(b"more upload"), false))
                .await
                .is_err(), "reset must cancel the upload");
        };
        let server_fut = async move {
            srv.assert_client_handshake().await;
            ready_tx.send(()).unwrap();
            srv.recv_frame(frames::headers(1).request("GET", "https", "http2.akamai.com", "/")).await;
            if matches!(completion, Completion::Headers) {
                srv.send_frame(frames::headers(1).response(200).eos()).await;
            } else {
                srv.send_frame(frames::headers(1).response(200)).await;
            }
            if head_first {
                head_rx.await.unwrap();
            }
            // These mock sends do not yield: completion and reset reach the
            // connection in the same read, before the application is polled.
            match completion {
                Completion::Data => srv.send_frame(frames::data(1, b"response").eos()).await,
                Completion::Trailers => {
                    srv.send_frame(frames::data(1, b"response")).await;
                    srv.send_frame(frames::headers(1).field("x-finished", "yes").eos()).await;
                }
                Completion::Incomplete => srv.send_frame(frames::data(1, b"response")).await,
                Completion::Headers => {}
            }
            srv.send_frame(frames::reset(1).reason(reason)).await;
            // Keep the transport alive until all application assertions finish.
            futures::future::pending::<()>().await;
        };
        // The server stays alive while the client consumes the queued frames.
        tokio::pin!(server_fut);
        tokio::pin!(client_fut);
        tokio::select! {
            _ = &mut client_fut => {}
            _ = &mut server_fut => panic!("server stopped before client assertions"),
        }
    }).await.expect("streaming reset stalled");
}

async fn connection_error_after_completed_response(eof: bool) {
    for completion in
        [Completion::Data, Completion::Trailers, Completion::Headers]
    {
        for head_first in [false, true] {
            completed_response_survives_later_event(
                completion,
                head_first,
                None,
                Some(eof),
            )
            .await;
        }
    }
}

async fn completed_response_survives_later_event(
    completion: Completion,
    head_first: bool,
    second_reset: Option<Reason>,
    termination: Option<bool>,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (io, mut srv) = mock::new();
        let (event_tx, event_rx) = tokio::sync::oneshot::channel();
        let (processed_tx, processed_rx) = tokio::sync::oneshot::channel();
        let (terminated_tx, terminated_rx) = tokio::sync::oneshot::channel();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (head_tx, head_rx) = tokio::sync::oneshot::channel();
        let client_fut = async move {
            let (mut conn, mut client) = ClientBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            conn.drive(ready_rx).await.unwrap();
            let (response, mut upload) = client
                .send_request_streaming(build_test_request(), false)
                .unwrap();
            let mut response = Some(response);
            let delivered = if head_first {
                let delivered = conn
                    .drive(response.take().unwrap())
                    .await
                    .unwrap();
                head_tx.send(()).unwrap();
                Some(delivered)
            } else {
                None
            };
            conn.drive(event_rx).await.unwrap();
            if termination == Some(true) {
                // Process completion and reset before injecting an I/O error.
                assert!(
                    poll_fn(|cx| Pin::new(&mut conn).poll(cx))
                        .now_or_never()
                        .is_none()
                );
                processed_tx.send(()).unwrap();
                terminated_rx.await.unwrap();
            }
            if termination.is_some() {
                assert!((&mut conn).await.is_err());
            } else {
                assert!(
                    poll_fn(|cx| Pin::new(&mut conn).poll(cx))
                        .now_or_never()
                        .is_none()
                );
            }
            let (head, mut body) = match delivered {
                Some(delivered) => delivered,
                None => response.take().unwrap().await.unwrap(),
            };
            assert_eq!(head.status(), &StatusCode::OK);
            if !matches!(completion, Completion::Headers) {
                match body.frame().await.unwrap().unwrap() {
                    BodyFrame::Data(data) => {
                        assert_eq!(data, Bytes::from_static(b"response"))
                    }
                    BodyFrame::Trailers(_) => {
                        panic!("DATA must precede trailers")
                    }
                }
            }
            if matches!(completion, Completion::Trailers) {
                let mut expected = HeaderMap::new();
                expected.insert("x-finished", "yes");
                match body.frame().await.unwrap().unwrap() {
                    BodyFrame::Trailers(actual) => {
                        assert_eq!(actual, expected)
                    }
                    BodyFrame::Data(_) => panic!("expected trailers"),
                }
            }
            assert!(body.frame().await.is_none());
            assert!(body.frame().await.is_none());
            assert!(
                upload
                    .send_data(Bytes::from_static(b"more upload"), false)
                    .await
                    .is_err()
            );
            if termination.is_some() {
                assert!(
                    client
                        .send_request_streaming(build_test_request(), false)
                        .is_err()
                );
            }
        };
        let server_fut = async move {
            srv.assert_client_handshake().await;
            ready_tx.send(()).unwrap();
            srv.recv_frame(frames::headers(1).request(
                "GET",
                "https",
                "http2.akamai.com",
                "/",
            ))
            .await;
            if matches!(completion, Completion::Headers) {
                srv.send_frame(frames::headers(1).response(200).eos())
                    .await;
            } else {
                srv.send_frame(frames::headers(1).response(200))
                    .await;
            }
            if head_first {
                head_rx.await.unwrap();
            }
            match completion {
                Completion::Data => {
                    srv.send_frame(frames::data(1, b"response").eos())
                        .await
                }
                Completion::Trailers => {
                    srv.send_frame(frames::data(1, b"response"))
                        .await;
                    srv.send_frame(
                        frames::headers(1)
                            .field("x-finished", "yes")
                            .eos(),
                    )
                    .await;
                }
                Completion::Headers => {}
                Completion::Incomplete => unreachable!(),
            }
            srv.send_frame(frames::reset(1)).await;
            if let Some(reason) = second_reset {
                srv.send_frame(frames::reset(1).reason(reason))
                    .await;
            }
            if let Some(eof) = termination {
                if eof {
                    event_tx.send(()).unwrap();
                    processed_rx.await.unwrap();
                    srv.close_without_notify();
                    terminated_tx.send(()).unwrap();
                    futures::future::pending::<()>().await;
                    return;
                }
                // DATA on stream zero is a connection-level protocol error.
                tokio::io::AsyncWriteExt::write_all(
                    &mut srv,
                    &[0, 0, 1, 0, 0, 0, 0, 0, 0, 1],
                )
                .await
                .unwrap();
            }
            event_tx.send(()).unwrap();
            futures::future::pending::<()>().await;
        };
        tokio::pin!(client_fut);
        tokio::pin!(server_fut);
        tokio::select! {
            _ = &mut client_fut => {}
            _ = &mut server_fut => client_fut.await,
        }
    })
    .await
    .expect("connection error stalled");
}

#[tokio::test]
async fn completed_response_reset_does_not_hide_connection_eof() {
    connection_error_after_completed_response(true).await;
}

#[tokio::test]
async fn completed_response_reset_does_not_hide_connection_protocol_error() {
    connection_error_after_completed_response(false).await;
}

#[tokio::test]
async fn completed_response_survives_second_no_error_reset() {
    for completion in
        [Completion::Data, Completion::Trailers, Completion::Headers]
    {
        for head_first in [false, true] {
            completed_response_survives_later_event(
                completion,
                head_first,
                Some(Reason::NO_ERROR),
                None,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn completed_response_survives_second_cancel_reset() {
    for completion in
        [Completion::Data, Completion::Trailers, Completion::Headers]
    {
        for head_first in [false, true] {
            completed_response_survives_later_event(
                completion,
                head_first,
                Some(Reason::CANCEL),
                None,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn completed_data_before_no_error_reset() {
    response_then_reset(Completion::Data, false, Reason::NO_ERROR).await;
}

#[tokio::test]
async fn delivered_head_then_completed_data_before_no_error_reset() {
    response_then_reset(Completion::Data, true, Reason::NO_ERROR).await;
}

#[tokio::test]
async fn completed_trailers_before_no_error_reset() {
    response_then_reset(Completion::Trailers, false, Reason::NO_ERROR).await;
}

#[tokio::test]
async fn completed_head_only_before_no_error_reset() {
    response_then_reset(Completion::Headers, false, Reason::NO_ERROR).await;
}

#[tokio::test]
async fn incomplete_response_before_no_error_reset_is_error() {
    response_then_reset(Completion::Incomplete, false, Reason::NO_ERROR).await;
}

#[tokio::test]
async fn delivered_head_then_incomplete_response_reset_is_error() {
    response_then_reset(Completion::Incomplete, true, Reason::NO_ERROR).await;
}

#[tokio::test]
async fn completed_response_before_other_reset_is_error() {
    response_then_reset(Completion::Data, false, Reason::CANCEL).await;
}

#[tokio::test]
async fn delivered_head_then_completed_response_other_reset_is_error() {
    response_then_reset(Completion::Data, true, Reason::CANCEL).await;
}
