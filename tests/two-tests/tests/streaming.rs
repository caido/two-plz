use support::prelude::*;
use two_plz::message::{BodyFrame, RecvBody};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const LARGE_BODY_LEN: usize = 256 * 1024;

// Drivers are aborted even if a test assertion fails.
struct Driver(tokio::task::JoinHandle<()>);

impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn connection_pair() -> (
    client::SendRequest,
    server::ServerConnection<tokio::io::DuplexStream>,
    Driver,
) {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (client_result, server_result) = tokio::join!(
        ClientBuilder::new().handshake(client_io),
        ServerBuilder::new().handshake(server_io),
    );
    let (connection, sender) = client_result.unwrap();
    let server = server_result.unwrap();
    let driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    (sender, server, driver)
}

fn drive_server(
    mut server: server::ServerConnection<tokio::io::DuplexStream>,
) -> Driver {
    Driver(tokio::spawn(async move {
        let _ = poll_fn(|cx| server.poll_closed(cx)).await;
    }))
}

async fn collect_data(body: &mut RecvBody) -> Vec<u8> {
    let mut received = Vec::new();
    while let Some(frame) = body.frame().await {
        match frame.unwrap() {
            BodyFrame::Data(data) => received.extend_from_slice(&data),
            BodyFrame::Trailers(_) => panic!("unexpected trailers"),
        }
    }
    received
}

#[tokio::test]
async fn request_and_response_headers_arrive_before_body_end() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (response, mut request_sender) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();

        // No request DATA or END_STREAM has been sent: accepting must only need headers.
        let (request, mut request_body, mut respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        assert!(request.body_as_ref().is_none());
        let _server_driver = drive_server(server);
        let mut response_sender = respond
            .send_response_streaming(build_test_response(), false)
            .unwrap();

        // Likewise, receiving the response head must not wait for response DATA.
        let (head, mut response_body) = response.await.unwrap();
        assert_eq!(head.status(), &StatusCode::OK);
        assert!(head.body_as_ref().is_none());
        assert!(
            request_body
                .frame()
                .now_or_never()
                .is_none()
        );
        assert!(
            response_body
                .frame()
                .now_or_never()
                .is_none()
        );

        request_sender
            .send_data(Bytes::from_static(b"request"), true)
            .await
            .unwrap();
        response_sender
            .send_data(Bytes::from_static(b"response"), true)
            .await
            .unwrap();
        assert_eq!(collect_data(&mut request_body).await, b"request");
        assert_eq!(collect_data(&mut response_body).await, b"response");
        assert!(request_body.frame().await.is_none());
        assert!(response_body.frame().await.is_none());
    })
    .await
    .expect("headers or body stalled");
}

#[tokio::test]
async fn large_request_and_response_cross_flow_control_windows() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (response, mut request_sender) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, mut request_body, mut respond) = server.accept_streaming().await.unwrap().unwrap();
        let _server_driver = drive_server(server);
        let mut response_sender = respond
            .send_response_streaming(build_test_response(), false)
            .unwrap();
        let (_, mut response_body) = response.await.unwrap();

        // Each direction exceeds both the default stream and connection windows (65,535).
        let request_data = Bytes::from(vec![0x31; LARGE_BODY_LEN]);
        let response_data = Bytes::from(vec![0x72; LARGE_BODY_LEN]);
        let (request_sent, response_sent, request_received, response_received) = tokio::join!(
            request_sender.send_data(request_data.clone(), true),
            response_sender.send_data(response_data.clone(), true),
            collect_data(&mut request_body),
            collect_data(&mut response_body),
        );
        request_sent.unwrap();
        response_sent.unwrap();
        assert_eq!(request_received.as_slice(), request_data.as_ref());
        assert_eq!(response_received.as_slice(), response_data.as_ref());
    }).await.expect("large streaming transfer stalled");
}

#[tokio::test]
async fn request_and_response_trailers_follow_data_and_end_body() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (response, mut request_sender) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, mut request_body, mut respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let _server_driver = drive_server(server);
        let mut response_sender = respond
            .send_response_streaming(build_test_response(), false)
            .unwrap();
        let (_, mut response_body) = response.await.unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", "verified");

        request_sender
            .send_data(Bytes::from_static(b"request"), false)
            .await
            .unwrap();
        request_sender
            .send_trailers(trailers.clone())
            .unwrap();
        response_sender
            .send_data(Bytes::from_static(b"response"), false)
            .await
            .unwrap();
        response_sender
            .send_trailers(trailers.clone())
            .unwrap();

        for (body, expected) in [
            (&mut request_body, &b"request"[..]),
            (&mut response_body, &b"response"[..]),
        ] {
            let mut data = Vec::new();
            loop {
                match body
                    .frame()
                    .await
                    .expect("body ended before trailers")
                    .unwrap()
                {
                    BodyFrame::Data(chunk) => data.extend_from_slice(&chunk),
                    BodyFrame::Trailers(actual) => {
                        assert_eq!(actual, trailers);
                        break;
                    }
                }
            }
            assert_eq!(data, expected);
            assert!(body.frame().await.is_none());
            assert!(body.frame().await.is_none());
        }
    })
    .await
    .expect("streaming trailers stalled");
}

#[tokio::test]
async fn consuming_body_frames_unblocks_backpressured_sender() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (response, mut request_sender) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, mut request_body, mut respond) = server.accept_streaming().await.unwrap().unwrap();
        let _server_driver = drive_server(server);
        let _response_sender = respond
            .send_response_streaming(build_test_response(), true)
            .unwrap();
        let (_, mut response_body) = response.await.unwrap();
        assert!(response_body.frame().await.is_none());

        // Keep the same send future alive across the timeout: canceling send_data
        // can leave a queued prefix, so recreating it would duplicate bytes.
        let payload = Bytes::from(vec![0xa5; 1024 * 1024]);
        let send = request_sender.send_data(payload.clone(), true);
        tokio::pin!(send);
        assert!(tokio::time::timeout(Duration::from_millis(100), &mut send).await.is_err(),
            "sender completed while the receiver consumed no flow-control capacity");

        let (sent, received) = tokio::join!(send, collect_data(&mut request_body));
        sent.unwrap();
        assert_eq!(received.as_slice(), payload.as_ref());
    }).await.expect("consumption did not unblock sender");
}

#[tokio::test]
async fn dropping_unfinished_upload_cancels_peer_body() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (_response, upload) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, mut body, _respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let _server_driver = drive_server(server);
        assert!(body.frame().now_or_never().is_none());
        drop(upload);
        assert!(
            body.frame()
                .await
                .expect("abandoned upload ended without an error")
                .is_err()
        );
        assert!(body.frame().await.is_none());
    })
    .await
    .expect("dropping upload did not cancel peer body");
}

#[tokio::test]
async fn dropping_unfinished_receive_body_wakes_pending_sender_with_error() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (_response, mut upload) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, body, _respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let _server_driver = drive_server(server);
        let send =
            upload.send_data(Bytes::from(vec![0x45; 1024 * 1024]), true);
        tokio::pin!(send);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut send)
                .await
                .is_err()
        );
        drop(body);
        assert!(
            send.await.is_err(),
            "abandoned receiver did not fail pending send"
        );
    })
    .await
    .expect("dropping receive body did not wake sender");
}

#[tokio::test]
async fn queued_final_data_survives_send_body_drop() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (_response, mut upload) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, mut body, _respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        // Do not drive the server while the final bytes are queued and the sender is dropped.
        upload
            .send_data(Bytes::from_static(b"final data"), true)
            .await
            .unwrap();
        drop(upload);
        let _server_driver = drive_server(server);
        assert_eq!(collect_data(&mut body).await, b"final data");
        assert!(body.frame().await.is_none());
    })
    .await
    .expect("dropping completed sender lost final data");
}

#[tokio::test]
async fn queued_trailers_survive_send_body_drop() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (_response, mut upload) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, mut body, _respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        upload
            .send_data(Bytes::from_static(b"before trailers"), false)
            .await
            .unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert("x-finished", "yes");
        upload
            .send_trailers(trailers.clone())
            .unwrap();
        drop(upload);
        let _server_driver = drive_server(server);
        let mut received = Vec::new();
        loop {
            match body
                .frame()
                .await
                .expect("trailers were lost")
                .unwrap()
            {
                BodyFrame::Data(data) => received.extend_from_slice(&data),
                BodyFrame::Trailers(actual) => {
                    assert_eq!(actual, trailers);
                    break;
                }
            }
        }
        assert_eq!(received, b"before trailers");
        assert!(body.frame().await.is_none());
    })
    .await
    .expect("dropping completed sender lost trailers");
}

#[tokio::test]
async fn server_rejects_mixing_acceptance_modes_in_both_directions() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        for streaming_first in [false, true] {
            let (_client, mut server, _client_driver) =
                connection_pair().await;
            // A pending poll fixes the acceptance mode, even before any request arrives.
            if streaming_first {
                assert!(
                    server
                        .accept_streaming()
                        .now_or_never()
                        .is_none()
                );
                assert!(
                    server
                        .accept()
                        .await
                        .expect("connection unexpectedly closed")
                        .is_err()
                );
            } else {
                assert!(server.accept().now_or_never().is_none());
                assert!(
                    server
                        .accept_streaming()
                        .await
                        .expect("connection unexpectedly closed")
                        .is_err()
                );
            }
        }
    })
    .await
    .expect("mixed acceptance modes did not fail promptly");
}

#[tokio::test]
async fn connection_eof_wakes_pending_sender_with_error() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (_response, mut upload) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, body, respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let send =
            upload.send_data(Bytes::from(vec![0x65; 1024 * 1024]), true);
        tokio::pin!(send);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut send)
                .await
                .is_err()
        );
        // Keep the receive handles alive so this tests transport EOF, not their cancellation.
        drop(server);
        assert!(
            send.await.is_err(),
            "connection EOF did not fail pending send"
        );
        drop(body);
        drop(respond);
    })
    .await
    .expect("connection EOF did not wake pending sender");
}

#[tokio::test]
async fn dropping_response_future_with_upload_alive_cancels_pending_response_sender()
 {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (response, upload) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, request_body, mut respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let _server_driver = drive_server(server);
        let mut response_sender = respond
            .send_response_streaming(build_test_response(), false)
            .unwrap();
        let send = response_sender
            .send_data(Bytes::from(vec![0x46; 1024 * 1024]), true);
        tokio::pin!(send);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut send)
                .await
                .is_err()
        );
        drop(response);
        assert!(
            send.await.is_err(),
            "dropping response future did not cancel peer sender"
        );
        // Explicit drops after the assertion keep sibling handles alive throughout cancellation.
        drop(upload);
        drop(request_body);
        drop(respond);
    })
    .await
    .expect("dropping response future did not wake response sender");
}

#[tokio::test]
async fn terminal_body_error_reclaims_queued_connection_credit_with_siblings_alive()
 {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (first_response, mut first_upload) = client
            .send_request_streaming(build_test_request(), false)
            .unwrap();
        let (_, first_request_body, mut first_respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let mut first_sender = first_respond
            .send_response_streaming(build_test_response(), false)
            .unwrap();

        // Open the second stream before moving the server into its connection driver.
        let (second_response, second_upload) = client
            .send_request_streaming(build_test_request(), true)
            .unwrap();
        let (_, mut second_request_body, mut second_respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let _server_driver = drive_server(server);
        let (_, mut first_body) = first_response.await.unwrap();
        assert!(
            second_request_body
                .frame()
                .await
                .is_none()
        );

        // With no receive-frame polls, DATA exhausts the connection window and
        // remains queued in the first body. Keep this send future alive until reset.
        let first_send =
            first_sender.send_data(Bytes::from(vec![0x61; 1024 * 1024]), true);
        tokio::pin!(first_send);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut first_send)
                .await
                .is_err()
        );
        first_respond.send_reset(Reason::CANCEL);
        assert!(first_send.await.is_err());
        // Observe the reset on the client without consuming any queued response
        // DATA. Local send failure alone does not prove the reset reached the peer.
        assert!(
            first_upload
                .send_data(Bytes::from(vec![0x63; 1024 * 1024]), false)
                .await
                .is_err()
        );
        assert!(
            first_body
                .frame()
                .await
                .expect("reset was silently discarded")
                .is_err()
        );
        assert!(first_body.frame().await.is_none());

        let mut second_sender = second_respond
            .send_response_streaming(build_test_response(), false)
            .unwrap();
        let (_, mut second_body) = second_response.await.unwrap();
        let payload = Bytes::from(vec![0x62; LARGE_BODY_LEN]);
        let (sent, received) = tokio::join!(
            second_sender.send_data(payload.clone(), true),
            collect_data(&mut second_body),
        );
        sent.unwrap();
        assert_eq!(received.as_slice(), payload.as_ref());

        // Retain the errored body and both directions' sibling handles until
        // the second transfer finishes: cleanup on final drop must not be needed.
        drop(first_body);
        drop(first_upload);
        drop(first_request_body);
        drop(first_respond);
        drop(second_upload);
    })
    .await
    .expect("terminal body error did not reclaim connection credit");
}

#[tokio::test]
async fn reset_is_delivered_as_body_error_then_end() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (mut client, mut server, _client_driver) = connection_pair().await;
        let (response, _request_sender) = client
            .send_request_streaming(build_test_request(), true)
            .unwrap();
        let (_, mut request_body, mut respond) = server
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        let _server_driver = drive_server(server);
        assert!(request_body.frame().await.is_none());
        let mut response_sender = respond
            .send_response_streaming(build_test_response(), false)
            .unwrap();
        let (_, mut response_body) = response.await.unwrap();
        response_sender.send_reset(Reason::CANCEL);
        assert!(
            response_body
                .frame()
                .await
                .expect("reset was silently discarded")
                .is_err()
        );
        assert!(response_body.frame().await.is_none());
        assert!(response_body.frame().await.is_none());
    })
    .await
    .expect("stream reset was not delivered");
}
