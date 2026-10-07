use support::prelude::*;
use two_plz::message::BodyFrame;

async fn check_connect_response(streaming: bool, status: u16) {
    check_response(streaming, status, false, "0").await;
}

async fn check_response(
    streaming: bool,
    status: u16,
    extended: bool,
    length: &'static str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (io, mut peer) = mock::new();
        let server = async move {
            peer.assert_client_handshake_with_settings(
                frames::settings().enable_connect_protocol(1),
            )
            .await;
            let mut pseudo = Pseudo {
                method: Some(Method::CONNECT),
                authority: util::byte_str("tunnel.example.com:8443").into(),
                ..Default::default()
            };
            if extended {
                pseudo.scheme = util::byte_str("https").into();
                pseudo.path = util::byte_str("/").into();
                pseudo.protocol = Some(Protocol::from_static("websocket"));
            }
            peer.recv_frame(frames::headers(1).pseudo(pseudo).eos())
                .await;
            peer.send_frame(
                frames::headers(1)
                    .response(status)
                    .field("content-length", length),
            )
            .await;
            peer.send_frame(frames::data(1, "tunnel data").eos())
                .await;
            if !(200..300).contains(&status) {
                peer.recv_frame(frames::reset(1).protocol_error())
                    .await;
            }
        };
        let client = async move {
            let (mut connection, mut sender) = ClientBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let uri = Uri::builder()
                .scheme(Scheme::HTTPS)
                .authority("tunnel.example.com:8443")
                .path("/")
                .build()
                .unwrap();
            // Process peer SETTINGS before creating an extended CONNECT stream.
            poll_fn(|cx| {
                let result = std::pin::Pin::new(&mut connection).poll(cx);
                assert!(
                    result.is_pending(),
                    "connection closed during negotiation"
                );
                if connection.is_extended_connect_protocol_enabled() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            let mut builder = Request::builder()
                .method(Method::CONNECT)
                .uri(uri);
            if extended {
                builder = builder.extension("websocket".into());
            }
            let request = builder.build();
            if streaming {
                let (response, _send) = sender
                    .send_request_streaming(request, true)
                    .unwrap();
                let received = async {
                    match response.await {
                        Ok((head, mut body)) => {
                            assert_eq!(head.status().as_u16(), status);
                            let mut data = Vec::new();
                            while let Some(frame) = body.frame().await {
                                match frame {
                                    Ok(BodyFrame::Data(bytes)) => {
                                        data.extend_from_slice(&bytes)
                                    }
                                    Ok(BodyFrame::Trailers(_)) => {
                                        panic!("unexpected trailers")
                                    }
                                    Err(error) => return Err(error),
                                }
                            }
                            Ok(data)
                        }
                        Err(error) => Err(error),
                    }
                };
                let result = connection.drive(received).await;
                if (200..300).contains(&status) {
                    assert_eq!(result.unwrap(), b"tunnel data");
                } else {
                    assert!(
                        result.is_err(),
                        "non-2xx CONNECT must validate content-length"
                    );
                }
            } else {
                let response = sender.send_request(request).unwrap();
                let result = connection.drive(response).await;
                if (200..300).contains(&status) {
                    assert_eq!(
                        result.unwrap().body_as_ref().unwrap(),
                        "tunnel data"
                    );
                } else {
                    assert!(
                        result.is_err(),
                        "non-2xx CONNECT must validate content-length"
                    );
                }
            }
        };
        join(server, client).await;
    })
    .await
    .expect("CONNECT content-length test stalled");
}

#[tokio::test]
async fn streaming_successful_connect_ignores_content_length() {
    check_connect_response(true, 200).await;
}

#[tokio::test]
async fn buffered_successful_connect_ignores_content_length() {
    check_connect_response(false, 200).await;
}

#[tokio::test]
async fn streaming_rejected_connect_validates_content_length() {
    check_connect_response(true, 403).await;
}

#[tokio::test]
async fn buffered_rejected_connect_validates_content_length() {
    check_connect_response(false, 403).await;
}

#[tokio::test]
async fn extended_connect_response_status_and_content_length_matrix() {
    for streaming in [false, true] {
        for status in [200, 201, 299, 300, 403] {
            check_response(streaming, status, true, "0").await;
        }
        // Successful CONNECT bypasses even syntactically invalid body lengths.
        check_response(streaming, 200, true, "not-a-number").await;
    }
}
