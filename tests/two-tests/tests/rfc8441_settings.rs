use support::prelude::*;
use tokio::io::AsyncWriteExt;

fn extended_request() -> Request {
    Request::builder()
        .method(Method::CONNECT)
        .extension("websocket".into())
        .uri(build_test_uri())
        .build()
}

#[tokio::test]
async fn client_setting_does_not_enable_extended_connect_on_server() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (io, mut peer) = mock::new();
        let peer_task = async move {
            peer.assert_server_handshake_with_settings(
                frames::settings().enable_connect_protocol(1),
            )
            .await;
            peer.send_frame(frames::headers(1).pseudo(Pseudo::request(
                Method::CONNECT,
                build_test_uri(),
                Some(Protocol::from_static("websocket")),
            )))
            .await;
            peer.recv_frame(frames::reset(1).protocol_error())
                .await;
        };
        let server_task = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            assert!(
                server
                    .accept_streaming()
                    .await
                    .is_none()
            );
        };
        join(peer_task, server_task).await;
    })
    .await
    .expect("client advertisement test stalled");
}

fn settings(values: &[u32]) -> Vec<u8> {
    let len = values.len() * 6;
    let mut bytes =
        vec![(len >> 16) as u8, (len >> 8) as u8, len as u8, 4, 0, 0, 0, 0, 0];
    for value in values {
        bytes.extend_from_slice(&[0, 8]);
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    bytes
}

#[tokio::test]
async fn rejected_extended_connect_does_not_consume_stream_id() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for (streaming, process_settings) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let (io, mut peer) = mock::new();
            let peer = async move {
                peer.assert_client_handshake_with_settings(
                    frames::settings().enable_connect_protocol(0),
                )
                .await;
                peer.recv_frame(
                    frames::headers(1)
                        .request("GET", "https", "http2.akamai.com", "/")
                        .eos(),
                )
                .await;
                peer.send_frame(frames::headers(1).response(200).eos())
                    .await;
            };
            let client = async move {
                let (mut conn, mut sender) = ClientBuilder::new()
                    .handshake(io)
                    .await
                    .unwrap();
                if process_settings {
                    poll_once(&mut conn).await.unwrap();
                    assert!(!conn.is_extended_connect_protocol_enabled());
                }
                if streaming {
                    assert!(
                        sender
                            .send_request_streaming(extended_request(), true)
                            .is_err()
                    );
                } else {
                    assert!(
                        sender
                            .send_request(extended_request())
                            .is_err()
                    );
                }
                let response = sender
                    .send_request(build_test_request())
                    .unwrap();
                conn.drive(response).await.unwrap();
            };
            join(peer, client).await;
        }
    })
    .await
    .expect("negotiation rejection stalled");
}

#[tokio::test]
async fn valid_settings_transitions_enable_extended_connect() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (io, mut peer) = tokio::io::duplex(4096);
        let (mut conn, _sender) = ClientBuilder::new()
            .handshake(io)
            .await
            .unwrap();
        assert!(!conn.is_extended_connect_protocol_enabled());
        for (values, enabled) in [
            (&[0][..], false),
            (&[0, 1][..], true),
            (&[1, 1][..], true),
            (&[][..], true),
        ] {
            peer.write_all(&settings(values))
                .await
                .unwrap();
            poll_once(&mut conn).await.unwrap();
            assert_eq!(conn.is_extended_connect_protocol_enabled(), enabled);
        }
    })
    .await
    .expect("SETTINGS transition stalled");
}

#[tokio::test]
async fn disabling_extended_connect_is_connection_protocol_error() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for (prior_enabled, values, ack_between) in [
            (false, &[1, 0][..], false),
            (true, &[0][..], false),
            (true, &[0, 1][..], false),
            (true, &[0, 1][..], true),
        ] {
            let (io, mut peer) = tokio::io::duplex(4096);
            let (mut conn, _sender) = ClientBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            if prior_enabled {
                peer.write_all(&settings(&[1]))
                    .await
                    .unwrap();
                poll_once(&mut conn).await.unwrap();
                assert!(conn.is_extended_connect_protocol_enabled());
                if ack_between {
                    peer.write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0])
                        .await
                        .unwrap();
                    poll_once(&mut conn).await.unwrap();
                    assert!(conn.is_extended_connect_protocol_enabled());
                }
            }
            peer.write_all(&settings(values))
                .await
                .unwrap();
            let err = conn
                .await
                .expect_err("disabling extended CONNECT must fail");
            assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
        }
    })
    .await
    .expect("SETTINGS rejection stalled");
}
