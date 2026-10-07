use support::prelude::*;
use two_plz::{ext::Protocol, frame::headers::Pseudo};

fn extended() -> Pseudo {
    Pseudo::request(
        Method::CONNECT,
        Uri::try_from("https://example.com/chat?room=1").unwrap(),
        Some(Protocol::from_static("websocket")),
    )
}

async fn rejected_request(pseudo: Pseudo) {
    let (io, mut client) = mock::new();
    let peer = async move {
        client.assert_server_handshake().await;
        client
            .send_frame(frames::headers(1).pseudo(pseudo).eos())
            .await;
        client
            .recv_frame(frames::reset(1).reason(Reason::PROTOCOL_ERROR))
            .await;
    };
    let server = async move {
        let mut server = ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(io)
            .await
            .unwrap();
        assert!(server.accept().await.is_none());
    };
    tokio::time::timeout(Duration::from_secs(5), join(peer, server))
        .await
        .unwrap();
}

// Literal fields without indexing keep the raw pseudo-header order explicit.
fn literal_field(block: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    assert!(name.len() < 127 && value.len() < 127);
    block.push(0);
    block.push(name.len() as u8);
    block.extend_from_slice(name);
    block.push(value.len() as u8);
    block.extend_from_slice(value);
}

fn raw_headers(stream_id: u8, block: &[u8]) -> Vec<u8> {
    let len = block.len();
    let mut frame = vec![
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        1,
        5, // END_STREAM | END_HEADERS
        0,
        0,
        0,
        stream_id,
    ];
    frame.extend_from_slice(block);
    frame
}

async fn rejected_raw_request_recovers(mut block: Vec<u8>) {
    // Insert a dynamic-table entry after the malformed field. The next stream
    // references it, proving decoding continued despite the stream error.
    block.push(0x40);
    block.push(10);
    block.extend_from_slice(b"x-recovery");
    block.push(2);
    block.extend_from_slice(b"ok");
    let (io, mut client) = mock::new();
    let peer = async move {
        client.assert_server_handshake().await;
        client.send_bytes(&raw_headers(1, &block)).await;
        client
            .recv_frame(frames::reset(1).reason(Reason::PROTOCOL_ERROR))
            .await;
        let mut valid = raw_extended_block();
        valid.push(0xbe); // First dynamic-table entry (index 62).
        client.send_bytes(&raw_headers(3, &valid)).await;
        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
    };
    let server = async move {
        let mut server = ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(io)
            .await
            .unwrap();
        let (request, mut respond) = server.accept().await.unwrap().unwrap();
        assert_eq!(request.method(), &Method::CONNECT);
        assert_eq!(
            request.headers().value_of_key("x-recovery"),
            Some(&b"ok"[..])
        );
        respond.send_response(build_test_response()).unwrap();
        assert!(server.accept().await.is_none());
    };
    tokio::time::timeout(Duration::from_secs(5), join(peer, server))
        .await
        .expect("malformed extended CONNECT prevented stream recovery");
}

fn raw_extended_block() -> Vec<u8> {
    let mut block = Vec::new();
    for (name, value) in [
        (b":method".as_slice(), b"CONNECT".as_slice()),
        (b":scheme", b"https"),
        (b":authority", b"example.com"),
        (b":path", b"/chat"),
        (b":protocol", b"websocket"),
    ] {
        literal_field(&mut block, name, value);
    }
    block
}

#[tokio::test]
async fn duplicate_protocol_is_rejected_and_next_stream_recovers() {
    let mut block = raw_extended_block();
    literal_field(&mut block, b":protocol", b"websocket");
    rejected_raw_request_recovers(block).await;
}

#[tokio::test]
async fn protocol_after_regular_field_is_rejected_and_next_stream_recovers() {
    let mut block = Vec::new();
    for (name, value) in [
        (b":method".as_slice(), b"CONNECT".as_slice()),
        (b":scheme", b"https"),
        (b":authority", b"example.com"),
        (b":path", b"/chat"),
        (b"x-before-protocol", b"ok"),
        (b":protocol", b"websocket"),
    ] {
        literal_field(&mut block, name, value);
    }
    rejected_raw_request_recovers(block).await;
}

#[tokio::test]
async fn connection_and_upgrade_are_forbidden_on_extended_connect() {
    for (name, value) in [
        (b"connection".as_slice(), b"upgrade".as_slice()),
        (b"upgrade", b"websocket"),
    ] {
        let mut block = raw_extended_block();
        literal_field(&mut block, name, value);
        rejected_raw_request_recovers(block).await;
    }
}

#[tokio::test]
async fn invalid_protocol_tokens_are_rejected() {
    for token in [
        "",
        "two tokens",
        "websocket/13",
        "websocket\t",
        "websocket\u{7f}",
        "café",
    ] {
        let mut pseudo = extended();
        pseudo.protocol = Some(Protocol::from(token));
        rejected_request(pseudo).await;
    }
}

#[tokio::test]
async fn extended_connect_requires_each_uri_pseudo_header() {
    for field in ["scheme", "path", "authority"] {
        for empty in [false, true] {
            let mut pseudo = extended();
            let value = empty.then(|| "".into());
            match field {
                "scheme" => pseudo.scheme = value,
                "path" => pseudo.path = value,
                "authority" => pseudo.authority = value,
                _ => unreachable!(),
            }
            rejected_request(pseudo).await;
        }
    }
    let mut pseudo = extended();
    pseudo.method = Some(Method::GET);
    rejected_request(pseudo).await;
}

#[tokio::test]
async fn protocol_and_websocket_headers_are_preserved() {
    for token in ["websocket", "custom-protocol", "unregistered!token"] {
        let (io, mut client) = mock::new();
        let mut pseudo = extended();
        pseudo.protocol = Some(Protocol::from(token));
        let peer = async move {
            client.assert_server_handshake().await;
            client
                .send_frame(
                    frames::headers(1)
                        .pseudo(pseudo)
                        .field("sec-websocket-version", "13")
                        .field("sec-websocket-protocol", "chat, superchat")
                        .field(
                            "sec-websocket-extensions",
                            "permessage-deflate",
                        )
                        .eos(),
                )
                .await;
            client
                .recv_frame(frames::headers(1).response(200).eos())
                .await;
        };
        let server = async move {
            let mut server = ServerBuilder::new()
                .enable_connect_protocol()
                .handshake(io)
                .await
                .unwrap();
            let (request, mut respond) =
                server.accept().await.unwrap().unwrap();
            assert_eq!(request.method(), &Method::CONNECT);
            assert_eq!(request.scheme(), Some(&Scheme::HTTPS));
            assert_eq!(request.authority(), Some("example.com"));
            assert_eq!(request.path(), "/chat");
            assert_eq!(request.query(), Some("room=1"));
            assert_eq!(
                request
                    .headers()
                    .value_of_key("sec-websocket-version"),
                Some(&b"13"[..])
            );
            assert_eq!(
                request
                    .headers()
                    .value_of_key("sec-websocket-protocol"),
                Some(&b"chat, superchat"[..])
            );
            assert_eq!(
                request
                    .headers()
                    .value_of_key("sec-websocket-extensions"),
                Some(&b"permessage-deflate"[..])
            );
            let (head, _) = request.into_message_head();
            assert_eq!(
                head.into_parts().2.unwrap().as_ref(),
                token.as_bytes()
            );
            respond
                .send_response(build_test_response())
                .unwrap();
            assert!(server.accept().await.is_none());
        };
        tokio::time::timeout(Duration::from_secs(5), join(peer, server))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn authorityless_get_remains_valid() {
    let (io, mut client) = mock::new();
    let mut pseudo = extended();
    pseudo.method = Some(Method::GET);
    pseudo.protocol = None;
    pseudo.authority = None;
    let peer = async move {
        client.assert_server_handshake().await;
        client
            .send_frame(frames::headers(1).pseudo(pseudo).eos())
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };
    let server = async move {
        let mut server = ServerBuilder::new()
            .handshake(io)
            .await
            .unwrap();
        let (request, mut respond) = server.accept().await.unwrap().unwrap();
        assert_eq!(request.path(), "/chat");
        assert_eq!(request.authority(), None);
        respond
            .send_response(build_test_response())
            .unwrap();
        assert!(server.accept().await.is_none());
    };
    tokio::time::timeout(Duration::from_secs(5), join(peer, server))
        .await
        .unwrap();
}

#[tokio::test]
async fn request_pseudo_headers_are_rejected_on_responses() {
    support::trace_init!();
    for field in ["method", "scheme", "path", "authority", "protocol"] {
        for status in [103, 200] {
            let (io, mut peer) = mock::new();
            let mut pseudo =
                Pseudo::response(StatusCode::from_u16(status).unwrap());
            let request = extended();
            match field {
                "method" => pseudo.method = request.method,
                "scheme" => pseudo.scheme = request.scheme,
                "path" => pseudo.path = request.path,
                "authority" => pseudo.authority = request.authority,
                "protocol" => pseudo.protocol = request.protocol,
                _ => unreachable!(),
            }
            let server = async move {
                peer.assert_client_handshake().await;
                peer.recv_frame(
                    frames::headers(1)
                        .request("GET", "https", "http2.akamai.com", "/")
                        .eos(),
                )
                .await;
                peer.send_frame(frames::headers(1).pseudo(pseudo))
                    .await;
                peer.recv_frame(
                    frames::reset(1).reason(Reason::PROTOCOL_ERROR),
                )
                .await;
            };
            let client = async move {
                let (connection, mut sender) = ClientBuilder::new()
                    .handshake(io)
                    .await
                    .unwrap();
                let driver = tokio::spawn(connection);
                let response = sender
                    .send_request(build_test_request())
                    .unwrap();
                assert!(response.await.is_err());
                driver.await.unwrap().unwrap();
            };
            tokio::time::timeout(Duration::from_secs(5), join(server, client))
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn invalid_outbound_extensions_error_without_downgrading() {
    let (io, _peer) = tokio::io::duplex(4096);
    let (_connection, mut sender) = ClientBuilder::new()
        .handshake(io)
        .await
        .unwrap();
    for extension in [
        b"".as_slice(),
        b"two tokens",
        b"websocket/13",
        b"bad\t",
        b"bad\x7f",
        b"bad\xff",
    ] {
        let request = Request::builder()
            .method(Method::CONNECT)
            .uri(Uri::try_from("https://example.com/chat").unwrap())
            .extension(Bytes::copy_from_slice(extension))
            .build();
        assert!(sender.send_request(request).is_err());
    }
    let request = Request::builder()
        .method(Method::GET)
        .uri(Uri::try_from("https://example.com/chat").unwrap())
        .extension("websocket".into())
        .build();
    assert!(sender.send_request(request).is_err());
}
