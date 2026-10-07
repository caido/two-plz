use support::prelude::*;
use tokio::io::AsyncWriteExt;

const SETTINGS_ACK: &[u8] = &[0, 0, 0, 4, 1, 0, 0, 0, 0];

// skipped
// push_request
// push_request_disabled
// push_request_against_concurrency
// push_request_with_data
// push_request_between_data
// sends_reset_no_error_when_req_body_is_dropped
// poll_reset
// poll_reset_io_error
// poll_reset_after_send_response_is_user_error
// serve_when_request_in_response_extensions : TODO

#[tokio::test]
async fn read_preface_in_multiple_frames() {
    support::trace_init!();

    let mock = mock_io::Builder::new()
        .read(b"PRI * HTTP/2.0")
        .read(b"\r\n\r\nSM\r\n\r\n")
        .write(frames::SETTINGS)
        .read(NEW_SETTINGS)
        .write(SETTINGS_ACK)
        .read(SETTINGS_ACK)
        .build();

    let mut s = ServerBuilder::new()
        .handshake(mock)
        .await
        .unwrap();

    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn server_builder_set_max_concurrent_streams() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let mut settings = frame::Settings::default();
    settings.set_max_concurrent_streams(Some(1));

    let client = async move {
        let recv_settings = client.assert_server_handshake().await;
        assert_frame_eq(recv_settings, settings);
        client
            .send_frame(frames::headers(1).request(
                "GET",
                "https",
                "example.com",
                "/",
            ))
            .await;
        client
            .send_frame(frames::headers(3).request(
                "GET",
                "https",
                "example.com",
                "/",
            ))
            .await;
        client
            .send_frame(frames::data(1, &b"hello"[..]).eos())
            .await;
        client
            .recv_frame(frames::reset(3).refused())
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_concurrent_streams(1)
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();

        assert_eq!(req.method(), &Method::GET);
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn serve_request() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn serve_connect() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("CONNECT", "http", "localhost", "")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::CONNECT);
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn recv_invalid_authority() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let bad_auth = util::byte_str("not:a/good authority");
    let mut bad_headers: frame::Headers = frames::headers(1)
        .request("CONNECT", "http", "localhost", "")
        .eos()
        .into();
    bad_headers.pseudo_mut().authority = Some(bad_auth);

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client.send_frame(bad_headers).await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::CONNECT);
        assert_eq!(
            req.authority()
                .as_ref()
                .unwrap()
                .as_bytes(),
            b"not:a/good authority"
        );
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn recv_uppercase_header_resets_stream_and_serves_next_request() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());

        // Literal HPACK names bypass the header builder's normalization.
        // Indexed :method GET, :scheme https, and :path / precede
        // literal :authority example.com and the invalid field X-Test: x.
        client
            .send_bytes(&[
                0, 0, 26, 1, 5, 0, 0, 0,
                1, // HEADERS, END_HEADERS | END_STREAM
                0x82, 0x87, 0x84, 0x01, 11, b'e', b'x', b'a', b'm', b'p',
                b'l', b'e', b'.', b'c', b'o', b'm', 0x00, 6, b'X', b'-', b'T',
                b'e', b's', b't', 1, b'x',
            ])
            .await;
        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);
        assert_eq!(resp.stream_id(), 3u32);
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn recv_missing_path_resets_stream_and_serves_next_request() {
    recv_invalid_path_and_serve_next_request(None).await;
}

#[tokio::test]
async fn recv_empty_path_resets_stream_and_serves_next_request() {
    recv_invalid_path_and_serve_next_request(Some("")).await;
}

#[tokio::test]
async fn recv_invalid_uri_path_resets_stream_and_serves_next_request() {
    support::trace_init!();
    let (mock, mut client) = mock::new();
    let mut invalid: frame::Headers = frames::headers(1)
        .request("GET", "https", "example.com", "/")
        .eos()
        .into();
    invalid.pseudo_mut().path = Some(util::byte_str("/bad path"));

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client.send_frame(invalid).await;
        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);
        assert_eq!(resp.stream_id(), 3u32);
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

async fn recv_invalid_path_and_serve_next_request(path: Option<&str>) {
    support::trace_init!();
    let (mock, mut client) = mock::new();
    let mut invalid: frame::Headers = frames::headers(1)
        .request("GET", "https", "example.com", "/")
        .eos()
        .into();
    invalid.pseudo_mut().path = path.map(util::byte_str);

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client.send_frame(invalid).await;
        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
        client
            .send_frame(
                frames::headers(3)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);
        assert_eq!(resp.stream_id(), 3u32);
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn recv_priority_on_idle_half_closed_and_closed_streams() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());

        // PRIORITY on idle streams, including an even-numbered stream, must
        // neither open them nor advance the last received request stream ID.
        client
            .send_bytes(&[
                0, 0, 5, 2, 0, 0, 0, 0, 9, // PRIORITY on stream 9
                0, 0, 0, 0, 15, // dependency 0, weight 16
                0, 0, 5, 2, 0, 0, 0, 0, 2, // PRIORITY on stream 2
                0, 0, 0, 0, 15,
            ])
            .await;
        client
            .send_frame(
                frames::headers(1)
                    .request("POST", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        // The request has ended but the response is still open.
        client
            .send_bytes(&[0, 0, 5, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 15])
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
        // Both directions of stream 1 are now closed.
        client
            .send_bytes(&[0, 0, 5, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 15])
            .await;

        for id in [3, 9] {
            client
                .send_frame(
                    frames::headers(id)
                        .request("GET", "https", "example.com", "/")
                        .eos(),
                )
                .await;
            client
                .recv_frame(frames::headers(id).response(200).eos())
                .await;
        }
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut resp) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::POST);
        assert_eq!(resp.stream_id(), 1u32);
        resp.send_response(build_test_response())
            .unwrap();
        for id in [3, 9] {
            let (req, mut resp) = s.accept().await.unwrap().unwrap();
            assert_eq!(req.method(), &Method::GET);
            assert_eq!(resp.stream_id(), id as u32);
            resp.send_response(build_test_response())
                .unwrap();
        }
        drop(req);
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn recv_connection_header() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let req = |id, name, val| {
        frames::headers(id)
            .request("GET", "https", "example.com", "/")
            .field(name, val)
            .eos()
    };

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(req(1, "connection", "foo"))
            .await;
        client
            .send_frame(req(3, "keep-alive", "5"))
            .await;
        client
            .send_frame(req(5, "proxy-connection", "bar"))
            .await;
        client
            .send_frame(req(7, "transfer-encoding", "chunked"))
            .await;
        client
            .send_frame(req(9, "upgrade", "HTTP/2"))
            .await;
        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
        client
            .recv_frame(frames::reset(3).protocol_error())
            .await;
        client
            .recv_frame(frames::reset(5).protocol_error())
            .await;
        client
            .recv_frame(frames::reset(7).protocol_error())
            .await;
        client
            .recv_frame(frames::reset(9).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn abrupt_shutdown() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("POST", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::go_away(1).internal_error())
            .await;
        client.recv_eof().await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        let (req, _resp) = s.accept().await.unwrap().unwrap();
        s.abrupt_shutdown(Reason::INTERNAL_ERROR);

        let srv_fut = async move {
            poll_fn(move |cx| s.poll_closed(cx))
                .await
                .expect("server");
        };

        drop(_resp);
        drop(req);

        srv_fut.await;
    };

    join(client, srv).await;
}

#[tokio::test]
async fn graceful_shutdown() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        // 2^31 - 1 = 2147483647
        // Note: not using a constant in the library because library devs
        // can be unsmart.
        client
            .recv_frame(frames::go_away(2147483647))
            .await;
        client
            .recv_frame(frames::ping(frame::Ping::SHUTDOWN))
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
        // Pretend this stream was sent while the GOAWAY was in flight
        client
            .send_frame(frames::headers(3).request(
                "POST",
                "https",
                "example.com",
                "/",
            ))
            .await;
        client
            .send_frame(frames::ping(frame::Ping::SHUTDOWN).pong())
            .await;
        client
            .recv_frame(frames::go_away(3))
            .await;
        // streams sent after GOAWAY receive no response
        client
            .send_frame(frames::headers(7).request(
                "POST",
                "https",
                "example.com",
                "/",
            ))
            .await;
        client
            .send_frame(frames::data(7, "").eos())
            .await;
        client
            .send_frame(frames::data(3, "").eos())
            .await;
        client
            .recv_frame(frames::headers(3).response(200).eos())
            .await;
        client.recv_eof().await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        // req 1
        let (req, mut resp) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);

        s.graceful_shutdown();

        // send resp 1
        resp.send_response(build_test_response())
            .unwrap();

        // req 3
        let (req, mut resp) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::POST);
        resp.send_response(build_test_response())
            .unwrap();

        assert!(s.next().await.is_none(), "unexpected request");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn goaway_even_if_client_sent_goaway() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(5)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        // Ping-pong so as to wait until server gets req
        client.ping_pong([0; 8]).await;
        client
            .send_frame(frames::go_away(0))
            .await;
        // 2^31 - 1 = 2147483647
        // Note: not using a constant in the library because library devs
        // can be unsmart.
        client
            .recv_frame(frames::go_away(2147483647))
            .await;
        client
            .recv_frame(frames::ping(frame::Ping::SHUTDOWN))
            .await;
        client
            .recv_frame(frames::headers(5).response(200).eos())
            .await;
        client
            .send_frame(frames::ping(frame::Ping::SHUTDOWN).pong())
            .await;
        client
            .recv_frame(frames::go_away(5))
            .await;
        client.recv_eof().await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        // req 1
        let (req, mut resp) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);

        s.graceful_shutdown();

        // send resp 1
        resp.send_response(build_test_response())
            .unwrap();

        assert!(s.next().await.is_none(), "unexpected request");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn sends_reset_cancel_when_res_is_dropped() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::reset(1).cancel())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        let (req, resp) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);

        drop(resp);

        assert!(s.next().await.is_none(), "unexpected request");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn too_big_headers_sends_431() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings().max_header_list_size(10));
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .field("some-header", "some-value")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(1).response(431).eos())
            .await;
        idle_ms(10).await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_header_list_size(10)
            .handshake(mock)
            .await
            .unwrap();

        let req = s.next().await;
        assert!(req.is_none(), "req is {:?}", req);
    };

    join(client, srv).await;
}

#[tokio::test]
async fn too_big_headers_sends_reset_after_431_if_not_eos() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings().max_header_list_size(10));
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .field("some-header", "some-value"),
            )
            .await;
        client
            .recv_frame(frames::headers(1).response(431).eos())
            .await;
        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_header_list_size(10)
            .handshake(mock)
            .await
            .unwrap();

        let req = s.next().await;
        assert!(req.is_none(), "req is {:?}", req);
    };

    join(client, srv).await;
}

#[tokio::test]
async fn too_many_continuation_frames_sends_goaway() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(
            settings,
            frames::settings().max_header_list_size(1024 * 32),
        );
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .field("some-header", "some-value"),
            )
            .await;
        // the mock impl automatically splits into CONTINUATION frames if the
        // headers are too big for one frame. So without a max header list size
        // set, we'll send a bunch of headers that will eventually get nuked.
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .field("a".repeat(10_000), "b".repeat(10_000))
                    .field("c".repeat(10_000), "d".repeat(10_000))
                    .field("e".repeat(10_000), "f".repeat(10_000))
                    .field("g".repeat(10_000), "h".repeat(10_000))
                    .field("i".repeat(10_000), "j".repeat(10_000))
                    .field("k".repeat(10_000), "l".repeat(10_000))
                    .field("m".repeat(10_000), "n".repeat(10_000))
                    .field("o".repeat(10_000), "p".repeat(10_000))
                    .field("y".repeat(10_000), "z".repeat(10_000)),
            )
            .await;
        client
            .recv_frame(
                frames::go_away(1)
                    .calm()
                    .data("too_many_continuations"),
            )
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_header_list_size(1024 * 32)
            .handshake(mock)
            .await
            .unwrap();

        let err = s
            .next()
            .await
            .unwrap()
            .expect_err("server");
        assert!(err.is_go_away());
        assert!(err.is_library());
        assert_eq!(err.reason(), Some(Reason::ENHANCE_YOUR_CALM));
    };

    join(client, srv).await;
}

#[tokio::test]
async fn pending_accept_recv_illegal_content_length_data() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("POST", "https", "example.com", "/")
                    .field("content-length", "1"),
            )
            .await;
        client
            .send_frame(frames::data(1, &b"hello"[..]).eos())
            .await;
        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn server_error_on_unclean_shutdown() {
    support::trace_init!();
    let (mock, mut client) = mock::new();
    let s = ServerBuilder::new().handshake(mock);

    client
        .write_all(b"PRI *")
        .await
        .expect("write");
    drop(client);

    assert!(s.await.is_err());
}

#[tokio::test]
async fn server_error_on_status_in_request() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(frames::headers(1).status(StatusCode::OK))
            .await;
        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn request_without_authority() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "", "/just-a-path")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        let (req, mut resp) = s.next().await.unwrap().unwrap();
        assert_eq!(req.path_and_query().as_str(), "/just-a-path");
        resp.send_response(build_test_response())
            .unwrap();
        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn send_reset_explicitly() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::reset(1).reason(Reason::ENHANCE_YOUR_CALM))
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        let (_req, mut resp) = s.next().await.unwrap().unwrap();
        resp.send_reset(Reason::ENHANCE_YOUR_CALM);
        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn send_reset_explicitly_does_not_affect_local_limit() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        for s in (1..9).step_by(2) {
            client
                .send_frame(
                    frames::headers(s)
                        .request("GET", "https", "example.com", "/")
                        .eos(),
                )
                .await;
            client
                .recv_frame(frames::reset(s).reason(Reason::INTERNAL_ERROR))
                .await;
        }
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        for _ in (1..9).step_by(2) {
            let (_req, mut resp) = s.next().await.unwrap().unwrap();
            resp.send_reset(Reason::INTERNAL_ERROR);
        }
        assert!(s.next().await.is_none());
    };

    join(client, srv).await;
}

#[tokio::test]
async fn extended_connect_protocol_disabled_by_default() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_eq!(settings.is_extended_connect_protocol_enabled(), None);

        client
            .send_frame(frames::headers(1).pseudo(Pseudo::request(
                Method::CONNECT,
                build_test_uri(),
                Protocol::from_static("the-bread-protocol").into(),
            )))
            .await;

        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        poll_fn(move |cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn extended_connect_protocol_enabled_during_handshake() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_eq!(
            settings.is_extended_connect_protocol_enabled(),
            Some(true)
        );

        client
            .send_frame(frames::headers(1).pseudo(Pseudo::request(
                Method::CONNECT,
                build_test_uri(),
                Protocol::from_static("the-bread-protocol").into(),
            )))
            .await;

        client
            .recv_frame(frames::headers(1).response(200))
            .await;
        client
            .send_frame(frames::data(1, "hello").eos())
            .await;
        client
            .recv_frame(frames::data(1, "world"))
            .await;
        client
            .recv_frame(frames::data(1, "").eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(mock)
            .await
            .unwrap();

        let (req, body, mut resp) = s
            .accept_streaming()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(req.method(), &Method::CONNECT);
        let send = resp
            .send_response_streaming(build_test_response(), false)
            .unwrap();
        let mut tunnel = two_plz::Tunnel::new(body, send).unwrap();
        let exchange = async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut data = Vec::new();
            tunnel
                .read_to_end(&mut data)
                .await
                .unwrap();
            assert_eq!(data, b"hello");
            tunnel
                .write_all(b"world")
                .await
                .unwrap();
            tunnel.shutdown().await.unwrap();
        };
        let drive = poll_fn(|cx| s.poll_closed(cx));
        let (_, result) = join(exchange, drive).await;
        result.expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn reject_pseudo_protocol_on_non_connect_request() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_eq!(
            settings.is_extended_connect_protocol_enabled(),
            Some(true)
        );

        client
            .send_frame(frames::headers(1).pseudo(Pseudo::request(
                Method::GET,
                build_test_uri(),
                Some(Protocol::from_static("the-bread-protocol")),
            )))
            .await;

        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(mock)
            .await
            .unwrap();

        assert!(s.next().await.is_none());

        poll_fn(move |cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn reject_extended_connect_request_without_scheme() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_eq!(
            settings.is_extended_connect_protocol_enabled(),
            Some(true)
        );

        client
            .send_frame(frames::headers(1).pseudo(Pseudo {
                method: Method::CONNECT.into(),
                authority: util::byte_str("example.com").into(),
                path: util::byte_str("/").into(),
                protocol: Protocol::from("the-bread-protocol").into(),
                ..Default::default()
            }))
            .await;

        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(mock)
            .await
            .unwrap();

        assert!(s.next().await.is_none());

        poll_fn(move |cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn reject_extended_connect_request_without_path() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_eq!(
            settings.is_extended_connect_protocol_enabled(),
            Some(true)
        );

        client
            .send_frame(frames::headers(1).pseudo(Pseudo {
                method: Method::CONNECT.into(),
                authority: util::byte_str("example.com").into(),
                scheme: util::byte_str("https").into(),
                protocol: Protocol::from("the-bread-protocol").into(),
                ..Default::default()
            }))
            .await;

        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(mock)
            .await
            .unwrap();

        assert!(s.next().await.is_none());

        poll_fn(move |cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn reject_informational_status_header_in_request() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let _ = client.assert_server_handshake().await;
        let status_code = 128;
        assert!(
            StatusCode::from_u16(status_code)
                .unwrap()
                .is_informational()
        );

        client
            .send_frame(frames::headers(1).response(status_code))
            .await;

        client
            .recv_frame(frames::reset(1).protocol_error())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();

        poll_fn(move |cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn client_drop_connection_without_close_notify() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let _ = client.assert_server_handshake().await;
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;

        // Client closed without notify causing UnexpectedEof
        client.close_without_notify();
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_concurrent_streams(1)
            .handshake(mock)
            .await
            .unwrap();

        let (req, mut resp) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);
        resp.send_response(build_test_response())
            .unwrap();

        // Step the conn state forward and hitting the EOF
        // But we have no outstanding request from client to be satisfied,
        // so we should not return an error
        poll_fn(move |cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn init_window_size_smaller_than_default_should_use_default_before_ack()
{
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        // Client can send in some data before ACK;
        // Server needs to make sure the Recv stream has default window size
        // as per https://datatracker.ietf.org/doc/html/rfc9113#name-initial-flow-control-window
        client.write_preface().await;
        client
            .send(frame::Settings::default().into())
            .await
            .unwrap();
        client
            .next()
            .await
            .expect("unexpected EOF")
            .unwrap();
        client
            .send_frame(frames::headers(1).request(
                "GET",
                "https",
                "example.com",
                "/",
            ))
            .await;
        client
            .send_frame(frames::data(1, &b"hello"[..]).eos())
            .await;
        client
            .send(frame::Settings::ack().into())
            .await
            .unwrap();
        client.next().await;
        client
            .recv_frame(frames::headers(1).response(200).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .max_concurrent_streams(1)
            .initial_window_size(1)
            .handshake(mock)
            .await
            .unwrap();

        let (req, mut resp) = s.next().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);
        resp.send_response(build_test_response())
            .unwrap();

        // Step the conn state forward and hitting the EOF
        // But we have no outstanding request from client to be satisfied,
        // so we should not return an error
        poll_fn(move |cx| s.poll_closed(cx))
            .await
            .expect("server");
    };

    join(client, srv).await;
}

#[tokio::test]
async fn serve_response_empty_body() {
    support::trace_init!();
    let (mock, mut client) = mock::new();

    let client = async move {
        let settings = client.assert_server_handshake().await;
        assert_frame_eq(settings, frames::settings());
        client
            .send_frame(
                frames::headers(1)
                    .request("GET", "https", "example.com", "/")
                    .eos(),
            )
            .await;
        client
            .recv_frame(frames::headers(1).response(200))
            .await;
        client
            .recv_frame(frames::data(1, vec![]).eos())
            .await;
    };

    let srv = async move {
        let mut s = ServerBuilder::new()
            .handshake(mock)
            .await
            .unwrap();
        let (req, mut sender) = s.accept().await.unwrap().unwrap();
        assert_eq!(req.method(), &Method::GET);
        let mut resp = build_test_response();
        resp.set_body(BytesMut::new());
        sender.send_response(resp).unwrap();
        assert!(s.accept().await.is_none());
    };

    join(client, srv).await;
}
