use support::prelude::*;

#[tokio::test]
async fn read_invalid_priority_error_scope() {
    use two_plz::proto::ProtoError;
    for (id, payload, expected, connection_error) in [
        (0u32, vec![0, 0, 0, 1, 0], Reason::PROTOCOL_ERROR, true),
        (1, vec![0, 0, 0, 1, 0], Reason::PROTOCOL_ERROR, false),
        (1, vec![0, 0, 0, 0], Reason::FRAME_SIZE_ERROR, false),
        (1, vec![0, 0, 0, 0, 0, 0], Reason::FRAME_SIZE_ERROR, false),
    ] {
        let mut wire = vec![0, 0, payload.len() as u8, 2, 0];
        wire.extend_from_slice(&id.to_be_bytes());
        wire.extend_from_slice(&payload);
        let mut codec = Codec::from(
            mock_io::Builder::new()
                .read(&wire)
                .build(),
        );
        let error = codec.next().await.unwrap().unwrap_err();
        match error {
            ProtoError::Reset(stream_id, reason, _) if !connection_error => {
                assert_eq!(stream_id, id);
                assert_eq!(reason, expected);
            }
            ProtoError::GoAway(_, reason, _) if connection_error => {
                assert_eq!(reason, expected)
            }
            other => panic!("unexpected error scope: {other:?}"),
        }
    }
}

#[tokio::test]
async fn read_none() {
    let mut codec = Codec::from(mock_io::Builder::new().build());

    assert_closed!(codec);
}

#[test]
#[ignore]
fn read_frame_too_big() {}

// ===== DATA =====

#[tokio::test]
async fn read_data_no_padding() {
    let mut codec = raw_codec! {
        read => [
            0, 0, 5, 0, 0, 0, 0, 0, 1,
            "hello",
        ];
    };

    let data = poll_frame!(Data, codec);
    assert_eq!(data.stream_id(), 1);
    assert_eq!(data.payload(), &b"hello"[..]);
    assert!(!data.is_end_stream());

    assert_closed!(codec);
}

#[tokio::test]
async fn read_data_empty_payload() {
    let mut codec = raw_codec! {
        read => [
            0, 0, 0, 0, 0, 0, 0, 0, 1,
        ];
    };

    let data = poll_frame!(Data, codec);
    assert_eq!(data.stream_id(), 1);
    assert_eq!(data.payload(), &b""[..]);
    assert!(!data.is_end_stream());

    assert_closed!(codec);
}

#[tokio::test]
async fn read_data_end_stream() {
    let mut codec = raw_codec! {
        read => [
            0, 0, 5, 0, 1, 0, 0, 0, 1,
            "hello",
        ];
    };

    let data = poll_frame!(Data, codec);
    assert_eq!(data.stream_id(), 1);
    assert_eq!(data.payload(), &b"hello"[..]);
    assert!(data.is_end_stream());
    assert_closed!(codec);
}

#[tokio::test]
async fn read_data_padding() {
    let mut codec = raw_codec! {
        read => [
            0, 0, 16, 0, 0x8, 0, 0, 0, 1,
            5,       // Pad length
            "helloworld", // Data
            "\0\0\0\0\0", // Padding
        ];
    };

    let data = poll_frame!(Data, codec);
    assert_eq!(data.stream_id(), 1);
    assert_eq!(data.payload(), &b"helloworld"[..]);
    assert!(!data.is_end_stream());

    assert_closed!(codec);
}

#[tokio::test]
async fn read_push_promise() {
    let mut codec = raw_codec! {
        read => [
            0, 0, 0x5,
            0x5, 0x4,
            0, 0, 0, 0x1, // stream id
            0, 0, 0, 0x2, // promised id
            0x82, // HPACK :method="GET"
        ];
    };

    let pp = poll_frame!(PushPromise, codec);
    assert_eq!(pp.stream_id(), 1);
    assert_eq!(pp.promised_id(), 2);
    assert_eq!(pp.into_parts().0.method, Some(Method::GET));

    assert_closed!(codec);
}

#[tokio::test]
async fn read_data_stream_id_zero() {
    let mut codec = raw_codec! {
        read => [
            0, 0, 5, 0, 0, 0, 0, 0, 0,
            "hello", // Data
        ];
    };

    poll_err!(codec);
}

// ===== HEADERS =====

#[test]
#[ignore]
fn read_headers_without_pseudo() {}

#[test]
#[ignore]
fn read_headers_with_pseudo() {}

#[test]
#[ignore]
fn read_headers_empty_payload() {}

#[tokio::test]
async fn read_continuation_frames() {
    support::trace_init!();
    let (io, mut srv) = mock::new();

    let large = build_large_headers();
    let frame = large
        .iter()
        .fold(frames::headers(1).response(200), |frame, &(name, ref value)| {
            frame.field(name, &value[..])
        })
        .eos();

    let srv = async move {
        let settings = srv.assert_client_handshake().await;
        assert_default_settings!(settings);
        srv.recv_frame(
            frames::headers(1)
                .request("GET", "https", "http2.akamai.com", "/")
                .eos(),
        )
        .await;
        srv.send_frame(frame).await;
    };

    let client = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .handshake(io)
            .await
            .expect("handshake");

        let request = build_test_request();
        let req = async {
            let res = client
                .send_request(request)
                .expect("send_request")
                .await
                .expect("response");
            assert_eq!(*res.status(), StatusCode::OK);
            let expected = large.iter().fold(
                HeaderMap::new(),
                |mut map, &(name, ref value)| {
                    map.insert(name, value);
                    map
                },
            );
            assert_eq!(res.headers(), &expected);
        };

        conn.drive(req).await;
        conn.await.expect("client");
    };

    join(srv, client).await;
}

#[tokio::test]
async fn update_max_frame_len_at_rest() {
    use futures::StreamExt;
    use tokio::io::AsyncReadExt;

    support::trace_init!();
    // TODO(hyper): add test for updating max frame length in flight as well?
    let mut codec = raw_codec! {
        read => [
            0, 0, 5, 0, 0, 0, 0, 0, 1,
            "hello",
            0, 64, 1, 0, 0, 0, 0, 0, 1,
            vec![0; 16_385],
        ];
    };

    assert_eq!(poll_frame!(Data, codec).payload(), &b"hello"[..]);

    codec.set_max_recv_frame_size(16_384);

    assert_eq!(codec.max_recv_frame_size(), 16_384);
    assert_eq!(
        codec
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string(),
        "frame with invalid size"
    );

    // drain codec buffer
    let mut buf = Vec::new();
    codec
        .get_mut()
        .read_to_end(&mut buf)
        .await
        .unwrap();
}

#[tokio::test]
async fn read_goaway_with_debug_data() {
    let mut codec = raw_codec! {
        read => [
            // head
            0, 0, 22, 7, 0, 0, 0, 0, 0,
            // last_stream_id
            0, 0, 0, 1,
            // error_code
            0, 0, 0, 11,
            // debug_data
            "too_many_pings",
        ];
    };

    let data = poll_frame!(GoAway, codec);
    assert_eq!(data.reason(), frame::Reason::ENHANCE_YOUR_CALM);
    assert_eq!(data.last_stream_id(), 1);
    assert_eq!(&**data.debug_data(), b"too_many_pings");

    assert_closed!(codec);
}

#[tokio::test]
async fn read_malformed_headers_continuation_preserves_hpack_table() {
    use two_plz::proto::ProtoError;

    // Split the indexed literal at every boundary to cover NeedMore as well
    // as a complete malformed fragment without END_HEADERS.
    let indexed = [0x40, 1, b'x', 1, b'y'];
    for split in 0..=indexed.len() {
        let mut first = vec![
            0x82, 0x87, 0x84, // :method GET, :scheme https, :path /
            0, 1, b'X', 1, b'v', // Uppercase name, without indexing
        ];
        first.extend_from_slice(&indexed[..split]);
        let mut wire = vec![0, 0, first.len() as u8, 1, 1, 0, 0, 0, 1];
        wire.extend_from_slice(&first);
        // A non-final continuation must also retain the malformed state.
        wire.extend_from_slice(&[0, 0, 0, 9, 0, 0, 0, 0, 1]);
        wire.extend_from_slice(&[
            0,
            0,
            (indexed.len() - split) as u8,
            9,
            4,
            0,
            0,
            0,
            1,
        ]);
        wire.extend_from_slice(&indexed[split..]);
        wire.extend_from_slice(&[
            0, 0, 4, 1, 5, 0, 0, 0, 3, 0x82, 0x87, 0x84,
            0xbe, // Index 62: x=y from the rejected block
        ]);

        let mut codec = Codec::from(
            mock_io::Builder::new()
                .read(&wire)
                .build(),
        );
        match codec.next().await.unwrap().unwrap_err() {
            ProtoError::Reset(id, reason, _) => {
                assert_eq!(id, 1);
                assert_eq!(reason, Reason::PROTOCOL_ERROR);
            }
            other => panic!("unexpected error at split {split}: {other:?}"),
        }
        let headers = poll_frame!(Headers, codec);
        assert_eq!(headers.stream_id(), 3);
        let (pseudo, fields) = headers.into_parts();
        assert_eq!(pseudo.method, Some(Method::GET));
        let mut expected = HeaderMap::new();
        expected.insert("x", "y");
        assert_eq!(fields, expected);
        assert_closed!(codec);
    }
}
