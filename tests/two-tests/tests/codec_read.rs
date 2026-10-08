use support::prelude::*;

fn append_header_fragment(
    wire: &mut Vec<u8>,
    kind: u8,
    flags: u8,
    payload: &[u8],
) {
    wire.extend_from_slice(&[
        0,
        0,
        payload.len() as u8,
        kind,
        flags,
        0,
        0,
        0,
        1,
    ]);
    wire.extend_from_slice(payload);
}

#[tokio::test]
async fn hpack_failures_have_connection_compression_reason() {
    use two_plz::proto::ProtoError;
    for payload in [
        vec![0x80],                                     // Index zero
        vec![0xff],                                     // Incomplete integer
        vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f], // Overflow
        vec![0, 1, b'x', 2, b'y'],                      // Incomplete string
        vec![0, 1, b'x', 0x81, 0xff], // Invalid Huffman padding
        vec![0x82, 0x20],             // Table update after a field
    ] {
        for kind in [1, 5] {
            for continuation in [false, true] {
                let mut wire = Vec::new();
                let mut initial = if kind == 5 {
                    vec![0, 0, 0, 2]
                } else {
                    vec![]
                };
                if !continuation {
                    initial.extend_from_slice(&payload);
                }
                append_header_fragment(
                    &mut wire,
                    kind,
                    if continuation {
                        0
                    } else {
                        4
                    },
                    &initial,
                );
                if continuation {
                    append_header_fragment(&mut wire, 9, 4, &payload);
                }
                let mut codec = Codec::from(
                    mock_io::Builder::new()
                        .read(&wire)
                        .build(),
                );
                match codec.next().await.unwrap().unwrap_err() {
                    ProtoError::GoAway(_, reason, _) => assert_eq!(
                        reason,
                        Reason::COMPRESSION_ERROR,
                        "kind={kind} continuation={continuation} payload={payload:?}"
                    ),
                    other => {
                        panic!("wrong compression error scope: {other:?}")
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn hpack_resize_after_previous_fragment_is_compression_error() {
    use two_plz::proto::ProtoError;
    let mut wire = Vec::new();
    append_header_fragment(&mut wire, 1, 0, &[0x82]);
    append_header_fragment(&mut wire, 9, 4, &[0x20]);
    let mut codec = Codec::from(
        mock_io::Builder::new()
            .read(&wire)
            .build(),
    );
    match codec.next().await.unwrap().unwrap_err() {
        ProtoError::GoAway(_, reason, _) => {
            assert_eq!(reason, Reason::COMPRESSION_ERROR)
        }
        other => panic!("wrong late resize error scope: {other:?}"),
    }
}

#[tokio::test]
async fn hpack_split_representations_decode_at_every_boundary() {
    // Size update 4096, request pseudoheaders, and a new indexed literal.
    let block = [0x3f, 0xe1, 0x1f, 0x82, 0x87, 0x84, 0x40, 1, b'x', 1, b'y'];
    for split in 0..=block.len() {
        let mut wire = Vec::new();
        append_header_fragment(&mut wire, 1, 0, &block[..split]);
        append_header_fragment(&mut wire, 9, 4, &block[split..]);
        // A new block can resize again.
        append_header_fragment(&mut wire, 1, 4, &[0x20, 0x82]);
        let mut codec = Codec::from(
            mock_io::Builder::new()
                .read(&wire)
                .build(),
        );
        let (_, fields) = poll_frame!(Headers, codec).into_parts();
        let mut expected = HeaderMap::new();
        expected.insert("x", "y");
        assert_eq!(fields, expected, "split {split}");
        poll_frame!(Headers, codec);
        assert_closed!(codec);
    }
}

#[tokio::test]
async fn acknowledged_header_table_limits_require_minimum_and_latest_ceiling()
{
    use two_plz::proto::ProtoError;
    for (limits, update, accepted) in [
        (vec![0], vec![0x20], true),
        (vec![128], vec![0x3f, 0x61], true),
        (vec![0], vec![0x21], false),
        (vec![0], vec![], false), // Missing required reduction
        (vec![8192], vec![], true), // Increase need not change selected capacity
        (vec![0, 128], vec![0x3f, 0x61], false), // Skipped minimum
        (vec![0, 128], vec![0x20, 0x3f, 0x61], true),
        (vec![128, 0], vec![0x3f, 0x61], false), // Latest ceiling is zero
        (vec![128, 0], vec![0x20], true),
        (vec![128, 256, 64, 128], vec![0x3f, 0x21, 0x3f, 0x61], true),
        (vec![128, 256, 64, 128], vec![0x3f, 0x61], false),
    ] {
        let mut payload = update;
        payload.push(0x82);
        let mut wire = Vec::new();
        append_header_fragment(&mut wire, 1, 4, &payload);
        let mut codec = Codec::from(
            mock_io::Builder::new()
                .read(&wire)
                .build(),
        );
        for limit in limits {
            codec.set_recv_header_table_size(limit);
        }
        let result = codec.next().await.unwrap();
        if accepted {
            assert!(result.is_ok(), "{result:?}");
        } else {
            match result.unwrap_err() {
                ProtoError::GoAway(_, reason, _) => {
                    assert_eq!(reason, Reason::COMPRESSION_ERROR)
                }
                other => panic!("wrong size error scope: {other:?}"),
            }
        }
    }
}

#[tokio::test]
async fn required_table_reduction_cannot_be_omitted_from_empty_block() {
    use two_plz::proto::ProtoError;
    for continuation in [false, true] {
        let mut wire = Vec::new();
        append_header_fragment(
            &mut wire,
            1,
            if continuation {
                0
            } else {
                4
            },
            &[],
        );
        if continuation {
            append_header_fragment(&mut wire, 9, 4, &[]);
        }
        let mut codec = Codec::from(
            mock_io::Builder::new()
                .read(&wire)
                .build(),
        );
        codec.set_recv_header_table_size(0);
        match codec.next().await.unwrap().unwrap_err() {
            ProtoError::GoAway(_, reason, _) => {
                assert_eq!(reason, Reason::COMPRESSION_ERROR)
            }
            other => panic!(
                "missing empty-block update must fail compression: {other:?}"
            ),
        }
    }
}

#[tokio::test]
async fn acknowledged_decrease_then_increase_evicts_before_dynamic_reference()
{
    use two_plz::proto::ProtoError;
    let mut wire = Vec::new();
    append_header_fragment(&mut wire, 1, 4, &[0x40, 1, b'x', 1, b'y']);
    // ACKs are read separately, as Connection does before applying each limit.
    wire.extend_from_slice(&[0, 0, 0, 4, 1, 0, 0, 0, 0]);
    wire.extend_from_slice(&[0, 0, 0, 4, 1, 0, 0, 0, 0]);
    append_header_fragment(&mut wire, 1, 0, &[]);
    append_header_fragment(&mut wire, 9, 0, &[0x20, 0x3f]);
    append_header_fragment(&mut wire, 9, 4, &[0x61, 0xbe]);
    let mut codec = Codec::from(
        mock_io::Builder::new()
            .read(&wire)
            .build(),
    );
    poll_frame!(Headers, codec);
    poll_frame!(Settings, codec);
    codec.set_recv_header_table_size(0);
    poll_frame!(Settings, codec);
    codec.set_recv_header_table_size(128);
    match codec.next().await.unwrap().unwrap_err() {
        ProtoError::GoAway(_, reason, _) => {
            assert_eq!(reason, Reason::COMPRESSION_ERROR)
        }
        other => panic!(
            "evicted dynamic reference must fail compression: {other:?}"
        ),
    }
}

#[tokio::test]
async fn update_max_frame_len_in_flight_keeps_parsed_limit() {
    use futures::FutureExt;
    use tokio::io::AsyncWriteExt;
    let (io, mut peer) = tokio::io::duplex(32_768);
    let mut codec = Codec::from(io);
    codec.set_max_recv_frame_size(32_768);
    peer.write_all(&[0, 64, 1, 0, 0, 0, 0, 0, 1])
        .await
        .unwrap();
    assert!(codec.next().now_or_never().is_none());
    codec.set_max_recv_frame_size(16_384);
    peer.write_all(&vec![0; 16_385])
        .await
        .unwrap();
    assert_eq!(poll_frame!(Data, codec).payload().len(), 16_385);
    // The next frame is checked against the new limit.
    peer.write_all(&[0, 64, 1, 0, 0, 0, 0, 0, 1])
        .await
        .unwrap();
    assert_eq!(
        codec
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string(),
        "frame with invalid size"
    );
}

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

#[tokio::test]
async fn read_frame_too_big() {
    let mut codec = Codec::from(
        mock_io::Builder::new()
            .read(&[0, 64, 1, 0, 0, 0, 0, 0, 1])
            .build(),
    );
    assert_eq!(
        codec
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string(),
        "frame with invalid size"
    );
}

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

#[tokio::test]
async fn read_headers_without_pseudo() {
    let mut wire = Vec::new();
    append_header_fragment(&mut wire, 1, 4, &[0, 1, b'x', 1, b'y']);
    let mut codec = Codec::from(
        mock_io::Builder::new()
            .read(&wire)
            .build(),
    );
    let (pseudo, fields) = poll_frame!(Headers, codec).into_parts();
    assert_eq!(pseudo.method, None);
    let mut expected = HeaderMap::new();
    expected.insert("x", "y");
    assert_eq!(fields, expected);
}

#[tokio::test]
async fn read_headers_with_pseudo() {
    let mut wire = Vec::new();
    append_header_fragment(&mut wire, 1, 4, &[0x82, 0x87, 0x84]);
    let mut codec = Codec::from(
        mock_io::Builder::new()
            .read(&wire)
            .build(),
    );
    let (pseudo, fields) = poll_frame!(Headers, codec).into_parts();
    assert_eq!(pseudo.method, Some(Method::GET));
    assert!(fields.is_empty());
}

#[tokio::test]
async fn read_headers_empty_payload() {
    let mut wire = Vec::new();
    append_header_fragment(&mut wire, 1, 4, &[]);
    let mut codec = Codec::from(
        mock_io::Builder::new()
            .read(&wire)
            .build(),
    );
    let (pseudo, fields) = poll_frame!(Headers, codec).into_parts();
    assert_eq!(pseudo.method, None);
    assert!(fields.is_empty());
}

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
    // Changing the limit before parsing the next frame applies immediately.
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
async fn self_dependent_headers_preserve_hpack_table() {
    let block = [0x82, 0x87, 0x84, 0x40, 1, b'x', 1, b'y'];
    for split in 0..=block.len() {
        let fragmented = split != block.len();
        let mut wire = vec![
            0,
            0,
            (5 + split) as u8,
            1,
            if fragmented {
                0x21
            } else {
                0x25
            },
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            1,
            0,
        ]; // Priority depends on stream 1 itself.
        wire.extend_from_slice(&block[..split]);
        if fragmented {
            wire.extend_from_slice(&[0, 0, 0, 9, 0, 0, 0, 0, 1]);
            wire.extend_from_slice(&[
                0,
                0,
                (block.len() - split) as u8,
                9,
                4,
                0,
                0,
                0,
                1,
            ]);
            wire.extend_from_slice(&block[split..]);
        }
        assert_rejected_block_preserves_table(wire).await;
    }
}

#[tokio::test]
async fn malformed_pseudoheaders_preserve_hpack_table() {
    let malformed = [
        vec![0x40, 8, b':', b'u', b'n', b'k', b'n', b'o', b'w', b'n', 1, b'x'],
        vec![
            0x40, 7, b':', b's', b't', b'a', b't', b'u', b's', 3, b'a', b'b',
            b'c',
        ],
        vec![0x48, 3, b'a', b'b', b'c'], // Indexed :status name.
        vec![0x44, 1, 0xff],             // Indexed :path name, invalid UTF-8.
        vec![0x40, 5, b':', b'p', b'a', b't', b'h', 1, 0xff],
    ];
    for bad in malformed {
        let mut block = bad;
        block.extend_from_slice(&[0x40, 1, b'x', 1, b'y']);
        for split in 0..=block.len() {
            let fragmented = split != block.len();
            let mut wire = vec![
                0,
                0,
                split as u8,
                1,
                if fragmented {
                    1
                } else {
                    5
                },
                0,
                0,
                0,
                1,
            ];
            wire.extend_from_slice(&block[..split]);
            if fragmented {
                wire.extend_from_slice(&[
                    0,
                    0,
                    (block.len() - split) as u8,
                    9,
                    4,
                    0,
                    0,
                    0,
                    1,
                ]);
                wire.extend_from_slice(&block[split..]);
            }
            assert_rejected_block_preserves_table(wire).await;
        }
    }
}

async fn assert_rejected_block_preserves_table(mut wire: Vec<u8>) {
    use two_plz::proto::ProtoError;
    wire.extend_from_slice(&[
        0, 0, 4, 1, 5, 0, 0, 0, 3, 0x82, 0x87, 0x84, 0xbe,
    ]); // Dynamic index 62: x=y.
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
        other => panic!("expected stream reset: {other:?}"),
    }
    let headers = poll_frame!(Headers, codec);
    assert_eq!(headers.stream_id(), 3);
    let (_, fields) = headers.into_parts();
    let mut expected = HeaderMap::new();
    expected.insert("x", "y");
    assert_eq!(fields, expected);
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
