//! Offline, independently specified RFC 7541 Appendix C fixtures.
use crate::hpack::{Decoder, Encoder, Header};
use bytes::{Bytes, BytesMut};
use serde_json::Value;
use std::{fs, io::Cursor, path::Path};

struct Story {
    table_size: usize,
    cases: Vec<(Vec<u8>, Vec<Header>)>,
}

fn load_story(path: &Path) -> Story {
    let data = fs::read_to_string(path).unwrap_or_else(|e| {
        panic!("missing or unreadable HPACK fixture {}: {e}", path.display())
    });
    parse_story(&data)
}

fn parse_story(data: &str) -> Story {
    let story: Value =
        serde_json::from_str(data).expect("invalid HPACK fixture JSON");
    let cases = story["cases"]
        .as_array()
        .expect("fixture must contain cases");
    assert!(!cases.is_empty(), "fixture cases must not be empty");
    let table_size = usize::try_from(
        story["header_table_size"]
            .as_u64()
            .expect("fixture must specify header_table_size"),
    )
    .unwrap();
    let cases = cases
        .iter()
        .enumerate()
        .map(|(index, case)| {
            assert_eq!(
                case["seqno"].as_u64(),
                Some(index as u64),
                "fixture sequence must be contiguous and ordered"
            );
            let wire = hex::decode(
                case["wire"]
                    .as_str()
                    .expect("fixture wire must be hex"),
            )
            .expect("invalid fixture hex");
            assert!(!wire.is_empty(), "fixture wire must not be empty");
            let headers = case["headers"]
                .as_array()
                .expect("fixture must contain headers");
            assert!(!headers.is_empty(), "fixture headers must not be empty");
            let headers = headers
                .iter()
                .map(|header| {
                    let header = header
                        .as_object()
                        .expect("fixture header must be an object");
                    assert_eq!(
                        header.len(),
                        1,
                        "fixture header must have exactly one field"
                    );
                    let (name, value) = header.iter().next().unwrap();
                    Header::new(
                        Bytes::copy_from_slice(name.as_bytes()),
                        Bytes::copy_from_slice(
                            value
                                .as_str()
                                .expect("fixture value must be a string")
                                .as_bytes(),
                        ),
                    )
                    .unwrap()
                })
                .collect();
            (wire, headers)
        })
        .collect();
    Story {
        table_size,
        cases,
    }
}

fn decode(decoder: &mut Decoder, wire: &[u8]) -> Vec<Header> {
    let mut buf = BytesMut::from(wire);
    let mut headers = Vec::new();
    let mut cursor = Cursor::new(&mut buf);
    decoder
        .decode(&mut cursor, |header| headers.push(header))
        .unwrap();
    assert!(cursor.get_ref().is_empty(), "fixture must be fully consumed");
    headers
}

fn test_fixture(path: &str) {
    let story = load_story(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/hpack")
            .join(path),
    );
    let mut decoder = Decoder::new(story.table_size);
    // The published bytes, not this encoder, are the decoding oracle.
    for (wire, expected) in &story.cases {
        assert_eq!(&decode(&mut decoder, wire), expected, "{path}");
    }
    // Encoding strategy need not reproduce the RFC's indexing/Huffman choices.
    let mut encoder = Encoder::default();
    encoder.update_max_size(story.table_size);
    let mut decoder = Decoder::new(story.table_size);
    for (_, expected) in &story.cases {
        let mut wire = BytesMut::with_capacity(64 * 1024);
        let input: Vec<_> = expected
            .iter()
            .cloned()
            .map(Into::into)
            .collect();
        encoder.encode(input, &mut wire);
        assert_eq!(
            &decode(&mut decoder, &wire),
            expected,
            "{path} encoder round trip"
        );
    }
}

#[test]
fn rfc7541_requests_plain() {
    test_fixture("rfc7541/requests-plain.json");
}
#[test]
fn rfc7541_requests_huffman() {
    test_fixture("rfc7541/requests-huffman.json");
}
#[test]
fn rfc7541_responses_plain() {
    test_fixture("rfc7541/responses-plain.json");
}
#[test]
fn rfc7541_responses_huffman() {
    test_fixture("rfc7541/responses-huffman.json");
}

#[test]
#[should_panic(expected = "missing or unreadable HPACK fixture")]
fn missing_fixture_fails() {
    load_story(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/hpack/missing-fixture.json"),
    );
}
#[test]
#[should_panic(expected = "fixture must contain cases")]
fn missing_cases_fails() {
    parse_story("{}");
}
#[test]
#[should_panic(expected = "fixture cases must not be empty")]
fn empty_cases_fails() {
    parse_story(r#"{"cases":[]}"#);
}
#[test]
#[should_panic(expected = "invalid HPACK fixture JSON")]
fn malformed_fixture_fails() {
    parse_story("{");
}

#[test]
fn raw_octets_literal_and_indexed_reference() {
    // Hand-authored RFC 7541 section 6.2.1 literal: one-byte name x,
    // two-byte non-UTF-8 value; then section 6.1 index 62 (newest entry).
    let expected =
        Header::new(Bytes::from_static(b"x"), Bytes::from_static(b"\x80\xff"))
            .unwrap();
    let mut decoder = Decoder::default();
    assert_eq!(
        decode(&mut decoder, b"\x40\x01x\x02\x80\xff"),
        vec![expected.clone()]
    );
    assert_eq!(decode(&mut decoder, b"\xbe"), vec![expected.clone()]);
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::default();
    for _ in 0..2 {
        let mut wire = BytesMut::with_capacity(1024);
        encoder.encode(vec![expected.clone().into()], &mut wire);
        assert_eq!(decode(&mut decoder, &wire), vec![expected.clone()]);
    }
}
