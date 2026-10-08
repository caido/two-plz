use super::{StreamDependency, StreamId, util};
use crate::ext::Protocol;
use crate::frame::{Error, Frame, Head, Kind};
use crate::hpack::{self, BytesStr};
use header_plz::uri::Uri;
use header_plz::uri::scheme::Scheme;
use http_plz::Request;

use header_plz::Method;
use header_plz::StatusCode;
use header_plz::const_headers::*;
use header_plz::message_head::header_map::Hmap;
use header_plz::{Header, HeaderMap};
use header_plz::{RequestPseudoHeader, RequestSensitivity};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use std::fmt;
use std::io::Cursor;

type EncodeBuf<'a> = bytes::buf::Limit<&'a mut BytesMut>;

/// Header frame
///
/// This could be either a request or a response.
#[derive(Eq, PartialEq)]
pub struct Headers {
    /// The ID of the stream with which this frame is associated.
    stream_id: StreamId,

    /// The stream dependency information, if any.
    stream_dep: Option<StreamDependency>,

    /// The header block fragment
    header_block: HeaderBlock,

    /// The associated flags
    flags: HeadersFlag,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct HeadersFlag(u8);

#[derive(Eq, PartialEq)]
pub struct PushPromise {
    /// The ID of the stream with which this frame is associated.
    stream_id: StreamId,

    /// The ID of the stream being reserved by this PushPromise.
    promised_id: StreamId,

    /// The header block fragment
    header_block: HeaderBlock,

    /// The associated flags
    flags: PushPromiseFlag,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct PushPromiseFlag(u8);

#[derive(Debug)]
pub struct Continuation {
    /// Stream ID of continuation frame
    stream_id: StreamId,

    header_block: EncodingHeaderBlock,
}

// TODO(hyper): These fields shouldn't be `pub`
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Pseudo {
    // Request
    pub method: Option<Method>,
    pub scheme: Option<BytesStr>,
    pub authority: Option<BytesStr>,
    pub path: Option<BytesStr>,
    pub protocol: Option<Protocol>,

    // Response
    pub status: Option<StatusCode>,
    pub sensitivity: RequestSensitivity,
    pub status_sensitive: bool,
}

#[derive(Debug)]
pub struct Iter {
    /// Pseudo headers
    pseudo: Option<Pseudo>,

    /// Header fields
    fields: std::vec::IntoIter<Header>,
}

#[derive(Debug, Eq)]
struct HeaderBlock {
    /// The decoded header fields
    fields: HeaderMap,

    /// Precomputed size of all of our header fields, for perf reasons
    field_size: usize,

    /// Set to true if decoding went over the max header list size.
    is_over_size: bool,

    /// Validation state retained across header block fragments.
    regular_field_seen: bool,
    malformed: bool,

    /// Pseudo headers, these are broken out as they must be sent as part of the
    /// headers frame.
    pseudo: Pseudo,
}

impl PartialEq for HeaderBlock {
    fn eq(&self, other: &Self) -> bool {
        // Decoder progress is not part of the frame's semantic contents.
        self.fields == other.fields
            && self.field_size == other.field_size
            && self.is_over_size == other.is_over_size
            && self.pseudo == other.pseudo
    }
}

#[derive(Debug)]
struct EncodingHeaderBlock {
    hpack: Bytes,
}

const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const PRIORITY: u8 = 0x20;
const ALL: u8 = END_STREAM | END_HEADERS | PADDED | PRIORITY;

// ===== impl Headers =====

impl Headers {
    /// Create a new HEADERS frame
    pub fn new(
        stream_id: StreamId,
        pseudo: Pseudo,
        fields: HeaderMap,
    ) -> Self {
        Headers {
            stream_id,
            stream_dep: None,
            header_block: HeaderBlock {
                field_size: calculate_headermap_size(&fields),
                fields,
                is_over_size: false,
                regular_field_seen: false,
                malformed: false,
                pseudo,
            },
            flags: HeadersFlag::default(),
        }
    }

    pub fn trailers(stream_id: StreamId, fields: HeaderMap) -> Self {
        let mut flags = HeadersFlag::default();
        flags.set_end_stream();

        Headers {
            stream_id,
            stream_dep: None,
            header_block: HeaderBlock {
                field_size: calculate_headermap_size(&fields),
                fields,
                is_over_size: false,
                regular_field_seen: false,
                malformed: false,
                pseudo: Pseudo::default(),
            },
            flags,
        }
    }

    /// Loads the header frame but doesn't actually do HPACK decoding.
    ///
    /// HPACK decoding is done in the `load_hpack` step.
    pub fn load(
        head: Head,
        mut src: BytesMut,
    ) -> Result<(Self, BytesMut), Error> {
        let flags = HeadersFlag(head.flag());
        let mut pad = 0;

        tracing::trace!("loading headers; flags={:?}", flags);

        if head.stream_id().is_zero() {
            return Err(Error::InvalidStreamId);
        }

        // Read the padding length
        if flags.is_padded() {
            if src.is_empty() {
                return Err(Error::MalformedMessage);
            }
            pad = src[0] as usize;

            // Drop the padding
            src.advance(1);
        }

        // Read the stream dependency
        let stream_dep = if flags.is_priority() {
            if src.len() < 5 {
                return Err(Error::MalformedMessage);
            }
            let stream_dep = StreamDependency::load(&src[..5])?;

            // Drop the next 5 bytes
            src.advance(5);

            Some(stream_dep)
        } else {
            None
        };

        if pad > 0 {
            if pad > src.len() {
                return Err(Error::TooMuchPadding);
            }

            let len = src.len() - pad;
            src.truncate(len);
        }

        // Stream errors must not interrupt consumption of the connection's
        // HPACK block, including any subsequent CONTINUATION frames.
        let invalid_dependency = stream_dep
            .as_ref()
            .is_some_and(|dep| dep.dependency_id() == head.stream_id());
        let headers = Headers {
            stream_id: head.stream_id(),
            stream_dep,
            header_block: HeaderBlock {
                fields: HeaderMap::new(),
                field_size: 0,
                is_over_size: false,
                regular_field_seen: false,
                malformed: invalid_dependency,
                pseudo: Pseudo::default(),
            },
            flags,
        };

        Ok((headers, src))
    }

    pub fn load_hpack(
        &mut self,
        src: &mut BytesMut,
        max_header_list_size: usize,
        decoder: &mut hpack::Decoder,
    ) -> Result<(), Error> {
        self.header_block
            .load(src, max_header_list_size, decoder)
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn is_end_headers(&self) -> bool {
        self.flags.is_end_headers()
    }

    pub fn set_end_headers(&mut self) {
        self.flags.set_end_headers();
    }

    pub fn is_end_stream(&self) -> bool {
        self.flags.is_end_stream()
    }

    pub fn set_end_stream(&mut self) {
        self.flags.set_end_stream()
    }

    pub fn unset_end_stream(&mut self) {
        self.flags.unset_end_stream()
    }

    pub fn is_over_size(&self) -> bool {
        self.header_block.is_over_size
    }

    pub fn into_parts(self) -> (Pseudo, HeaderMap) {
        (self.header_block.pseudo, self.header_block.fields)
    }

    #[cfg(feature = "unstable")]
    pub fn pseudo_mut(&mut self) -> &mut Pseudo {
        &mut self.header_block.pseudo
    }

    pub(crate) fn pseudo(&self) -> &Pseudo {
        &self.header_block.pseudo
    }

    /// Whether it has status 1xx
    pub(crate) fn is_informational(&self) -> bool {
        self.header_block
            .pseudo
            .is_informational()
    }

    pub fn fields(&self) -> &HeaderMap {
        &self.header_block.fields
    }

    pub fn into_fields(self) -> HeaderMap {
        self.header_block.fields
    }

    pub fn encode(
        self,
        encoder: &mut hpack::Encoder,
        dst: &mut EncodeBuf<'_>,
    ) -> Option<Continuation> {
        // At this point, the `is_end_headers` flag should always be set
        debug_assert!(self.flags.is_end_headers());

        // Get the HEADERS frame head
        let head = self.head();

        self.header_block
            .into_encoding(encoder)
            .encode(&head, dst, |_| {})
    }

    fn head(&self) -> Head {
        Head::new(Kind::Headers, self.flags.into(), self.stream_id)
    }
}

impl<T> From<Headers> for Frame<T> {
    fn from(src: Headers) -> Self {
        Frame::Headers(src)
    }
}

impl fmt::Debug for Headers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("Headers");
        builder
            .field("stream_id", &self.stream_id)
            .field("flags", &self.flags);

        if let Some(ref protocol) = self.header_block.pseudo.protocol {
            builder.field("protocol", protocol);
        }

        if let Some(ref dep) = self.stream_dep {
            builder.field("stream_dep", dep);
        }

        // `fields` and `pseudo` purposefully not included
        builder.finish()
    }
}

// ===== util =====

#[derive(Debug, PartialEq, Eq)]
pub struct ParseU64Error;

pub fn parse_u64(src: &[u8]) -> Result<u64, ParseU64Error> {
    if src.is_empty() {
        return Err(ParseU64Error);
    }

    let mut ret = 0u64;

    for &d in src {
        if !d.is_ascii_digit() {
            return Err(ParseU64Error);
        }

        ret = ret
            .checked_mul(10)
            .and_then(|value| value.checked_add((d - b'0') as u64))
            .ok_or(ParseU64Error)?;
    }

    Ok(ret)
}

#[cfg(test)]
#[test]
fn content_length_integer_boundaries() {
    assert_eq!(parse_u64(b"0"), Ok(0));
    assert_eq!(parse_u64(b"00000000000000000000001"), Ok(1));
    assert_eq!(parse_u64(b"18446744073709551615"), Ok(u64::MAX));
    for invalid in [b"".as_slice(), b"18446744073709551616", b"-1", b"1x"] {
        assert_eq!(parse_u64(invalid), Err(ParseU64Error));
    }
}

// ===== impl PushPromise =====

#[derive(Debug)]
pub enum PushPromiseHeaderError {
    InvalidContentLength(Result<u64, ParseU64Error>),
    NotSafeAndCacheable,
}

impl PushPromise {
    pub fn new(
        stream_id: StreamId,
        promised_id: StreamId,
        pseudo: Pseudo,
        fields: HeaderMap,
    ) -> Self {
        PushPromise {
            flags: PushPromiseFlag::default(),
            header_block: HeaderBlock {
                field_size: calculate_headermap_size(&fields),
                fields,
                is_over_size: false,
                regular_field_seen: false,
                malformed: false,
                pseudo,
            },
            promised_id,
            stream_id,
        }
    }

    pub fn validate_request(
        req: &Request,
    ) -> Result<(), PushPromiseHeaderError> {
        use PushPromiseHeaderError::*;
        // The spec has some requirements for promised request headers
        // [https://httpwg.org/specs/rfc7540.html#PushRequests]

        // A promised request "that indicates the presence of a request body
        // MUST reset the promised stream with a stream error"
        if let Some(content_length) = req
            .headers()
            .value_of_key(CONTENT_LENGTH)
        {
            let parsed_length = parse_u64(content_length);
            if parsed_length != Ok(0) {
                return Err(InvalidContentLength(parsed_length));
            }
        }
        // "The server MUST include a method in the :method pseudo-header field
        // that is safe and cacheable"
        if !Self::safe_and_cacheable(req.method()) {
            return Err(NotSafeAndCacheable);
        }

        Ok(())
    }

    fn safe_and_cacheable(method: &Method) -> bool {
        // Cacheable: https://httpwg.org/specs/rfc7231.html#cacheable.methods
        // Safe: https://httpwg.org/specs/rfc7231.html#safe.methods
        *method == Method::GET || *method == Method::HEAD
    }

    pub fn fields(&self) -> &HeaderMap {
        &self.header_block.fields
    }

    #[cfg(feature = "unstable")]
    pub fn into_fields(self) -> HeaderMap {
        self.header_block.fields
    }

    /// Loads the push promise frame but doesn't actually do HPACK decoding.
    ///
    /// HPACK decoding is done in the `load_hpack` step.
    pub fn load(
        head: Head,
        mut src: BytesMut,
    ) -> Result<(Self, BytesMut), Error> {
        let flags = PushPromiseFlag(head.flag());
        let mut pad = 0;

        if head.stream_id().is_zero() {
            return Err(Error::InvalidStreamId);
        }

        // Read the padding length
        if flags.is_padded() {
            if src.is_empty() {
                return Err(Error::MalformedMessage);
            }

            // The length byte exists; below, require the four-byte promised
            // ID and ensure padding fits in the remaining payload.
            pad = src[0] as usize;

            // Drop the padding
            src.advance(1);
        }

        if src.len() < 4 {
            return Err(Error::MalformedMessage);
        }

        let (promised_id, _) = StreamId::parse(&src[..4]);
        // Drop promised_id bytes
        src.advance(4);

        if pad > 0 {
            if pad > src.len() {
                return Err(Error::TooMuchPadding);
            }

            let len = src.len() - pad;
            src.truncate(len);
        }

        let frame = PushPromise {
            flags,
            header_block: HeaderBlock {
                fields: HeaderMap::new(),
                field_size: 0,
                is_over_size: false,
                regular_field_seen: false,
                malformed: false,
                pseudo: Pseudo::default(),
            },
            promised_id,
            stream_id: head.stream_id(),
        };
        Ok((frame, src))
    }

    pub fn load_hpack(
        &mut self,
        src: &mut BytesMut,
        max_header_list_size: usize,
        decoder: &mut hpack::Decoder,
    ) -> Result<(), Error> {
        self.header_block
            .load(src, max_header_list_size, decoder)
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn promised_id(&self) -> StreamId {
        self.promised_id
    }

    pub(crate) fn is_malformed(&self) -> bool {
        self.header_block.malformed
    }

    pub fn is_end_headers(&self) -> bool {
        self.flags.is_end_headers()
    }

    pub fn set_end_headers(&mut self) {
        self.flags.set_end_headers();
    }

    pub fn is_over_size(&self) -> bool {
        self.header_block.is_over_size
    }

    pub fn encode(
        self,
        encoder: &mut hpack::Encoder,
        dst: &mut EncodeBuf<'_>,
    ) -> Option<Continuation> {
        // At this point, the `is_end_headers` flag should always be set
        debug_assert!(self.flags.is_end_headers());

        let head = self.head();
        let promised_id = self.promised_id;

        self.header_block
            .into_encoding(encoder)
            .encode(&head, dst, |dst| {
                dst.put_u32(promised_id.into());
            })
    }

    fn head(&self) -> Head {
        Head::new(Kind::PushPromise, self.flags.into(), self.stream_id)
    }

    /// Consume `self`, returning the parts of the frame
    pub fn into_parts(self) -> (Pseudo, HeaderMap) {
        (self.header_block.pseudo, self.header_block.fields)
    }
}

impl<T> From<PushPromise> for Frame<T> {
    fn from(src: PushPromise) -> Self {
        Frame::PushPromise(src)
    }
}

impl fmt::Debug for PushPromise {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PushPromise")
            .field("stream_id", &self.stream_id)
            .field("promised_id", &self.promised_id)
            .field("flags", &self.flags)
            // `fields` and `pseudo` purposefully not included
            .finish()
    }
}

// ===== impl Continuation =====

impl Continuation {
    fn head(&self) -> Head {
        Head::new(Kind::Continuation, END_HEADERS, self.stream_id)
    }

    pub fn encode(self, dst: &mut EncodeBuf<'_>) -> Option<Continuation> {
        // Get the CONTINUATION frame head
        let head = self.head();

        self.header_block
            .encode(&head, dst, |_| {})
    }
}

// ===== impl Pseudo =====

impl Pseudo {
    pub fn request(
        method: Method,
        uri: Uri,
        protocol: Option<Protocol>,
    ) -> Self {
        let (scheme, path) = if method == Method::CONNECT && protocol.is_none()
        {
            (None, None)
        } else {
            let path = uri
                .path_and_query()
                // TODO: avoid cloning
                .clone()
                .into_inner();
            let path = if !path.is_empty() {
                BytesStr::from(path.into_inner())
            } else if method == Method::OPTIONS {
                BytesStr::from_static("*")
            } else {
                BytesStr::from_static("/")
            };
            (uri.scheme(), Some(path))
        };

        let mut pseudo = Pseudo {
            method: Some(method),
            scheme: None,
            authority: None,
            path,
            protocol,
            status: None,
            sensitivity: RequestSensitivity::default(),
            status_sensitive: false,
        };

        // If the URI includes a scheme component, add it to the pseudo headers
        if let Some(scheme) = scheme {
            pseudo.set_scheme(scheme.clone());
        }
        // If the URI includes an authority component, add it to the pseudo
        // headers
        if let Some(authority) = uri.authority() {
            pseudo.set_authority(BytesStr::from(authority));
        }

        pseudo
    }

    pub fn response(status: StatusCode) -> Self {
        Pseudo {
            method: None,
            scheme: None,
            authority: None,
            path: None,
            protocol: None,
            status: Some(status),
            sensitivity: RequestSensitivity::default(),
            status_sensitive: false,
        }
    }

    #[cfg(feature = "unstable")]
    pub fn set_status(&mut self, value: StatusCode) {
        self.status = Some(value);
    }

    pub fn set_scheme(&mut self, scheme: Scheme) {
        let bytes_str = match scheme.as_str() {
            "http" => BytesStr::from_static("http"),
            "https" => BytesStr::from_static("https"),
            s => BytesStr::from(s),
        };
        self.scheme = Some(bytes_str);
    }

    #[cfg(feature = "unstable")]
    pub fn set_protocol(&mut self, protocol: Protocol) {
        self.protocol = Some(protocol);
    }

    pub fn set_authority(&mut self, authority: BytesStr) {
        self.authority = Some(authority);
    }

    /// Whether it has status 1xx
    pub(crate) fn is_informational(&self) -> bool {
        self.status
            .is_some_and(|status| status.is_informational())
    }
}

// ===== impl EncodingHeaderBlock =====

impl EncodingHeaderBlock {
    fn encode<F>(
        mut self,
        head: &Head,
        dst: &mut EncodeBuf<'_>,
        f: F,
    ) -> Option<Continuation>
    where
        F: FnOnce(&mut EncodeBuf<'_>),
    {
        let head_pos = dst.get_ref().len();

        // At this point, we don't know how big the h2 frame will be.
        // So, we write the head with length 0, then write the body, and
        // finally write the length once we know the size.
        head.encode(0, dst);

        let payload_pos = dst.get_ref().len();

        f(dst);

        // Now, encode the header payload
        let continuation = if self.hpack.len() > dst.remaining_mut() {
            dst.put((&mut self.hpack).take(dst.remaining_mut()));

            Some(Continuation {
                stream_id: head.stream_id(),
                header_block: self,
            })
        } else {
            dst.put_slice(&self.hpack);

            None
        };

        // Compute the header block length
        let payload_len = (dst.get_ref().len() - payload_pos) as u64;

        // Write the frame length
        let payload_len_be = payload_len.to_be_bytes();
        assert!(
            payload_len_be[0..5]
                .iter()
                .all(|b| *b == 0)
        );
        (dst.get_mut()[head_pos..head_pos + 3])
            .copy_from_slice(&payload_len_be[5..]);

        if continuation.is_some() {
            // There will be continuation frames, so the `is_end_headers` flag
            // must be unset
            debug_assert!(
                dst.get_ref()[head_pos + 4] & END_HEADERS == END_HEADERS
            );

            dst.get_mut()[head_pos + 4] -= END_HEADERS;
        }

        continuation
    }
}

// ===== impl Iter =====

impl Iterator for Iter {
    type Item = hpack::Header<Option<Bytes>>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::hpack::Header::*;

        if let Some(ref mut pseudo) = self.pseudo {
            if let Some(method) = pseudo.method.take() {
                return Some(
                    Method(method).with_sensitive(
                        pseudo
                            .sensitivity
                            .is_sensitive(RequestPseudoHeader::Method),
                    ),
                );
            }

            if let Some(scheme) = pseudo.scheme.take() {
                return Some(
                    Scheme(scheme).with_sensitive(
                        pseudo
                            .sensitivity
                            .is_sensitive(RequestPseudoHeader::Scheme),
                    ),
                );
            }

            if let Some(authority) = pseudo.authority.take() {
                return Some(
                    Authority(authority).with_sensitive(
                        pseudo
                            .sensitivity
                            .is_sensitive(RequestPseudoHeader::Authority),
                    ),
                );
            }

            if let Some(path) = pseudo.path.take() {
                return Some(
                    Path(path).with_sensitive(
                        pseudo
                            .sensitivity
                            .is_sensitive(RequestPseudoHeader::Path),
                    ),
                );
            }

            if let Some(protocol) = pseudo.protocol.take() {
                return Some(
                    Protocol(protocol).with_sensitive(
                        pseudo
                            .sensitivity
                            .is_sensitive(RequestPseudoHeader::Protocol),
                    ),
                );
            }

            if let Some(status) = pseudo.status.take() {
                return Some(
                    Status(status).with_sensitive(pseudo.status_sensitive),
                );
            }
        }

        self.pseudo = None;

        self.fields.next().map(|h| {
            let (name, value, sensitive) = h.into_parts();
            let name = Some(name);
            Field {
                name,
                value,
            }
            .with_sensitive(sensitive)
        })
    }
}

#[cfg(test)]
#[test]
fn pseudo_sensitivity_message_forwarding() {
    use crate::message::{IntoPseudo, frames_to_request, frames_to_response};
    fn roundtrip(pseudo: Pseudo, response: bool) {
        let mut fields = HeaderMap::new();
        let mut regular = Header::new(
            Bytes::from_static(b"x-secret"),
            Bytes::from_static(b"opaque"),
        );
        regular.set_sensitive(true);
        fields.extend([regular]);
        let original_policy = pseudo.sensitivity;
        let original_status = pseudo.status_sensitive;
        let mut encoder = hpack::Encoder::default();
        // Warm all static/dynamic matches before encoding the marked fields.
        let mut warm = BytesMut::new();
        let mut unmarked = Pseudo::default();
        unmarked.method = pseudo.method.clone();
        unmarked.scheme = pseudo.scheme.clone();
        unmarked.authority = pseudo.authority.clone();
        unmarked.path = pseudo.path.clone();
        unmarked.protocol = pseudo.protocol.clone();
        unmarked.status = pseudo.status;
        encoder.encode(
            Iter {
                pseudo: Some(unmarked),
                fields: HeaderMap::new().into_iter(),
            },
            &mut warm,
        );
        let mut decoder = hpack::Decoder::default();
        decoder
            .decode(&mut Cursor::new(&mut warm), |_| {})
            .unwrap();
        let mut wire = BytesMut::new();
        encoder.encode(
            Iter {
                pseudo: Some(pseudo),
                fields: fields.into_iter(),
            },
            &mut wire,
        );
        let mut frame = Headers::new(
            StreamId::from(1),
            Pseudo::default(),
            HeaderMap::new(),
        );
        // Decode bytewise, exercising policy persistence across fragments.
        let bytes = wire.freeze();
        let mut fragment = BytesMut::new();
        for byte in bytes {
            fragment.extend_from_slice(&[byte]);
            match frame.header_block.load(
                &mut fragment,
                usize::MAX,
                &mut decoder,
            ) {
                Ok(())
                | Err(Error::Hpack(hpack::DecoderError::NeedMore(_))) => {}
                Err(error) => panic!("unexpected fragment error: {error:?}"),
            }
        }
        let (pseudo, fields) = frame.into_parts();
        assert_eq!(pseudo.sensitivity, original_policy);
        assert_eq!(pseudo.status_sensitive, original_status);
        let (forwarded, fields) = if response {
            let (line, fields) =
                frames_to_response(pseudo, fields, StreamId::from(1))
                    .unwrap()
                    .into_message_head();
            assert_eq!(line.is_sensitive(), original_status);
            (line.into_pseudo(), fields)
        } else {
            let scheme = pseudo.scheme.clone();
            let authority = pseudo.authority.clone();
            let path = pseudo.path.clone();
            let protocol = pseudo.protocol.clone();
            let (line, fields) =
                frames_to_request(pseudo, fields, StreamId::from(1))
                    .unwrap()
                    .into_message_head();
            assert_eq!(line.sensitivity(), original_policy);
            let forwarded = line.into_pseudo();
            assert_eq!(forwarded.scheme, scheme);
            assert_eq!(forwarded.authority, authority);
            assert_eq!(forwarded.path, path);
            assert_eq!(forwarded.protocol, protocol);
            (forwarded, fields)
        };
        assert!(
            fields
                .iter()
                .next()
                .unwrap()
                .is_sensitive()
        );
        let expected: Vec<_> = Iter {
            pseudo: Some(forwarded),
            fields: fields.into_iter(),
        }
        .collect();
        let mut wire = BytesMut::new();
        encoder.encode(expected.clone(), &mut wire);
        let mut index = 0;
        decoder
            .decode(&mut Cursor::new(&mut wire), |header| {
                assert_eq!(
                    header.is_sensitive(),
                    expected[index].is_sensitive()
                );
                assert!(
                    header.value_eq(&expected[index].clone().reify().unwrap())
                );
                index += 1;
            })
            .unwrap();
        assert_eq!(index, expected.len());
    }
    for selector in [
        RequestPseudoHeader::Method,
        RequestPseudoHeader::Scheme,
        RequestPseudoHeader::Authority,
        RequestPseudoHeader::Path,
        RequestPseudoHeader::Protocol,
    ] {
        let uri = Uri::builder()
            .scheme(Scheme::HTTPS)
            .authority("example.com")
            .path("/socket?q=1")
            .build()
            .unwrap();
        let mut pseudo = Pseudo::request(
            Method::CONNECT,
            uri,
            Some(Protocol::from_static("websocket")),
        );
        pseudo
            .sensitivity
            .set_sensitive(selector, true);
        roundtrip(pseudo, false);
    }
    let mut pseudo = Pseudo::request(
        Method::GET,
        Uri::builder()
            .scheme(Scheme::HTTPS)
            .path("/only?q=1")
            .build()
            .unwrap(),
        None,
    );
    pseudo
        .sensitivity
        .set_sensitive(RequestPseudoHeader::Scheme, true);
    roundtrip(pseudo, false);
    let mut pseudo = Pseudo::request(
        Method::CONNECT,
        Uri::builder()
            .authority("example.com:443")
            .build()
            .unwrap(),
        None,
    );
    pseudo
        .sensitivity
        .set_sensitive(RequestPseudoHeader::Authority, true);
    roundtrip(pseudo, false);
    for status in [StatusCode::OK, StatusCode::CONTINUE] {
        let mut pseudo = Pseudo::response(status);
        pseudo.status_sensitive = true;
        roundtrip(pseudo, true);
    }
}

#[cfg(test)]
#[test]
fn sensitivity_duplicate_forwarding() {
    let mut fields = HeaderMap::new();
    for sensitive in [false, true, false, true] {
        let mut field = Header::new(
            Bytes::from_static(b"x-secret"),
            Bytes::from_static(b"same"),
        );
        field.set_sensitive(sensitive);
        fields.extend(std::iter::once(field));
    }
    let mut encoder = hpack::Encoder::default();
    let block = HeaderBlock {
        fields,
        field_size: 0,
        is_over_size: false,
        regular_field_seen: false,
        malformed: false,
        pseudo: Pseudo::default(),
    }
    .into_encoding(&mut encoder);
    let mut decoded =
        Headers::new(StreamId::from(1), Pseudo::default(), HeaderMap::new());
    let mut decoder = hpack::Decoder::default();
    let mut wire = BytesMut::from(block.hpack.as_ref());
    decoded
        .header_block
        .load(&mut wire, usize::MAX, &mut decoder)
        .unwrap();
    let flags: Vec<_> = decoded
        .header_block
        .fields
        .iter()
        .map(|h| h.is_sensitive())
        .collect();
    assert_eq!(flags, [false, true, false, true]);
    let mut forwarded = BytesMut::new();
    encoder.encode(
        Iter {
            pseudo: None,
            fields: decoded.header_block.fields.into_iter(),
        },
        &mut forwarded,
    );
    let mut flags = Vec::new();
    decoder
        .decode(&mut std::io::Cursor::new(&mut forwarded), |h| {
            flags.push(h.is_sensitive())
        })
        .unwrap();
    assert_eq!(flags, [false, true, false, true]);
}

// ===== impl HeadersFlag =====

impl HeadersFlag {
    pub fn empty() -> HeadersFlag {
        HeadersFlag(0)
    }

    pub fn load(bits: u8) -> HeadersFlag {
        HeadersFlag(bits & ALL)
    }

    pub fn is_end_stream(&self) -> bool {
        self.0 & END_STREAM == END_STREAM
    }

    pub fn set_end_stream(&mut self) {
        self.0 |= END_STREAM;
    }

    pub fn unset_end_stream(&mut self) {
        self.0 &= !END_STREAM;
    }

    pub fn is_end_headers(&self) -> bool {
        self.0 & END_HEADERS == END_HEADERS
    }

    pub fn set_end_headers(&mut self) {
        self.0 |= END_HEADERS;
    }

    pub fn is_padded(&self) -> bool {
        self.0 & PADDED == PADDED
    }

    pub fn is_priority(&self) -> bool {
        self.0 & PRIORITY == PRIORITY
    }
}

impl Default for HeadersFlag {
    /// Returns a `HeadersFlag` value with `END_HEADERS` set.
    fn default() -> Self {
        HeadersFlag(END_HEADERS)
    }
}

impl From<HeadersFlag> for u8 {
    fn from(src: HeadersFlag) -> u8 {
        src.0
    }
}

impl fmt::Debug for HeadersFlag {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        util::debug_flags(fmt, self.0)
            .flag_if(self.is_end_headers(), "END_HEADERS")
            .flag_if(self.is_end_stream(), "END_STREAM")
            .flag_if(self.is_padded(), "PADDED")
            .flag_if(self.is_priority(), "PRIORITY")
            .finish()
    }
}

// ===== impl PushPromiseFlag =====

impl PushPromiseFlag {
    pub fn empty() -> PushPromiseFlag {
        PushPromiseFlag(0)
    }

    pub fn load(bits: u8) -> PushPromiseFlag {
        PushPromiseFlag(bits & ALL)
    }

    pub fn is_end_headers(&self) -> bool {
        self.0 & END_HEADERS == END_HEADERS
    }

    pub fn set_end_headers(&mut self) {
        self.0 |= END_HEADERS;
    }

    pub fn is_padded(&self) -> bool {
        self.0 & PADDED == PADDED
    }
}

impl Default for PushPromiseFlag {
    /// Returns a `PushPromiseFlag` value with `END_HEADERS` set.
    fn default() -> Self {
        PushPromiseFlag(END_HEADERS)
    }
}

impl From<PushPromiseFlag> for u8 {
    fn from(src: PushPromiseFlag) -> u8 {
        src.0
    }
}

impl fmt::Debug for PushPromiseFlag {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        util::debug_flags(fmt, self.0)
            .flag_if(self.is_end_headers(), "END_HEADERS")
            .flag_if(self.is_padded(), "PADDED")
            .finish()
    }
}

// ===== HeaderBlock =====

impl HeaderBlock {
    fn load(
        &mut self,
        src: &mut BytesMut,
        max_header_list_size: usize,
        decoder: &mut hpack::Decoder,
    ) -> Result<(), Error> {
        let mut reg = self.regular_field_seen;
        let mut malformed = self.malformed;
        let mut headers_size = self.calculate_header_list_size();

        macro_rules! set_pseudo {
            ($field:ident, $val:expr, $policy:expr) => {{
                if reg {
                    tracing::trace!("load_hpack; header malformed -- pseudo not at head of block");
                    malformed = true;
                } else if self.pseudo.$field.is_some() {
                    tracing::trace!("load_hpack; header malformed -- repeated pseudo");
                    malformed = true;
                } else {
                    let __val = $val;
                    headers_size +=
                        decoded_header_size(stringify!($field).len() + 1, __val.as_str().len());
                    if headers_size < max_header_list_size {
                        self.pseudo.$field = Some(__val);
                        $policy;
                    } else if !self.is_over_size {
                        tracing::trace!("load_hpack; header list size over max");
                        self.is_over_size = true;
                    }
                }
            }};
        }

        let mut cursor = Cursor::new(src);

        // If the header frame is malformed, we still have to continue decoding
        // the headers. A malformed header frame is a stream level error, but
        // the hpack state is connection level. In order to maintain correct
        // state for other streams, the hpack decoding process must complete.
        let res = decoder.decode_fragment(&mut cursor, |header| {
            use crate::hpack::Header::*;

            if !header.is_valid_field() {
                malformed = true;
                reg = true;
                let size = header.len();
                headers_size = headers_size.saturating_add(size);
                self.field_size = self.field_size.saturating_add(size);
                if headers_size >= max_header_list_size {
                    self.is_over_size = true;
                }
                return;
            }

            let sensitive = header.is_sensitive();
            match header.into_unmarked() {
                Sensitive(_) => unreachable!("sensitivity wrapper removed"),
                Field { name, value } => {
                    // Every regular field ends the pseudoheader section, even
                    // when the field itself makes the message malformed.
                    reg = true;
                    // Connection level header fields are not supported and must
                    // result in a protocol error.

                    if name.as_ref() == CONNECTION
                        || name.as_ref() == TRANSFER_ENCODING
                        || name.as_ref() == UPGRADE
                        || name.as_ref() == b"keep-alive"
                        || name.as_ref() == b"proxy-connection"
                    {
                        tracing::trace!("load_hpack; connection level header");
                        malformed = true;
                    } else if name.as_ref() == TE && !value.eq_ignore_ascii_case(b"trailers") {
                        tracing::trace!(
                            "load_hpack; TE header not set to trailers; val={:?}",
                            value
                        );
                        malformed = true;
                    } else {
                        reg = true;

                        headers_size += decoded_header_size(name.len(), value.len());
                        if headers_size < max_header_list_size {
                            self.field_size +=
                                decoded_header_size(name.len(), value.len());
                            let mut field = Header::new(name, value);
                            field.set_sensitive(sensitive);
                            self.fields.extend(std::iter::once(field));
                        } else if !self.is_over_size {
                            tracing::trace!("load_hpack; header list size over max");
                            self.is_over_size = true;
                        }
                    }
                }
                Authority(v) => set_pseudo!(authority, v, self.pseudo.sensitivity.set_sensitive(RequestPseudoHeader::Authority, sensitive)),
                Method(v) => set_pseudo!(method, v, self.pseudo.sensitivity.set_sensitive(RequestPseudoHeader::Method, sensitive)),
                Scheme(v) => set_pseudo!(scheme, v, self.pseudo.sensitivity.set_sensitive(RequestPseudoHeader::Scheme, sensitive)),
                Path(v) => set_pseudo!(path, v, self.pseudo.sensitivity.set_sensitive(RequestPseudoHeader::Path, sensitive)),
                Protocol(v) => set_pseudo!(protocol, v, self.pseudo.sensitivity.set_sensitive(RequestPseudoHeader::Protocol, sensitive)),
                Status(v) => set_pseudo!(status, v, self.pseudo.status_sensitive = sensitive),
            }
        });

        // Decoding may need another fragment after already validating fields.
        self.regular_field_seen = reg;
        self.malformed = malformed;

        if let Err(e) = res {
            tracing::trace!("hpack decoding error; err={:?}", e);
            return Err(e.into());
        }

        if malformed {
            tracing::trace!("malformed message");
            return Err(Error::MalformedMessage);
        }

        Ok(())
    }

    fn into_encoding(
        self,
        encoder: &mut hpack::Encoder,
    ) -> EncodingHeaderBlock {
        let mut hpack = BytesMut::new();
        let headers = Iter {
            pseudo: Some(self.pseudo),
            fields: self.fields.into_iter(),
        };

        encoder.encode(headers, &mut hpack);

        EncodingHeaderBlock {
            hpack: hpack.freeze(),
        }
    }

    /// Calculates the size of the currently decoded header list.
    ///
    /// According to http://httpwg.org/specs/rfc7540.html#SETTINGS_MAX_HEADER_LIST_SIZE
    ///
    /// > The value is based on the uncompressed size of header fields,
    /// > including the length of the name and value in octets plus an
    /// > overhead of 32 octets for each header field.
    fn calculate_header_list_size(&self) -> usize {
        macro_rules! pseudo_size {
            ($name:ident) => {{
                self.pseudo
                    .$name
                    .as_ref()
                    .map(|m| {
                        decoded_header_size(
                            stringify!($name).len() + 1,
                            m.as_str().len(),
                        )
                    })
                    .unwrap_or(0)
            }};
        }

        pseudo_size!(method)
            + pseudo_size!(scheme)
            + pseudo_size!(status)
            + pseudo_size!(authority)
            + pseudo_size!(path)
            + self.field_size
    }
}

fn calculate_headermap_size(map: &HeaderMap) -> usize {
    map.iter()
        .map(|h| h.len() + 32)
        //.map(|(name, value)| {
        //    decoded_header_size(name.as_str().len(), value.len())
        //})
        .sum::<usize>()
}

fn decoded_header_size(name: usize, value: usize) -> usize {
    name + value + 32
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame;
    use crate::hpack::{Encoder, huffman};

    fn huff_decode(src: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        huffman::decode(src, &mut buf).unwrap()
    }

    #[test]
    fn push_promise_empty_fragment_and_padding_boundaries() {
        for (flags, payload) in [
            (0, b"\0\0\0\x02".as_slice()),
            (8, b"\0\0\0\0\x02".as_slice()),
            (8, b"\x01\0\0\0\x02\0".as_slice()),
        ] {
            let head = Head::new(Kind::PushPromise, flags, StreamId::from(1));
            let (promise, fragment) =
                PushPromise::load(head, BytesMut::from(payload)).unwrap();
            assert_eq!(promise.promised_id(), StreamId::from(2));
            assert!(fragment.is_empty());
        }
        for payload in [b"".as_slice(), b"\0", b"\0\0\0"] {
            let head = Head::new(Kind::PushPromise, 0, StreamId::from(1));
            assert!(matches!(
                PushPromise::load(head, BytesMut::from(payload)),
                Err(Error::MalformedMessage)
            ));
        }
        let head = Head::new(Kind::PushPromise, 8, StreamId::from(1));
        assert!(matches!(
            PushPromise::load(head, BytesMut::from(&b"\x01\0\0\0\x02"[..])),
            Err(Error::TooMuchPadding)
        ));
    }

    #[test]
    fn outbound_opaque_value_round_trip() {
        let mut fields = HeaderMap::new();
        fields.insert(
            Bytes::from_static(b"x-opaque"),
            Bytes::from_static(b"\x80\xff"),
        );
        let headers =
            Headers::new(StreamId::from(1), Pseudo::default(), fields);
        let mut encoded = BytesMut::new();
        assert!(
            headers
                .encode(
                    &mut Encoder::default(),
                    &mut (&mut encoded).limit(4096)
                )
                .is_none()
        );
        let mut decoded = Vec::new();
        let mut payload = encoded.split_off(frame::HEADER_LEN);
        hpack::Decoder::default()
            .decode(&mut Cursor::new(&mut payload), |h| decoded.push(h))
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].value_slice(), b"\x80\xff");
    }

    #[test]
    fn malformed_fields_preserve_dynamic_table() {
        for name in [b"".as_slice(), b"X-bad", b"x bad"] {
            let mut decoder = hpack::Decoder::default();
            let mut encoder = Encoder::default();
            let bad = hpack::Header::new(
                Bytes::copy_from_slice(name),
                Bytes::from_static(b"value"),
            )
            .unwrap();
            let good = hpack::Header::new(
                Bytes::from_static(b"x-good"),
                Bytes::from_static(b"valid"),
            )
            .unwrap();
            let mut wire = BytesMut::new();
            encoder.encode(vec![bad.into(), good.clone().into()], &mut wire);
            let mut block = Headers::new(
                StreamId::from(1),
                Pseudo::default(),
                HeaderMap::new(),
            );
            assert!(
                block
                    .load_hpack(&mut wire, 4096, &mut decoder)
                    .is_err()
            );
            // The last incrementally indexed field is dynamic entry 62.
            let mut wire = BytesMut::from(&b"\xbe"[..]);
            let mut next = Headers::new(
                StreamId::from(3),
                Pseudo::default(),
                HeaderMap::new(),
            );
            next.load_hpack(&mut wire, 4096, &mut decoder)
                .unwrap();
            let mut fields = next.fields().iter();
            let field = fields
                .next()
                .expect("dynamic field must survive malformed block");
            let (name, value) = field.clone().into_inner();
            assert_eq!(name.as_ref(), b"x-good");
            assert_eq!(value.as_ref(), b"valid");
            assert!(fields.next().is_none());
        }
    }

    #[test]
    fn test_nameless_header_at_resume() {
        let mut encoder = Encoder::default();
        let mut first = BytesMut::new();
        let mut headers = HeaderMap::new();
        for (value, sensitive) in
            [("world", false), ("zomg", true), ("sup", false)]
        {
            let mut header = Header::new(
                Bytes::from_static(b"hello"),
                Bytes::copy_from_slice(value.as_bytes()),
            );
            header.set_sensitive(sensitive);
            headers.extend([header]);
        }
        let continuation =
            Headers::new(StreamId::from(1), Pseudo::default(), headers)
                .encode(
                    &mut encoder,
                    &mut (&mut first).limit(frame::HEADER_LEN + 8),
                )
                .unwrap();
        assert_eq!(first[3], 1);
        assert_eq!(first[4] & 4, 0);
        let mut second = BytesMut::new();
        assert!(
            continuation
                .encode(&mut (&mut second).limit(1024))
                .is_none()
        );
        assert_eq!(second[3], 9);
        assert_eq!(second[4] & 4, 4);
        let mut wire = BytesMut::from(&first[frame::HEADER_LEN..]);
        wire.extend_from_slice(&second[frame::HEADER_LEN..]);
        let mut decoded = Vec::new();
        hpack::Decoder::default()
            .decode(&mut Cursor::new(&mut wire), |h| {
                assert_eq!(h.name().as_slice(), b"hello");
                decoded.push((h.value_slice().to_vec(), h.is_sensitive()));
            })
            .unwrap();
        assert_eq!(
            decoded,
            vec![
                (b"world".to_vec(), false),
                (b"zomg".to_vec(), true),
                (b"sup".to_vec(), false)
            ]
        );
    }

    #[test]
    fn test_connect_request_pseudo_headers_omits_path_and_scheme() {
        // CONNECT requests MUST NOT include :scheme & :path pseudo-header fields
        // See: https://datatracker.ietf.org/doc/html/rfc9113#section-8.5

        assert_eq!(
            Pseudo::request(
                Method::CONNECT,
                Uri::builder()
                    .authority("example.com:8443")
                    .build()
                    .unwrap(),
                None
            ),
            Pseudo {
                method: Method::CONNECT.into(),
                authority: BytesStr::from_static("example.com:8443").into(),
                ..Default::default()
            }
        );

        assert_eq!(
            Pseudo::request(
                Method::CONNECT,
                Uri::builder()
                    .authority("example.com")
                    .build()
                    .unwrap(),
                None
            ),
            Pseudo {
                method: Method::CONNECT.into(),
                authority: BytesStr::from_static("example.com").into(),
                ..Default::default()
            }
        );

        assert_eq!(
            Pseudo::request(
                Method::CONNECT,
                Uri::builder()
                    .authority("example.com:8443")
                    .build()
                    .unwrap(),
                None
            ),
            Pseudo {
                method: Method::CONNECT.into(),
                authority: BytesStr::from_static("example.com:8443").into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_extended_connect_request_pseudo_headers_includes_path_and_scheme()
    {
        // On requests that contain the :protocol pseudo-header field, the
        // :scheme and :path pseudo-header fields of the target URI (see
        // Section 5) MUST also be included.
        // See: https://datatracker.ietf.org/doc/html/rfc8441#section-4

        assert_eq!(
            Pseudo::request(
                Method::CONNECT,
                Uri::builder()
                    .authority("example.com:8443")
                    .scheme(Scheme::HTTPS)
                    .build()
                    .unwrap(),
                Protocol::from_static("the-bread-protocol").into()
            ),
            Pseudo {
                method: Method::CONNECT.into(),
                authority: BytesStr::from_static("example.com:8443").into(),
                scheme: BytesStr::from_static("https").into(),
                path: BytesStr::from_static("/").into(),
                protocol: Protocol::from_static("the-bread-protocol").into(),
                ..Default::default()
            }
        );

        assert_eq!(
            Pseudo::request(
                Method::CONNECT,
                Uri::builder()
                    .authority("example.com:8443")
                    .scheme(Scheme::HTTPS)
                    .path("/test")
                    .build()
                    .unwrap(),
                Protocol::from_static("the-bread-protocol").into()
            ),
            Pseudo {
                method: Method::CONNECT.into(),
                authority: BytesStr::from_static("example.com:8443").into(),
                scheme: BytesStr::from_static("https").into(),
                path: BytesStr::from_static("/test").into(),
                protocol: Protocol::from_static("the-bread-protocol").into(),
                ..Default::default()
            }
        );

        assert_eq!(
            Pseudo::request(
                Method::CONNECT,
                Uri::builder()
                    .authority("example.com")
                    .scheme(Scheme::HTTP)
                    .path("/a/b/c")
                    .build()
                    .unwrap(),
                Protocol::from_static("the-bread-protocol").into()
            ),
            Pseudo {
                method: Method::CONNECT.into(),
                authority: BytesStr::from_static("example.com").into(),
                scheme: BytesStr::from_static("http").into(),
                path: BytesStr::from_static("/a/b/c").into(),
                protocol: Protocol::from_static("the-bread-protocol").into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_options_request_with_empty_path_has_asterisk_as_pseudo_path() {
        // an OPTIONS request for an "http" or "https" URI that does not include a path component;
        // these MUST include a ":path" pseudo-header field with a value of '*' (see Section 7.1 of [HTTP]).
        // See: https://datatracker.ietf.org/doc/html/rfc9113#section-8.3.1

        let _a = Uri::builder()
            .authority("example.com:8080")
            .build()
            .unwrap();
        assert_eq!(
            Pseudo::request(
                Method::OPTIONS,
                Uri::builder()
                    .authority("example.com:8080")
                    .build()
                    .unwrap(),
                None,
            ),
            Pseudo {
                method: Method::OPTIONS.into(),
                authority: BytesStr::from_static("example.com:8080").into(),
                path: BytesStr::from_static("*").into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_non_option_and_non_connect_requests_include_path_and_scheme() {
        let methods = [
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::HEAD,
            Method::PATCH,
            Method::TRACE,
        ];

        for method in methods {
            assert_eq!(
                Pseudo::request(
                    method.clone(),
                    Uri::builder()
                        .authority("example.com:8080")
                        .scheme(Scheme::HTTP)
                        .build()
                        .unwrap(),
                    None,
                ),
                Pseudo {
                    method: method.clone().into(),
                    authority: BytesStr::from_static("example.com:8080")
                        .into(),
                    scheme: BytesStr::from_static("http").into(),
                    path: BytesStr::from_static("/").into(),
                    ..Default::default()
                }
            );
            assert_eq!(
                Pseudo::request(
                    method.clone(),
                    Uri::builder()
                        .authority("example.com")
                        .scheme(Scheme::HTTPS)
                        .path("/a/b/c")
                        .build()
                        .unwrap(),
                    None,
                ),
                Pseudo {
                    method: method.into(),
                    authority: BytesStr::from_static("example.com").into(),
                    scheme: BytesStr::from_static("https").into(),
                    path: BytesStr::from_static("/a/b/c").into(),
                    ..Default::default()
                }
            );
        }
    }
}
