use super::{Header, header::BytesStr, huffman};
use crate::frame;

use bytes::{Buf, Bytes, BytesMut};
use header_plz::Method;
use header_plz::const_headers as header;
use header_plz::status::{InvalidStatusCode, StatusCode};

use std::collections::VecDeque;
use std::io::Cursor;
use std::str::Utf8Error;

/// Decodes headers using HPACK
#[derive(Debug)]
pub struct Decoder {
    // Protocol indicated that the max table size will update
    max_size_update: Option<usize>,
    // Smallest acknowledged reduction not yet signaled by the encoder.
    required_size_update: Option<usize>,
    last_max_update: usize,
    can_resize: bool,
    table: Table,
    buffer: BytesMut,
}

/// Represents all errors that can be encountered while performing the decoding
/// of an HPACK header set.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DecoderError {
    InvalidRepresentation,
    InvalidIntegerPrefix,
    InvalidTableIndex,
    InvalidHuffmanCode,
    InvalidUtf8,
    InvalidStatusCode,
    InvalidPseudoheader,
    InvalidMaxDynamicSize,
    IntegerOverflow,
    NeedMore(NeedMore),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NeedMore {
    UnexpectedEndOfStream,
    IntegerUnderflow,
    StringUnderflow,
}

enum Representation {
    /// Indexed header field representation
    ///
    /// An indexed header field representation identifies an entry in either the
    /// static table or the dynamic table (see Section 2.3).
    ///
    /// # Header encoding
    ///
    /// ```text
    ///   0   1   2   3   4   5   6   7
    /// +---+---+---+---+---+---+---+---+
    /// | 1 |        Index (7+)         |
    /// +---+---------------------------+
    /// ```
    Indexed,

    /// Literal Header Field with Incremental Indexing
    ///
    /// A literal header field with incremental indexing representation results
    /// in appending a header field to the decoded header list and inserting it
    /// as a new entry into the dynamic table.
    ///
    /// # Header encoding
    ///
    /// ```text
    ///   0   1   2   3   4   5   6   7
    /// +---+---+---+---+---+---+---+---+
    /// | 0 | 1 |      Index (6+)       |
    /// +---+---+-----------------------+
    /// | H |     Value Length (7+)     |
    /// +---+---------------------------+
    /// | Value String (Length octets)  |
    /// +-------------------------------+
    /// ```
    LiteralWithIndexing,

    /// Literal Header Field without Indexing
    ///
    /// A literal header field without indexing representation results in
    /// appending a header field to the decoded header list without altering the
    /// dynamic table.
    ///
    /// # Header encoding
    ///
    /// ```text
    ///   0   1   2   3   4   5   6   7
    /// +---+---+---+---+---+---+---+---+
    /// | 0 | 0 | 0 | 0 |  Index (4+)   |
    /// +---+---+-----------------------+
    /// | H |     Value Length (7+)     |
    /// +---+---------------------------+
    /// | Value String (Length octets)  |
    /// +-------------------------------+
    /// ```
    LiteralWithoutIndexing,

    /// Literal Header Field Never Indexed
    ///
    /// A literal header field never-indexed representation results in appending
    /// a header field to the decoded header list without altering the dynamic
    /// table. Intermediaries MUST use the same representation for encoding this
    /// header field.
    ///
    /// ```text
    ///   0   1   2   3   4   5   6   7
    /// +---+---+---+---+---+---+---+---+
    /// | 0 | 0 | 0 | 1 |  Index (4+)   |
    /// +---+---+-----------------------+
    /// | H |     Value Length (7+)     |
    /// +---+---------------------------+
    /// | Value String (Length octets)  |
    /// +-------------------------------+
    /// ```
    LiteralNeverIndexed,

    /// Dynamic Table Size Update
    ///
    /// A dynamic table size update signals a change to the size of the dynamic
    /// table.
    ///
    /// # Header encoding
    ///
    /// ```text
    ///   0   1   2   3   4   5   6   7
    /// +---+---+---+---+---+---+---+---+
    /// | 0 | 0 | 1 |   Max size (5+)   |
    /// +---+---------------------------+
    /// ```
    SizeUpdate,
}

#[derive(Debug)]
struct Table {
    entries: VecDeque<Header>,
    size: usize,
    max_size: usize,
}

struct StringMarker {
    offset: usize,
    len: usize,
    string: Option<Bytes>,
}

// ===== impl Decoder =====

impl Decoder {
    /// Creates a new `Decoder` with all settings set to default values.
    pub fn new(size: usize) -> Decoder {
        Decoder {
            max_size_update: None,
            required_size_update: None,
            last_max_update: size,
            can_resize: true,
            table: Table::new(size),
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Queues a potential size update
    #[allow(dead_code)]
    pub fn queue_size_update(&mut self, size: usize) {
        // The last acknowledged SETTINGS value is the ceiling (RFC 7541
        // section 6.3). Retain intervening reductions separately so a later
        // increase cannot hide the eviction required by section 4.2.
        if size < self.table.max_size {
            self.required_size_update = Some(
                self.required_size_update
                    .map_or(size, |previous| previous.min(size)),
            );
        }
        self.max_size_update = Some(size);
    }

    /// Starts a header block. CONTINUATION fragments must not restart it.
    pub fn begin_block(&mut self) {
        self.can_resize = true;
        if let Some(size) = self.max_size_update.take() {
            self.last_max_update = size;
        }
    }

    /// Checks requirements that also apply to an empty completed block.
    pub fn end_block(&self) -> Result<(), DecoderError> {
        if self.required_size_update.is_some() {
            Err(DecoderError::InvalidMaxDynamicSize)
        } else {
            Ok(())
        }
    }

    /// Decodes a complete header block, starting fresh block-local state.
    pub fn decode<F>(
        &mut self,
        src: &mut Cursor<&mut BytesMut>,
        f: F,
    ) -> Result<(), DecoderError>
    where
        F: FnMut(Header),
    {
        self.begin_block();
        self.decode_fragment(src, f)?;
        self.end_block()
    }

    /// Decodes a fragment while preserving block-local resize eligibility.
    pub fn decode_fragment<F>(
        &mut self,
        src: &mut Cursor<&mut BytesMut>,
        mut f: F,
    ) -> Result<(), DecoderError>
    where
        F: FnMut(Header),
    {
        use self::Representation::*;

        let span = tracing::trace_span!("hpack::decode");
        let _e = span.enter();

        tracing::trace!("decode");

        while let Some(ty) = peek_u8(src) {
            // At this point we are always at the beginning of the next block
            // within the HPACK data. The type of the block can always be
            // determined from the first byte.
            let representation = Representation::load(ty)?;
            if self.required_size_update.is_some()
                && !matches!(representation, SizeUpdate)
            {
                return Err(DecoderError::InvalidMaxDynamicSize);
            }
            match representation {
                Indexed => {
                    tracing::trace!(rem = src.remaining(), kind = %"Indexed");
                    let entry = self.decode_indexed(src)?;
                    self.can_resize = false;
                    consume(src);
                    f(entry);
                }
                LiteralWithIndexing => {
                    tracing::trace!(rem = src.remaining(), kind = %"LiteralWithIndexing");
                    let entry = self.decode_literal(src, true)?;
                    self.can_resize = false;

                    // Insert the header into the table
                    self.table.insert(entry.clone());
                    consume(src);

                    f(entry);
                }
                LiteralWithoutIndexing => {
                    tracing::trace!(rem = src.remaining(), kind = %"LiteralWithoutIndexing");
                    let entry = self.decode_literal(src, false)?;
                    self.can_resize = false;
                    consume(src);
                    f(entry);
                }
                LiteralNeverIndexed => {
                    tracing::trace!(rem = src.remaining(), kind = %"LiteralNeverIndexed");
                    let entry = self.decode_literal(src, false)?;
                    self.can_resize = false;
                    consume(src);

                    let entry = entry.with_sensitive(true);

                    f(entry);
                }
                SizeUpdate => {
                    tracing::trace!(rem = src.remaining(), kind = %"SizeUpdate");
                    if !self.can_resize {
                        return Err(DecoderError::InvalidMaxDynamicSize);
                    }

                    // Handle the dynamic table size update
                    self.process_size_update(src)?;
                    consume(src);
                }
            }
        }

        Ok(())
    }

    fn process_size_update(
        &mut self,
        buf: &mut Cursor<&mut BytesMut>,
    ) -> Result<(), DecoderError> {
        let new_size = decode_int(buf, 5)?;

        if new_size > self.last_max_update
            || self
                .required_size_update
                .is_some_and(|minimum| new_size > minimum)
        {
            return Err(DecoderError::InvalidMaxDynamicSize);
        }
        self.required_size_update = None;

        tracing::debug!(
            from = self.table.size(),
            to = new_size,
            "Decoder changed max table size"
        );

        self.table.set_max_size(new_size);

        Ok(())
    }

    fn decode_indexed(
        &self,
        buf: &mut Cursor<&mut BytesMut>,
    ) -> Result<Header, DecoderError> {
        let index = decode_int(buf, 7)?;
        self.table.get(index)
    }

    fn decode_literal(
        &mut self,
        buf: &mut Cursor<&mut BytesMut>,
        index: bool,
    ) -> Result<Header, DecoderError> {
        let prefix = if index {
            6
        } else {
            4
        };

        // Extract the table index for the name, or 0 if not indexed
        let table_idx = decode_int(buf, prefix)?;

        // First, read the header name
        if table_idx == 0 {
            let old_pos = buf.position();
            let name_marker = self.try_decode_string(buf)?;
            let value_marker = self.try_decode_string(buf)?;
            buf.set_position(old_pos);
            // Read the name as a literal
            let name = name_marker.consume(buf);
            let value = value_marker.consume(buf);
            Header::new(name, value)
        } else {
            let e = self.table.get(table_idx)?;
            let value = self.decode_string(buf)?;

            e.name().into_entry(value)
        }
    }

    fn try_decode_string(
        &mut self,
        buf: &mut Cursor<&mut BytesMut>,
    ) -> Result<StringMarker, DecoderError> {
        let old_pos = buf.position();
        const HUFF_FLAG: u8 = 0b1000_0000;

        // The first bit in the first byte contains the huffman encoded flag.
        let huff = match peek_u8(buf) {
            Some(hdr) => (hdr & HUFF_FLAG) == HUFF_FLAG,
            None => {
                return Err(DecoderError::NeedMore(
                    NeedMore::UnexpectedEndOfStream,
                ));
            }
        };

        // Decode the string length using 7 bit prefix
        let len = decode_int(buf, 7)?;

        if len > buf.remaining() {
            tracing::trace!(
                len,
                remaining = buf.remaining(),
                "decode_string underflow",
            );
            return Err(DecoderError::NeedMore(NeedMore::StringUnderflow));
        }

        let offset = (buf.position() - old_pos) as usize;
        if huff {
            let ret = {
                let raw = &buf.chunk()[..len];
                huffman::decode(raw, &mut self.buffer).map(|buf| {
                    StringMarker {
                        offset,
                        len,
                        string: Some(BytesMut::freeze(buf)),
                    }
                })
            };

            buf.advance(len);
            ret
        } else {
            buf.advance(len);
            Ok(StringMarker {
                offset,
                len,
                string: None,
            })
        }
    }

    fn decode_string(
        &mut self,
        buf: &mut Cursor<&mut BytesMut>,
    ) -> Result<Bytes, DecoderError> {
        let old_pos = buf.position();
        let marker = self.try_decode_string(buf)?;
        buf.set_position(old_pos);
        Ok(marker.consume(buf))
    }
}

impl Default for Decoder {
    fn default() -> Decoder {
        Decoder::new(4096)
    }
}

// ===== impl Representation =====

impl Representation {
    pub fn load(byte: u8) -> Result<Representation, DecoderError> {
        const INDEXED: u8 = 0b1000_0000;
        const LITERAL_WITH_INDEXING: u8 = 0b0100_0000;
        const LITERAL_WITHOUT_INDEXING: u8 = 0b1111_0000;
        const LITERAL_NEVER_INDEXED: u8 = 0b0001_0000;
        const SIZE_UPDATE_MASK: u8 = 0b1110_0000;
        const SIZE_UPDATE: u8 = 0b0010_0000;

        // RFC 7541 section 6 prefixes: 1, 01, 0000, 0001, and 001.
        // Test the leading tag bits; remaining bits encode the integer.

        if byte & INDEXED == INDEXED {
            Ok(Representation::Indexed)
        } else if byte & LITERAL_WITH_INDEXING == LITERAL_WITH_INDEXING {
            Ok(Representation::LiteralWithIndexing)
        } else if byte & LITERAL_WITHOUT_INDEXING == 0 {
            Ok(Representation::LiteralWithoutIndexing)
        } else if byte & LITERAL_WITHOUT_INDEXING == LITERAL_NEVER_INDEXED {
            Ok(Representation::LiteralNeverIndexed)
        } else if byte & SIZE_UPDATE_MASK == SIZE_UPDATE {
            Ok(Representation::SizeUpdate)
        } else {
            Err(DecoderError::InvalidRepresentation)
        }
    }
}

fn decode_int<B: Buf>(
    buf: &mut B,
    prefix_size: u8,
) -> Result<usize, DecoderError> {
    // The octet limit is chosen such that the maximum allowed *value* can
    // never overflow an unsigned 32-bit integer. The maximum value of any
    // integer that can be encoded with 5 octets is ~2^28
    const MAX_BYTES: usize = 5;
    const VARINT_MASK: u8 = 0b0111_1111;
    const VARINT_FLAG: u8 = 0b1000_0000;

    if !(1..=8).contains(&prefix_size) {
        return Err(DecoderError::InvalidIntegerPrefix);
    }

    if !buf.has_remaining() {
        return Err(DecoderError::NeedMore(NeedMore::IntegerUnderflow));
    }

    let mask = if prefix_size == 8 {
        0xFF
    } else {
        (1u8 << prefix_size).wrapping_sub(1)
    };

    let mut ret = (buf.get_u8() & mask) as usize;

    if ret < mask as usize {
        // Value fits in the prefix bits
        return Ok(ret);
    }

    // The int did not fit in the prefix bits, so continue reading.
    //
    // The total number of bytes used to represent the int. The first byte was
    // the prefix, so start at 1.
    let mut bytes = 1;

    // The rest of the int is stored as a varint -- 7 bits for the value and 1
    // bit to indicate if it is the last byte.
    let mut shift = 0;

    while buf.has_remaining() {
        let b = buf.get_u8();

        bytes += 1;
        ret += ((b & VARINT_MASK) as usize) << shift;
        shift += 7;

        if b & VARINT_FLAG == 0 {
            return Ok(ret);
        }

        if bytes == MAX_BYTES {
            // The spec requires that this situation is an error
            return Err(DecoderError::IntegerOverflow);
        }
    }

    Err(DecoderError::NeedMore(NeedMore::IntegerUnderflow))
}

fn peek_u8<B: Buf>(buf: &B) -> Option<u8> {
    if buf.has_remaining() {
        Some(buf.chunk()[0])
    } else {
        None
    }
}

fn take(buf: &mut Cursor<&mut BytesMut>, n: usize) -> Bytes {
    let pos = buf.position() as usize;
    let mut head = buf.get_mut().split_to(pos + n);
    buf.set_position(0);
    head.advance(pos);
    head.freeze()
}

impl StringMarker {
    fn consume(self, buf: &mut Cursor<&mut BytesMut>) -> Bytes {
        buf.advance(self.offset);
        match self.string {
            Some(string) => {
                buf.advance(self.len);
                string
            }
            None => take(buf, self.len),
        }
    }
}

fn consume(buf: &mut Cursor<&mut BytesMut>) {
    // remove bytes from the internal BytesMut when they have been successfully
    // decoded. This is a more permanent cursor position, which will be
    // used to resume if decoding was only partial.
    take(buf, 0);
}

// ===== impl Table =====

impl Table {
    fn new(max_size: usize) -> Table {
        Table {
            entries: VecDeque::new(),
            size: 0,
            max_size,
        }
    }

    fn size(&self) -> usize {
        self.size
    }

    /// Returns the entry located at the given index.
    ///
    /// The table is 1-indexed and constructed in such a way that the first
    /// entries belong to the static table, followed by entries in the dynamic
    /// table. They are merged into a single index address space, though.
    ///
    /// This is according to the [HPACK spec, section 2.3.3.]
    /// (http://http2.github.io/http2-spec/compression.html#index.address.space)
    pub fn get(&self, index: usize) -> Result<Header, DecoderError> {
        if index == 0 {
            return Err(DecoderError::InvalidTableIndex);
        }

        if index <= 61 {
            return Ok(get_static(index));
        }

        // Convert the index for lookup in the entries structure.
        match self.entries.get(index - 62) {
            Some(e) => Ok(e.clone()),
            None => Err(DecoderError::InvalidTableIndex),
        }
    }

    fn insert(&mut self, entry: Header) {
        let len = entry.len();

        self.reserve(len);

        if self.size + len <= self.max_size {
            self.size += len;

            // Track the entry
            self.entries.push_front(entry);
        }
    }

    fn set_max_size(&mut self, size: usize) {
        self.max_size = size;
        // Make the table size fit within the new constraints.
        self.consolidate();
    }

    fn reserve(&mut self, size: usize) {
        while self.size + size > self.max_size {
            match self.entries.pop_back() {
                Some(last) => {
                    self.size -= last.len();
                }
                None => return,
            }
        }
    }

    fn consolidate(&mut self) {
        while self.size > self.max_size {
            {
                let last = match self.entries.back() {
                    Some(x) => x,
                    None => {
                        // Can never happen as the size of the table must reach
                        // 0 by the time we've exhausted all elements.
                        panic!("Size of table != 0, but no headers left!");
                    }
                };

                self.size -= last.len();
            }

            self.entries.pop_back();
        }
    }
}

// ===== impl NDecoderError =====

impl From<Utf8Error> for DecoderError {
    fn from(_: Utf8Error) -> DecoderError {
        // Text-only header components must be valid UTF-8; ordinary field
        // values remain opaque bytes and do not use this conversion.
        DecoderError::InvalidUtf8
    }
}

impl From<InvalidStatusCode> for DecoderError {
    fn from(_: InvalidStatusCode) -> DecoderError {
        DecoderError::InvalidStatusCode
    }
}

impl From<DecoderError> for frame::Error {
    fn from(src: DecoderError) -> Self {
        frame::Error::Hpack(src)
    }
}

/// Get an entry from the static table
pub fn get_static(idx: usize) -> Header {
    match idx {
        1 => Header::Authority(BytesStr::from_static("")),
        2 => Header::Method(Method::GET),
        3 => Header::Method(Method::POST),
        4 => Header::Path(BytesStr::from_static("/")),
        5 => Header::Path(BytesStr::from_static("/index.html")),
        6 => Header::Scheme(BytesStr::from_static("http")),
        7 => Header::Scheme(BytesStr::from_static("https")),
        8 => Header::Status(StatusCode::OK),
        9 => Header::Status(StatusCode::NO_CONTENT),
        10 => Header::Status(StatusCode::PARTIAL_CONTENT),
        11 => Header::Status(StatusCode::NOT_MODIFIED),
        12 => Header::Status(StatusCode::BAD_REQUEST),
        13 => Header::Status(StatusCode::NOT_FOUND),
        14 => Header::Status(StatusCode::INTERNAL_SERVER_ERROR),
        _ => {
            let (name, value) = match idx {
                15 => (header::ACCEPT_CHARSET, ""),
                16 => (header::ACCEPT_ENCODING, "gzip, deflate"),
                17 => (header::ACCEPT_LANGUAGE, ""),
                18 => (header::ACCEPT_RANGES, ""),
                19 => (header::ACCEPT, ""),
                20 => (header::ACCESS_CONTROL_ALLOW_ORIGIN, ""),
                21 => (header::AGE, ""),
                22 => (header::ALLOW, ""),
                23 => (header::AUTHORIZATION, ""),
                24 => (header::CACHE_CONTROL, ""),
                25 => (header::CONTENT_DISPOSITION, ""),
                26 => (header::CONTENT_ENCODING, ""),
                27 => (header::CONTENT_LANGUAGE, ""),
                28 => (header::CONTENT_LENGTH, ""),
                29 => (header::CONTENT_LOCATION, ""),
                30 => (header::CONTENT_RANGE, ""),
                31 => (header::CONTENT_TYPE, ""),
                32 => (header::COOKIE, ""),
                33 => (header::DATE, ""),
                34 => (header::ETAG, ""),
                35 => (header::EXPECT, ""),
                36 => (header::EXPIRES, ""),
                37 => (header::FROM, ""),
                38 => (header::HOST, ""),
                39 => (header::IF_MATCH, ""),
                40 => (header::IF_MODIFIED_SINCE, ""),
                41 => (header::IF_NONE_MATCH, ""),
                42 => (header::IF_RANGE, ""),
                43 => (header::IF_UNMODIFIED_SINCE, ""),
                44 => (header::LAST_MODIFIED, ""),
                45 => (header::LINK, ""),
                46 => (header::LOCATION, ""),
                47 => (header::MAX_FORWARDS, ""),
                48 => (header::PROXY_AUTHENTICATE, ""),
                49 => (header::PROXY_AUTHORIZATION, ""),
                50 => (header::RANGE, ""),
                51 => (header::REFERER, ""),
                52 => (header::REFRESH, ""),
                53 => (header::RETRY_AFTER, ""),
                54 => (header::SERVER, ""),
                55 => (header::SET_COOKIE, ""),
                56 => (header::STRICT_TRANSPORT_SECURITY, ""),
                57 => (header::TRANSFER_ENCODING, ""),
                58 => (header::USER_AGENT, ""),
                59 => (header::VARY, ""),
                60 => (header::VIA, ""),
                61 => (header::WWW_AUTHENTICATE, ""),
                _ => unreachable!(),
            };

            Header::Field {
                name: Bytes::copy_from_slice(name),
                value: Bytes::from_static(value.as_bytes()),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn huff_encode(src: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        huffman::encode(src, &mut buf);
        buf
    }

    #[test]
    fn test_peek_u8() {
        let b = 0xff;
        let mut buf = Cursor::new(vec![b]);
        assert_eq!(peek_u8(&buf), Some(b));
        assert_eq!(buf.get_u8(), b);
        assert_eq!(peek_u8(&buf), None);
    }

    #[test]
    fn test_decode_string_empty() {
        let mut de = Decoder::new(0);
        let mut buf = BytesMut::new();
        let err = de
            .decode_string(&mut Cursor::new(&mut buf))
            .unwrap_err();
        assert_eq!(
            err,
            DecoderError::NeedMore(NeedMore::UnexpectedEndOfStream)
        );
    }

    #[test]
    fn test_decode_empty() {
        let mut de = Decoder::new(0);
        let mut buf = BytesMut::new();
        let _: () = de
            .decode(&mut Cursor::new(&mut buf), |_| {})
            .unwrap();
    }

    #[test]
    fn test_decode_indexed_larger_than_table() {
        let mut de = Decoder::new(0);

        let mut buf = BytesMut::new();
        buf.extend([0b01000000, 0x80 | 2]);
        buf.extend(huff_encode(b"foo"));
        buf.extend([0x80 | 3]);
        buf.extend(huff_encode(b"bar"));

        let mut res = vec![];
        de.decode(&mut Cursor::new(&mut buf), |h| {
            res.push(h);
        })
        .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(de.table.size(), 0);

        match res[0] {
            Header::Field {
                ref name,
                ref value,
            } => {
                assert_eq!(*name, "foo");
                assert_eq!(*value, "bar");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_decode_continuation_header_with_non_huff_encoded_name() {
        let mut de = Decoder::new(0);
        let value = huff_encode(b"bar");
        let mut buf = BytesMut::new();
        // header name is non_huff encoded
        buf.extend([0b01000000, 3]);
        buf.extend(b"foo");
        // header value is partial
        buf.extend([0x80 | 3]);
        buf.extend(&value[0..1]);

        let mut res = vec![];
        let e = de
            .decode(&mut Cursor::new(&mut buf), |h| {
                res.push(h);
            })
            .unwrap_err();
        // decode error because the header value is partial
        assert_eq!(e, DecoderError::NeedMore(NeedMore::StringUnderflow));

        // extend buf with the remaining header value
        buf.extend(&value[1..]);
        de.decode(&mut Cursor::new(&mut buf), |h| {
            res.push(h);
        })
        .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(de.table.size(), 0);

        match res[0] {
            Header::Field {
                ref name,
                ref value,
            } => {
                assert_eq!(*name, "foo");
                assert_eq!(*value, "bar");
            }
            _ => panic!(),
        }
    }
}
