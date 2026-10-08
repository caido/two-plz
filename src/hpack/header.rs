use crate::ext::Protocol;
use crate::hpack::decoder::DecoderError;

use header_plz::const_headers as header;

use bytes::Bytes;
use header_plz::Method;
use header_plz::status::StatusCode;
use std::fmt;

/// HTTP/2 Header
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Header<T = Bytes> {
    Field {
        name: T,
        value: Bytes,
    },
    // TODO(hyper): Change these types to `http::uri` types.
    Authority(BytesStr),
    Method(Method),
    Scheme(BytesStr),
    Path(BytesStr),
    Protocol(Protocol),
    Status(StatusCode),
    /// Encoding policy, independent from the field bytes.
    Sensitive(Box<Header<T>>),
}

/// The header field name
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Name<'a> {
    Field(&'a Bytes),
    Authority,
    Method,
    Scheme,
    Path,
    Protocol,
    Status,
}

#[doc(hidden)]
#[derive(Clone, Eq, PartialEq, Default, Hash)]
pub struct BytesStr(Bytes);

impl<'a> PartialEq<&'a str> for BytesStr {
    fn eq(&self, other: &&'a str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

pub fn len(name: &Bytes, value: &Bytes) -> usize {
    32 + name.len() + value.len()
}

impl Header<Option<Bytes>> {
    pub fn reify(self) -> Result<Header, (Bytes, bool)> {
        use self::Header::*;

        Ok(match self {
            Field {
                name: Some(n),
                value,
            } => Field {
                name: n,
                value,
            },
            Field {
                name: None,
                value,
            } => return Err((value, false)),
            Sensitive(header) => {
                return match header.reify() {
                    Ok(header) => Ok(header.with_sensitive(true)),
                    Err((value, _)) => Err((value, true)),
                };
            }
            Authority(v) => Authority(v),
            Method(v) => Method(v),
            Scheme(v) => Scheme(v),
            Path(v) => Path(v),
            Protocol(v) => Protocol(v),
            Status(v) => Status(v),
        })
    }
}

impl<T> Header<T> {
    pub fn with_sensitive(self, sensitive: bool) -> Self {
        if sensitive && !self.is_sensitive() {
            Self::Sensitive(Box::new(self))
        } else {
            self
        }
    }

    pub fn is_sensitive(&self) -> bool {
        matches!(self, Self::Sensitive(_))
    }

    pub fn into_unmarked(self) -> Self {
        match self {
            Self::Sensitive(header) => header.into_unmarked(),
            header => header,
        }
    }
}

impl Header {
    pub fn new(name: Bytes, value: Bytes) -> Result<Header, DecoderError> {
        if name.first() == Some(&b':') {
            // Typed HTTP validation is not an HPACK compression error. Keep
            // malformed pseudoheaders as raw fields for table synchronization
            // and reject them when assembling the complete message.
            let raw_value = value.clone();
            let parsed = (|| match &name[1..] {
                b"authority" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Header::Authority(value))
                }
                b"method" => {
                    let method = Method::from(value.as_ref());
                    Ok(Header::Method(method))
                }
                b"scheme" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Header::Scheme(value))
                }
                b"path" => {
                    let value = BytesStr::try_from(value)?;
                    Ok(Header::Path(value))
                }
                b"protocol" => {
                    let value = Protocol::try_from(value)?;
                    Ok(Header::Protocol(value))
                }
                b"status" => {
                    let status = StatusCode::from_bytes(&value)?;
                    Ok(Header::Status(status))
                }
                _ => Err(DecoderError::InvalidPseudoheader),
            })();
            Ok(parsed.unwrap_or_else(|_: DecoderError| Header::Field {
                name,
                value: raw_value,
            }))
        } else {
            // Keep the wire bytes for HPACK table synchronization. Field
            // syntax is validated when assembling the decoded message.
            Ok(Header::Field {
                name,
                value,
            })
        }
    }

    /// Validate regular field syntax after decoding, so malformed messages do
    /// not interrupt updates to the connection's HPACK table.
    pub(crate) fn is_valid_field(&self) -> bool {
        match self {
            Header::Sensitive(header) => header.is_valid_field(),
            Header::Field {
                name,
                value,
            } => !name.is_empty() && name.as_ref().iter().all(|b| {
                matches!(
                    b,
                    b'a'..=b'z' | b'0'..=b'9' | b'!' | b'#' | b'$' | b'%' |
                    b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' |
                    b'`' | b'|' | b'~'
                )
            }) && valid_value(value),
            Header::Method(method) => {
                !method.as_ref().is_empty()
                    && method.as_ref().iter().all(|b| {
                        matches!(b,
                    b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'!' | b'#' |
                    b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' |
                    b'^' | b'_' | b'`' | b'|' | b'~')
                    })
            }
            Header::Authority(value)
            | Header::Scheme(value)
            | Header::Path(value) => valid_value(value.as_ref()),
            Header::Protocol(value) => valid_value(value.as_ref()),
            Header::Status(_) => true,
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            Header::Sensitive(ref header) => header.len(),
            Header::Field {
                ref name,
                ref value,
            } => len(name, value),
            Header::Authority(ref v) => 32 + 10 + v.len(),
            Header::Method(ref v) => 32 + 7 + v.as_ref().len(),
            Header::Scheme(ref v) => 32 + 7 + v.len(),
            Header::Path(ref v) => 32 + 5 + v.len(),
            Header::Protocol(ref v) => 32 + 9 + v.as_str().len(),
            Header::Status(_) => 32 + 7 + 3,
        }
    }

    /// Returns the header name
    pub fn name(&self) -> Name<'_> {
        match *self {
            Header::Sensitive(ref header) => header.name(),
            Header::Field {
                ref name,
                ..
            } => Name::Field(name),
            Header::Authority(..) => Name::Authority,
            Header::Method(..) => Name::Method,
            Header::Scheme(..) => Name::Scheme,
            Header::Path(..) => Name::Path,
            Header::Protocol(..) => Name::Protocol,
            Header::Status(..) => Name::Status,
        }
    }

    pub fn value_slice(&self) -> &[u8] {
        match *self {
            Header::Sensitive(ref header) => header.value_slice(),
            Header::Field {
                ref value,
                ..
            } => value.as_ref(),
            Header::Authority(ref v) => v.as_ref(),
            Header::Method(ref v) => v.as_ref(),
            Header::Scheme(ref v) => v.as_ref(),
            Header::Path(ref v) => v.as_ref(),
            Header::Protocol(ref v) => v.as_ref(),
            Header::Status(ref v) => v.as_str().as_ref(),
        }
    }

    pub fn value_eq(&self, other: &Header) -> bool {
        if let Header::Sensitive(header) = other {
            return self.value_eq(header);
        }
        match *self {
            Header::Sensitive(ref header) => header.value_eq(other),
            Header::Field {
                ref value,
                ..
            } => {
                let a = value;
                match *other {
                    Header::Field {
                        ref value,
                        ..
                    } => a == value,
                    _ => false,
                }
            }
            Header::Authority(ref a) => match *other {
                Header::Authority(ref b) => a == b,
                _ => false,
            },
            Header::Method(ref a) => match *other {
                Header::Method(ref b) => a == b,
                _ => false,
            },
            Header::Scheme(ref a) => match *other {
                Header::Scheme(ref b) => a == b,
                _ => false,
            },
            Header::Path(ref a) => match *other {
                Header::Path(ref b) => a == b,
                _ => false,
            },
            Header::Protocol(ref a) => match *other {
                Header::Protocol(ref b) => a == b,
                _ => false,
            },
            Header::Status(ref a) => match *other {
                Header::Status(ref b) => a == b,
                _ => false,
            },
        }
    }

    pub fn skip_value_index(&self) -> bool {
        match *self {
            Header::Field {
                ref name,
                ..
            } => {
                let slice = name.as_ref();
                matches!(
                    slice,
                    header::CONTENT_LENGTH
                        | header::AGE
                        | header::AUTHORIZATION
                        | header::ETAG
                        | header::IF_MODIFIED_SINCE
                        | header::IF_NONE_MATCH
                        | header::LOCATION
                        | header::COOKIE
                        | header::SET_COOKIE
                )
            }
            Header::Path(..) => true,
            _ => false,
        }
    }
}

// Mostly for tests
impl From<Header> for Header<Option<Bytes>> {
    fn from(src: Header) -> Self {
        match src {
            Header::Sensitive(header) => {
                Header::Sensitive(Box::new((*header).into()))
            }
            Header::Field {
                name,
                value,
            } => Header::Field {
                name: Some(name),
                value,
            },
            Header::Authority(v) => Header::Authority(v),
            Header::Method(v) => Header::Method(v),
            Header::Scheme(v) => Header::Scheme(v),
            Header::Path(v) => Header::Path(v),
            Header::Protocol(v) => Header::Protocol(v),
            Header::Status(v) => Header::Status(v),
        }
    }
}

impl<'a> Name<'a> {
    pub fn into_entry(self, value: Bytes) -> Result<Header, DecoderError> {
        Header::new(Bytes::copy_from_slice(self.as_slice()), value)
    }

    pub fn as_slice(&self) -> &[u8] {
        match *self {
            Name::Field(ref name) => name.as_ref(),
            Name::Authority => b":authority",
            Name::Method => b":method",
            Name::Scheme => b":scheme",
            Name::Path => b":path",
            Name::Protocol => b":protocol",
            Name::Status => b":status",
        }
    }
}

/// HTTP/2 values permit opaque octets, but not controls (except interior HTAB)
/// or leading/trailing SP and HTAB.
fn valid_value(value: &[u8]) -> bool {
    !matches!(value.first(), Some(b' ' | b'\t'))
        && !matches!(value.last(), Some(b' ' | b'\t'))
        && value
            .iter()
            .all(|b| *b >= 0x20 && *b != 0x7f || *b == b'\t')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_field_byte_policy() {
        for value in [b"".as_slice(), b"a\tb", b"a b", b"\x80\xff"] {
            let field = Header::new(
                Bytes::from_static(b"x-test"),
                Bytes::copy_from_slice(value),
            )
            .unwrap();
            assert!(field.is_valid_field(), "{value:?}");
            assert_eq!(field.value_slice(), value);
            let _ = format!("{field:?}");
            assert_eq!(field.len(), 38 + value.len());
        }
        for value in [
            b" a".as_slice(),
            b"a ",
            b"\ta",
            b"a\t",
            b"a\0b",
            b"a\r",
            b"a\n",
            b"a\x01",
            b"a\x7f",
        ] {
            let field = Header::new(
                Bytes::from_static(b"x"),
                Bytes::copy_from_slice(value),
            )
            .unwrap();
            assert!(!field.is_valid_field(), "{value:?}");
        }
        for name in [b"".as_slice(), b"X-test", b"x y", b"x\x80"] {
            let field = Header::new(
                Bytes::copy_from_slice(name),
                Bytes::from_static(b"value"),
            )
            .unwrap();
            assert!(!field.is_valid_field(), "{name:?}");
        }
    }

    #[test]
    fn invalid_status_paths_agree() {
        for value in [b"abc".as_slice(), b"20", b"9999"] {
            let literal = Header::new(
                Bytes::from_static(b":status"),
                Bytes::copy_from_slice(value),
            );
            let indexed =
                Name::Status.into_entry(Bytes::copy_from_slice(value));
            let literal = literal.unwrap();
            let indexed = indexed.unwrap();
            assert_eq!(literal, indexed);
            assert!(!literal.is_valid_field());
            assert_eq!(literal.name().as_slice(), b":status");
            assert_eq!(literal.value_slice(), value);
        }
    }

    #[test]
    fn text_constructors_validate_utf8_and_compare_exactly() {
        assert!(BytesStr::try_from(Bytes::from_static(b"\xff")).is_err());
        assert!(
            std::panic::catch_unwind(|| BytesStr::from(Bytes::from_static(
                b"\xff"
            )))
            .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| BytesStr::unchecked_from_slice(
                b"\xff"
            ))
            .is_err()
        );
        assert_ne!(BytesStr::from_static("GZIP"), "gzip");
    }
}

// ===== impl BytesStr =====

impl BytesStr {
    pub const fn from_static(value: &'static str) -> Self {
        BytesStr(Bytes::from_static(value.as_bytes()))
    }

    pub fn unchecked_from_slice(value: &[u8]) -> Self {
        // Retained for compatibility; this constructor also enforces UTF-8.
        Self::try_from(Bytes::copy_from_slice(value))
            .expect("BytesStr requires UTF-8")
    }

    #[doc(hidden)]
    pub fn try_from(bytes: Bytes) -> Result<Self, std::str::Utf8Error> {
        std::str::from_utf8(bytes.as_ref())?;
        Ok(BytesStr(bytes))
    }

    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(self.0.as_ref()).expect("BytesStr requires UTF-8")
    }

    pub(crate) fn into_inner(self) -> Bytes {
        self.0
    }
}

impl From<&str> for BytesStr {
    fn from(value: &str) -> Self {
        BytesStr(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<Bytes> for BytesStr {
    fn from(value: Bytes) -> Self {
        Self::try_from(value).expect("BytesStr requires UTF-8")
    }
}

impl std::ops::Deref for BytesStr {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for BytesStr {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl fmt::Debug for BytesStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<&[u8]> for BytesStr {
    type Error = std::str::Utf8Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(BytesStr::from(str::from_utf8(value)?))
    }
}
