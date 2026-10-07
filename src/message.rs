use crate::ext::Protocol;
use crate::frame::Reason;
use crate::hpack::header;
use crate::{
    frame::{self, StreamId, headers::Pseudo},
    proto::ProtoError,
};
use header_plz::uri::Uri;
use header_plz::{
    HeaderMap, Method, RequestLine, ResponseLine, uri::scheme::Scheme,
};
use http_plz::{Message, Request, Response};

/// A received body chunk or the trailers that terminate a body.
#[derive(Debug)]
pub enum BodyFrame {
    Data(bytes::Bytes),
    Trailers(HeaderMap),
}

/// An incremental receive body. Keep driving the connection while reading it.
/// Flow-control capacity is returned when a frame is yielded, so callers should
/// process each chunk before requesting the next rather than accumulate chunks.
#[derive(Debug)]
pub struct RecvBody {
    pub(crate) inner: crate::proto::streams::OpaqueStreamRef,
    pub(crate) done: bool,
}

impl RecvBody {
    pub fn stream_id(&self) -> StreamId {
        self.inner.stream_id()
    }

    pub async fn frame(
        &mut self,
    ) -> Option<Result<BodyFrame, crate::error::OpError>> {
        futures::future::poll_fn(|cx| self.poll_frame(cx)).await
    }

    pub fn poll_frame(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<BodyFrame, crate::error::OpError>>>
    {
        if self.done {
            return std::task::Poll::Ready(None);
        }
        let result = self.inner.poll_body(cx);
        if matches!(result, std::task::Poll::Ready(None | Some(Err(_)))) {
            self.done = true;
        }
        result
    }
}

impl Drop for RecvBody {
    fn drop(&mut self) {
        if !self.done {
            self.inner.abandon_body();
        }
    }
}

#[cfg(feature = "stream")]
impl futures_core::Stream for RecvBody {
    type Item = Result<BodyFrame, crate::error::OpError>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_frame(cx)
    }
}

/// An incremental send body. Writes wait for bounded queue capacity. Keep
/// driving the connection while writing; completion means queued, not flushed.
#[derive(Debug)]
pub struct SendBody {
    pub(crate) inner: crate::proto::streams::StreamRef,
}

impl SendBody {
    pub fn stream_id(&self) -> StreamId {
        self.inner.stream_id()
    }

    /// Sends a chunk, optionally ending the stream. If canceled while pending,
    /// a prefix may already have been queued; use `poll_send_data` to retain the
    /// remaining bytes across cancellation.
    pub async fn send_data(
        &mut self,
        mut data: bytes::Bytes,
        end_stream: bool,
    ) -> Result<(), crate::error::OpError> {
        futures::future::poll_fn(|cx| {
            self.poll_send_data(cx, &mut data, end_stream)
        })
        .await
    }

    /// On Pending, `data` contains only the bytes not yet queued.
    pub fn poll_send_data(
        &mut self,
        cx: &mut std::task::Context<'_>,
        data: &mut bytes::Bytes,
        end_stream: bool,
    ) -> std::task::Poll<Result<(), crate::error::OpError>> {
        self.inner
            .poll_send_data(cx, data, end_stream)
            .map_err(Into::into)
    }

    pub fn send_trailers(
        &mut self,
        trailers: HeaderMap,
    ) -> Result<(), crate::error::OpError> {
        self.inner
            .send_trailers(trailers)
            .map_err(Into::into)
    }

    pub fn send_reset(&mut self, reason: Reason) {
        self.inner.send_reset(reason);
    }
}

impl Drop for SendBody {
    fn drop(&mut self) {
        self.inner.abandon_send();
    }
}

pub trait IntoPseudo {
    fn into_pseudo(self) -> Pseudo;
}

impl From<header::BytesStr> for header_plz::bytes_str::BytesStr {
    fn from(value: header::BytesStr) -> Self {
        header_plz::bytes_str::BytesStr::from(value.into_inner())
    }
}

impl IntoPseudo for RequestLine {
    fn into_pseudo(self) -> Pseudo {
        let (method, uri, ext) = self.into_parts();
        let is_connect = method == Method::CONNECT;
        let protocol = ext.and_then(|e| Protocol::try_from(*e).ok());
        let mut pseudo = Pseudo::request(method, uri, protocol);

        if pseudo.scheme.is_none() && !is_connect {
            pseudo.set_scheme(Scheme::HTTP)
        }

        pseudo
    }
}

impl IntoPseudo for ResponseLine {
    fn into_pseudo(self) -> Pseudo {
        Pseudo::response(self.into_parts())
    }
}

pub struct TwoTwoFrame {
    pub(crate) header: frame::Headers,
    data: Option<frame::Data>,
    trailer: Option<frame::Headers>,
}

impl TwoTwoFrame {
    pub fn take_data(&mut self) -> Option<frame::Data> {
        self.data.take()
    }

    pub fn take_trailer(&mut self) -> Option<frame::Headers> {
        self.trailer.take()
    }
}

impl<T> From<(StreamId, Message<T>)> for TwoTwoFrame
where
    T: IntoPseudo,
{
    fn from((stream_id, mut message): (StreamId, Message<T>)) -> Self {
        let body = message.take_body();
        let trailer = message.take_trailers();
        let (info_line, headers) = message.into_message_head();
        let pseudo = info_line.into_pseudo();
        let mut header = frame::Headers::new(stream_id, pseudo, headers);
        if body.is_none() && trailer.is_none() {
            header.set_end_stream();
            return TwoTwoFrame {
                header,
                data: None,
                trailer: None,
            };
        }
        let data = body.map(|b| {
            let mut frame = frame::Data::new(stream_id, b.freeze());
            if trailer.is_none() {
                frame.set_end_stream(true);
            }
            frame
        });

        let trailer = trailer.map(|t| frame::Headers::trailers(stream_id, t));

        TwoTwoFrame {
            header,
            data,
            trailer,
        }
    }
}

pub(crate) fn frames_to_request(
    pseudo: Pseudo,
    headers: HeaderMap,
    stream_id: StreamId,
) -> Result<Request, ProtoError> {
    // macro to return error
    macro_rules! malformed {
            ($($arg:tt)*) => {{
                tracing::debug!($($arg)*);
                return Err(ProtoError::library_reset(stream_id, Reason::PROTOCOL_ERROR));
            }}
        }

    // check status code in request
    if pseudo.status.is_some() {
        malformed!("malformed headers| :status field on request");
    }

    let mut b = Request::builder();

    // method check
    let is_connect;
    if let Some(method) = pseudo.method {
        is_connect = method == Method::CONNECT;
        b = b.method(method);
    } else {
        malformed!("malformed headers| missing method");
    }

    // add protocol for CONNECT requests
    let has_protocol = pseudo.protocol.is_some();
    if has_protocol {
        if is_connect {
            b = b.extension(pseudo.protocol.unwrap().into_bytes());
        } else {
            malformed!("malformed headers| :protocol on non-CONNECT request");
        }
    }

    // Uri
    let mut uri_b = Uri::builder();

    // authority
    let mut has_authority = false;
    if let Some(authority) = pseudo.authority {
        has_authority = true;
        uri_b = uri_b.authority(authority);
    }

    // A :scheme is required, except CONNECT.
    if let Some(scheme) = pseudo.scheme {
        if is_connect && !has_protocol {
            malformed!("malformed headers| :scheme in CONNECT");
        }
        let scheme = match Scheme::try_from(scheme.as_str()) {
            Ok(scheme) => scheme,
            Err(_) => malformed!("malformed headers| invalid scheme"),
        };

        // It's not possible to build an `Uri` from a scheme and path. So,
        // after validating is was a valid scheme, we just have to drop it
        // if there isn't an :authority.
        if has_authority {
            uri_b = uri_b.scheme(scheme);
        }
    } else if !is_connect || has_protocol {
        malformed!("malformed headers| missing scheme");
    }

    // path
    if let Some(path) = pseudo.path {
        if is_connect && !has_protocol {
            malformed!("malformed headers| :path in CONNECT");
        }

        // This cannot be empty
        if path.is_empty() {
            malformed!("malformed headers| missing path");
        }
        uri_b = uri_b.path(path.as_str());
    } else if !is_connect || has_protocol {
        malformed!("malformed headers| missing path");
    }

    let uri = match uri_b.build() {
        Ok(uri) => uri,
        Err(_) => malformed!("malformed headers| invalid URI"),
    };
    b = b.uri(uri);
    b = b.headers(headers);

    Ok(b.build())
}

pub(crate) fn frames_to_response(
    pseudo: Pseudo,
    headers: HeaderMap,
    _stream_id: StreamId,
) -> Result<Response, ProtoError> {
    let mut b = Response::builder();
    if let Some(status) = pseudo.status {
        b = b.status(status.into());
    }
    b = b.headers(headers);
    // safe to unwrap, status code error already checked in previous step
    Ok(b.build().expect("invalid scode"))
}
