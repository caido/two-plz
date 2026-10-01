pub use crate::proto::streams::PartialResponse;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    Codec, Connection,
    builder::{BuildConnection, Builder},
    error::OpError,
    frame::StreamId,
    proto::{config::ConnectionConfig, streams::Streams},
    role::Role,
    spa::Mode,
};

use crate::proto::streams::OpaqueStreamRef;
use http_plz::{Request, Response};

#[derive(Default)]
pub struct Client {
    spa_mode: Option<Mode>,
}

// ===== Builder =====
pub type ClientBuilder = Builder<Client>;

impl ClientBuilder {
    pub fn single_packet_attack_mode(mut self, mode: Mode) -> Self {
        self.role.spa_mode = Some(mode);
        self
    }
}

impl BuildConnection for Client {
    type Connection<T> = (ClientConnection<T>, SendRequest);

    fn role_opts() -> Self {
        Client::default()
    }

    fn is_server() -> bool {
        false
    }

    fn is_client() -> bool {
        true
    }

    fn init_stream_id() -> StreamId {
        1.into()
    }

    fn build<T>(
        role: Role,
        config: ConnectionConfig,
        codec: Codec<T, Bytes>,
    ) -> Self::Connection<T>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let is_spa = config.spa_tracker.is_some();
        let conn = ClientConnection {
            inner: Connection::new(role, config, codec),
        };
        let send_request = SendRequest {
            inner: conn.inner.streams.clone(),
            is_spa,
        };
        (conn, send_request)
    }

    fn take_spa_mode(&mut self) -> Option<Mode> {
        self.spa_mode.take()
    }

    fn is_spa(&self) -> bool {
        self.spa_mode.is_some()
    }
}

// client handshake => ClientConnection , SendRequest
// spawn || ClientConnection
// SendRequest.send_request(Request) => RecvResponse
// RecvResponse.recv_response().await => Response
// Response => compelete response

pub struct ClientConnection<T> {
    inner: Connection<T>,
}

impl<T> ClientConnection<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn is_extended_connect_protocol_enabled(&self) -> bool {
        self.inner
            .is_extended_connect_protocol_enabled()
    }

    /// Returns the maximum number of concurrent streams that may be initiated
    /// by this client.
    ///
    /// This limit is configured by the server peer by sending the
    /// [`SETTINGS_MAX_CONCURRENT_STREAMS` parameter][1] in a `SETTINGS` frame.
    /// This method returns the most recently received value. Before the peer's
    /// initial `SETTINGS` frame arrives, it returns the protocol default of no
    /// advertised limit.
    ///
    /// [1]: https://tools.ietf.org/html/rfc7540#section-5.1.2
    pub fn max_concurrent_send_streams(&self) -> usize {
        self.inner.max_send_streams()
    }
}

impl<T> Future for ClientConnection<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), OpError>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        self.inner
            .maybe_close_connection_if_no_streams();
        let had_streams_or_refs = self
            .inner
            .has_streams_or_other_references();
        let result = self.inner.poll(cx).map_err(Into::into);
        // if we had streams/refs, and don't anymore, wake up one more time to
        // ensure proper shutdown
        if result.is_pending()
            && had_streams_or_refs
            && !self
                .inner
                .has_streams_or_other_references()
        {
            cx.waker().wake_by_ref();
        }
        result
    }
}

// ===== Send Request ====
#[derive(Clone)]
pub struct SendRequest {
    inner: Streams<Bytes>,
    is_spa: bool,
}

impl SendRequest {
    pub fn send_request(
        &mut self,
        request: Request,
    ) -> Result<ResponseFuture, OpError> {
        self.inner
            .send_request(request, self.is_spa)
            .map_err(Into::into)
            .map(|s| ResponseFuture {
                inner: s,
            })
    }

    pub fn num_active_streams(&self) -> usize {
        self.inner.num_active_streams()
    }

    pub fn is_spa(&self) -> bool {
        self.is_spa
    }

    pub fn spa(
        &mut self,
        requests: Vec<Request>,
    ) -> Vec<Result<ResponseFuture, OpError>> {
        requests
            .into_iter()
            .map(|r| self.send_request(r))
            .collect()
    }
}

// ===== Response Future =====
#[derive(Debug)]
pub struct ResponseFuture {
    inner: OpaqueStreamRef,
}

impl ResponseFuture {
    pub fn stream_id(&self) -> StreamId {
        self.inner.stream_id()
    }

    pub fn take_partial_response(&mut self) -> Option<Response> {
        self.inner.take_partial_response()
    }
}

impl Future for ResponseFuture {
    type Output = Result<Response, PartialResponse>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        self.inner
            .poll_response(cx)
            .map_err(Into::into)
    }
}

pub async fn poll_once<T, E>(conn: &mut T) -> Result<(), E>
where
    T: Future<Output = Result<(), E>> + Unpin,
{
    futures::future::poll_fn(|cx| match Pin::new(&mut *conn).poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(r) => Poll::Ready(r),
    })
    .await
}
