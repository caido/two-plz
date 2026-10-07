use crate::codec::UserError;
use crate::error::{OpError, Reason};
use crate::spa::Mode;
use crate::{
    Codec, Connection,
    builder::{BuildConnection, Builder},
    frame::StreamId,
    proto::{config::ConnectionConfig, streams::StreamRef},
    role::Role,
};
use bytes::Bytes;
use futures::future::poll_fn;
use http_plz::{Request, Response};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

// ===== Builder =====
pub struct Server;

pub type ServerBuilder = Builder<Server>;

impl BuildConnection for Server {
    type Connection<T> = ServerConnection<T>;

    fn role_opts() -> Self {
        Server
    }

    fn is_server() -> bool {
        true
    }

    fn is_client() -> bool {
        false
    }

    fn init_stream_id() -> StreamId {
        2.into()
    }

    fn build<T>(
        role: Role,
        config: ConnectionConfig,
        codec: Codec<T, Bytes>,
    ) -> Self::Connection<T>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        ServerConnection {
            connection: Connection::new(role, config, codec),
            streaming_accept: None,
        }
    }

    fn take_spa_mode(&mut self) -> Option<Mode> {
        None
    }

    fn is_spa(&self) -> bool {
        false
    }
}

// server handshake => ServerConnection
// ServerConnection.accept() => Request, SendResponse
// Request => complete request
// SendResponse.send_response(Response)

pub struct ServerConnection<T> {
    connection: Connection<T>,
    streaming_accept: Option<bool>,
}

impl<T> ServerConnection<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Accepts request headers without waiting for the complete body.
    /// Use streaming acceptance from the first poll of this connection and do
    /// not mix it with buffered `accept`/`poll_accept` calls.
    pub async fn accept_streaming(
        &mut self,
    ) -> Option<
        Result<(Request, crate::message::RecvBody, SendResponse), OpError>,
    > {
        poll_fn(|cx| self.poll_accept_streaming(cx)).await
    }

    pub fn poll_accept_streaming(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        Option<
            Result<(Request, crate::message::RecvBody, SendResponse), OpError>,
        >,
    > {
        if self.streaming_accept == Some(false) {
            return Poll::Ready(Some(Err(UserError::Rejected.into())));
        }
        self.streaming_accept = Some(true);
        self.connection
            .streams
            .set_streaming_accept();
        if self.connection.poll(cx)?.is_ready() {
            return Poll::Ready(None);
        }
        if let Some(inner) = self.connection.next_accept() {
            let request = inner.take_request();
            let body = crate::message::RecvBody {
                inner: inner.opaque.clone(),
                done: false,
            };
            return Poll::Ready(Some(Ok((
                request,
                body,
                SendResponse {
                    inner,
                },
            ))));
        }
        Poll::Pending
    }

    pub async fn accept(
        &mut self,
    ) -> Option<Result<(Request, SendResponse), OpError>> {
        poll_fn(move |cx| self.poll_accept(cx)).await
    }

    pub fn poll_accept(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(Request, SendResponse), OpError>>> {
        if self.streaming_accept == Some(true) {
            return Poll::Ready(Some(Err(UserError::Rejected.into())));
        }
        self.streaming_accept = Some(false);
        if self.connection.poll(cx)?.is_ready() {
            // If the socket is closed, don't return anything
            // TODO: drop any pending streams
            return Poll::Ready(None);
        }

        if let Some(inner) = self.connection.next_accept() {
            tracing::trace!("accepted");
            let request = inner.take_request();
            let respond = SendResponse {
                inner,
            };
            return Poll::Ready(Some(Ok((request, respond))));
        }
        Poll::Pending
    }

    /// Returns `Ready` when the underlying connection has closed.
    ///
    /// If any new inbound streams are received during a call to `poll_closed`,
    /// they will be queued and returned on the next call to [`poll_accept`].
    ///
    /// This function will advance the internal connection state, driving
    /// progress on all the other handles (e.g. [`RecvStream`] and [`SendStream`]).
    ///
    /// See [here](index.html#managing-the-connection) for more details.
    ///
    /// [`poll_accept`]: struct.Connection.html#method.poll_accept
    /// [`RecvStream`]: ../struct.RecvStream.html
    /// [`SendStream`]: ../struct.SendStream.html
    pub fn poll_closed(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Result<(), OpError>> {
        if self.streaming_accept.is_none() {
            self.streaming_accept = Some(false);
        }
        self.connection
            .poll(cx)
            .map_err(Into::into)
    }

    pub fn num_wired_streams(&self) -> usize {
        self.connection.num_wired_streams()
    }

    /// Sets the connection to a GOAWAY state.
    ///
    /// Does not terminate the connection. Must continue being polled to close
    /// connection.
    ///
    /// After flushing the GOAWAY frame, the connection is closed. Any
    /// outstanding streams do not prevent the connection from closing. This
    /// should usually be reserved for shutting down when something bad
    /// external to `h2` has happened, and open streams cannot be properly
    /// handled.
    ///
    /// For graceful shutdowns, see [`graceful_shutdown`](Connection::graceful_shutdown).
    pub fn abrupt_shutdown(&mut self, reason: Reason) {
        self.connection
            .go_away_from_user(reason);
    }

    /// Starts a [graceful shutdown][1] process.
    ///
    /// Must continue being polled to close connection.
    ///
    /// It's possible to receive more requests after calling this method, since
    /// they might have been in-flight from the client already. After about
    /// 1 RTT, no new requests should be accepted. Once all active streams
    /// have completed, the connection is closed.
    ///
    /// [1]: http://httpwg.org/specs/rfc7540.html#GOAWAY
    pub fn graceful_shutdown(&mut self) {
        self.connection.go_away_gracefully();
    }
}

#[cfg(feature = "stream")]
impl<T> futures_core::Stream for ServerConnection<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<(Request, SendResponse), OpError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_accept(cx)
    }
}

// ===== SendResponse =====
#[derive(Debug)]
pub struct SendResponse {
    inner: StreamRef,
}

impl SendResponse {
    pub fn send_response(
        &mut self,
        response: Response,
    ) -> Result<(), UserError> {
        self.inner.send_response(response)
    }

    /// Sends response headers and returns a bounded incremental body sender.
    /// The response must not contain a buffered body or trailers.
    pub fn send_response_streaming(
        &mut self,
        response: Response,
        end_stream: bool,
    ) -> Result<crate::message::SendBody, OpError> {
        self.inner
            .send_response_streaming(response, end_stream)?;
        Ok(crate::message::SendBody {
            inner: self.inner.clone(),
        })
    }

    pub fn send_reset(&mut self, reason: Reason) {
        self.inner.send_reset(reason)
    }

    pub fn stream_id(&self) -> StreamId {
        self.inner.stream_id()
    }
}
