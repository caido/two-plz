//! Bidirectional DATA streams, including WebSockets over extended CONNECT.
//!
//! Enable `ServerBuilder::enable_connect_protocol`, send a CONNECT request with
//! a `Protocol::from_static("websocket")` request-line extension using
//! `SendRequest::send_request_streaming(request, false)`, and accept it with
//! `ServerConnection::accept_streaming`. Send a successful response with
//! `SendResponse::send_response_streaming(response, false)`. On each side, pair
//! the resulting receive and send bodies with [`Tunnel::new`]. The client must
//! check the response status before treating the stream as a WebSocket tunnel.
//!
//! Keep driving both connections independently. This module transports bytes;
//! WebSocket handshakes, framing, and close messages belong to the caller's codec.

use crate::{BodyFrame, RecvBody, SendBody, error::OpError, frame::Reason};
use bytes::{Buf, Bytes};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A flow-controlled, bidirectional HTTP/2 byte stream.
///
/// `shutdown` sends END_STREAM only in the write direction; reads remain usable.
/// Dropping an unfinished tunnel cancels the stream. `flush` waits for queued
/// DATA to be flushed to the transport, not for acknowledgement by the peer.
#[derive(Debug)]
pub struct Tunnel {
    recv: RecvBody,
    send: SendBody,
    pending_read: Bytes,
    write_closed: bool,
}

impl Tunnel {
    /// Pairs receive and send handles belonging to the same connection and stream.
    /// On mismatch, the handles are returned unchanged.
    pub fn new(
        recv: RecvBody,
        send: SendBody,
    ) -> Result<Self, (RecvBody, SendBody)> {
        if recv.inner.key != send.inner.opaque.key
            || !Arc::ptr_eq(&recv.inner.inner, &send.inner.opaque.inner)
        {
            return Err((recv, send));
        }
        Ok(Self {
            recv,
            send,
            pending_read: Bytes::new(),
            write_closed: false,
        })
    }

    pub fn stream_id(&self) -> crate::frame::StreamId {
        self.send.stream_id()
    }

    /// Aborts both directions with RST_STREAM.
    pub fn send_reset(&mut self, reason: Reason) {
        self.send.send_reset(reason);
        self.write_closed = true;
        self.pending_read = Bytes::new();
    }
}

fn io_error(error: OpError) -> io::Error {
    io::Error::other(error)
}

impl AsyncRead for Tunnel {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if !self.pending_read.is_empty() {
                let len = buf
                    .remaining()
                    .min(self.pending_read.len());
                buf.put_slice(&self.pending_read[..len]);
                self.pending_read.advance(len);
                return Poll::Ready(Ok(()));
            }
            match self.recv.poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Err(io_error(error)));
                }
                Poll::Ready(Some(Ok(BodyFrame::Data(data)))) => {
                    self.pending_read = data
                }
                Poll::Ready(Some(Ok(BodyFrame::Trailers(_)))) => {
                    self.send_reset(Reason::PROTOCOL_ERROR);
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "trailers on a tunnel",
                    )));
                }
            }
        }
    }
}

impl AsyncWrite for Tunnel {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tunnel write half is closed",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // Bound the copy independently of the caller's buffer size.
        let len = buf.len().min(16 * 1024);
        let mut data = Bytes::copy_from_slice(&buf[..len]);
        let result = self
            .send
            .poll_send_data(cx, &mut data, false);
        let sent = len - data.len();
        if sent != 0 {
            return Poll::Ready(Ok(sent));
        }
        result.map(|r| r.map(|()| 0).map_err(io_error))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        self.send
            .inner
            .poll_flush(cx)
            .map_err(|error| io_error(error.into()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.write_closed {
            let mut empty = Bytes::new();
            match self
                .send
                .poll_send_data(cx, &mut empty, true)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(io_error(error)));
                }
                Poll::Ready(Ok(())) => self.write_closed = true,
            }
        }
        self.poll_flush(cx)
    }
}
