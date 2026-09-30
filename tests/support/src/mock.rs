use crate::SendFrame;
use two_plz::codec::SendError;
use two_plz::frame::{self, Frame};
use two_plz::proto::ProtoError;

use futures::future::poll_fn;
use futures::{Stream, StreamExt, ready};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use super::assert::assert_frame_eq;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use std::{cmp, io};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// A mock I/O
#[derive(Debug)]
pub struct Mock {
    pipe: Pipe,
}

impl AsyncRead for Mock {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        assert!(
            buf.remaining() > 0,
            "attempted read with zero length buffer... wut?"
        );

        let mut me = self.pipe.inner.lock().unwrap();

        if me.unexpected_eof {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Simulate an unexpected eof error",
            )));
        }

        if me.rx.is_empty() {
            if me.closed {
                return Poll::Ready(Ok(()));
            }

            me.rx_task = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = cmp::min(buf.remaining(), me.rx.len());
        buf.put_slice(&me.rx[..n]);
        me.rx.drain(..n);

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Mock {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut me = self.pipe.inner.lock().unwrap();

        if me.closed {
            return Poll::Ready(Ok(buf.len()));
        }

        if me.tx_rem == 0 {
            me.tx_rem_task = Some(cx.waker().clone());
            return Poll::Pending;
        }

        if buf.len() > me.tx_rem {
            buf = &buf[..me.tx_rem];
        }

        me.tx.extend(buf);
        me.tx_rem -= buf.len();

        if let Some(task) = me.tx_task.take() {
            task.wake();
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        let mut me = self.pipe.inner.lock().unwrap();
        me.closed = true;

        if let Some(task) = me.tx_task.take() {
            task.wake();
        }
    }
}

#[derive(Debug)]
pub struct Handle {
    codec: crate::Codec<Pipe>,
    pending_frame: Option<Frame>,
}

impl Stream for Handle {
    type Item = Result<Frame, ProtoError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(frame) = self.pending_frame.take() {
            return Poll::Ready(Some(Ok(frame)));
        }
        Pin::new(&mut self.codec).poll_next(cx)
    }
}

impl AsyncRead for Handle {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.codec.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for Handle {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(self.codec.get_mut()).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(self.codec.get_mut()).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(self.codec.get_mut()).poll_shutdown(cx)
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Shutdown *shouldn't* need a real Waker...
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(self.codec.shutdown(&mut cx).is_ready());

        if let Ok(mut me) = self.codec.get_mut().inner.lock() {
            me.closed = true;

            if let Some(task) = me.rx_task.take() {
                task.wake();
            }
        }
    }
}

impl Handle {
    /// Get a mutable reference to inner Codec.
    pub fn codec_mut(&mut self) -> &mut crate::Codec<Pipe> {
        &mut self.codec
    }

    pub fn close_without_notify(&mut self) {
        let mut me = self
            .codec
            .get_mut()
            .inner
            .lock()
            .unwrap();
        me.unexpected_eof = true;
    }

    /// Send a frame
    pub async fn send(&mut self, item: SendFrame) -> Result<(), SendError> {
        // Queue the frame
        self.codec.buffer(item).unwrap();

        // Flush the frame
        poll_fn(|cx| {
            let p = self.codec.flush(cx);
            assert!(p.is_ready());
            p
        })
        .await?;
        Ok(())
    }

    /// Writes the client preface
    pub async fn write_preface(&mut self) {
        self.codec
            .get_mut()
            .write_all(PREFACE)
            .await
            .unwrap();
    }

    /// Read the client preface
    pub async fn read_preface(&mut self) -> io::Result<()> {
        let mut buf = vec![0u8; PREFACE.len()];
        self.read_exact(&mut buf).await?;
        assert_eq!(buf, PREFACE);
        Ok(())
    }

    pub async fn recv_frame<F: Into<Frame>>(&mut self, expected: F) {
        let frame = self.next().await.unwrap().unwrap();
        assert_frame_eq(frame, expected);
    }

    pub async fn recv_frame_raw(&mut self) -> Frame {
        self.next().await.unwrap().unwrap()
    }

    pub async fn send_frame<F: Into<SendFrame>>(&mut self, frame: F) {
        self.send(frame.into()).await.unwrap();
    }

    pub async fn recv_eof(&mut self) {
        let frame = self.next().await;
        assert!(frame.is_none());
    }

    pub async fn send_bytes(&mut self, data: &[u8]) {
        use bytes::Buf;
        use std::io::Cursor;

        let buf: Vec<_> = data.into();
        let mut buf = Cursor::new(buf);

        poll_fn(move |cx| {
            while buf.has_remaining() {
                let res = Pin::new(self.codec.get_mut())
                    .poll_write(cx, buf.chunk())
                    .map_err(|e| panic!("write err={:?}", e));

                let n = ready!(res).unwrap();
                buf.advance(n);
            }

            Poll::Ready(())
        })
        .await;
    }

    /// Perform the H2 handshake
    pub async fn assert_client_handshake(&mut self) -> frame::Settings {
        self.assert_client_handshake_with_settings(frame::Settings::default())
            .await
    }

    /// Perform the H2 handshake
    pub async fn assert_client_handshake_with_settings<T>(
        &mut self,
        settings: T,
    ) -> frame::Settings
    where
        T: Into<frame::Settings>,
    {
        let settings = settings.into();
        // Servers send their connection preface independently of the client
        // preface, so make it available before waiting for the client bytes.
        self.send(settings.into())
            .await
            .unwrap();
        // read preface
        self.read_preface().await.unwrap();

        // read settings
        let settings = match self.next().await {
            Some(frame) => match frame.unwrap() {
                Frame::Settings(settings) => {
                    // Send the ACK
                    let ack = frame::Settings::ack();
                    // TODO: Don't unwrap?
                    self.send(ack.into()).await.unwrap();

                    settings
                }
                frame => {
                    panic!("unexpected frame; frame={:?}", frame);
                }
            },
            None => {
                panic!("unexpected EOF");
            }
        };

        // Enhanced mode can send its initial PING before the SETTINGS ACK.
        // Preserve it so the caller still verifies and acknowledges the PING.
        let frame = self.next().await.unwrap().unwrap();
        let frame = if let Frame::Ping(_) = frame {
            self.pending_frame = Some(frame);
            self.codec
                .next()
                .await
                .unwrap()
                .unwrap()
        } else {
            frame
        };
        let f = assert_settings!(frame);
        assert!(f.is_ack());

        settings
    }

    /// Perform the H2 handshake
    pub async fn assert_server_handshake(&mut self) -> frame::Settings {
        self.assert_server_handshake_with_settings(frame::Settings::default())
            .await
    }

    /// Perform the H2 handshake
    pub async fn assert_server_handshake_with_settings<T>(
        &mut self,
        settings: T,
    ) -> frame::Settings
    where
        T: Into<frame::Settings>,
    {
        self.write_preface().await;

        // send settings
        let settings = settings.into();
        self.send(settings.into())
            .await
            .unwrap();

        // read settings
        let frame = self
            .next()
            .await
            .expect("unexpected EOF")
            .unwrap();
        let settings = assert_settings!(frame);

        // Send the ACK
        let ack = frame::Settings::ack();
        // TODO: Don't unwrap?
        self.send(ack.into()).await.unwrap();

        // read ack
        let frame = self.next().await;
        let f = assert_settings!(frame.unwrap().unwrap());
        // Is ACK
        assert!(f.is_ack());

        settings
    }

    pub async fn ping_pong(&mut self, payload: [u8; 8]) {
        self.send_frame(crate::frames::ping(payload))
            .await;
        self.recv_frame(crate::frames::ping(payload).pong())
            .await;
    }

    pub async fn buffer_bytes(&mut self, num: usize) {
        // Set tx_rem to num
        {
            let mut i = self
                .codec
                .get_mut()
                .inner
                .lock()
                .unwrap();
            i.tx_rem = num;
            if let Some(task) = i.tx_rem_task.take() {
                task.wake();
            }
        }

        poll_fn(move |cx| {
            {
                let mut inner = self
                    .codec
                    .get_mut()
                    .inner
                    .lock()
                    .unwrap();
                if inner.tx_rem == 0 {
                    inner.tx_rem = usize::MAX;
                } else {
                    inner.tx_task = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }

            Poll::Ready(())
        })
        .await;
    }

    pub async fn unbounded_bytes(&mut self) {
        let mut i = self
            .codec
            .get_mut()
            .inner
            .lock()
            .unwrap();
        i.tx_rem = usize::MAX;

        if let Some(task) = i.tx_rem_task.take() {
            task.wake();
        }
    }
}

#[derive(Debug)]
pub struct Pipe {
    inner: Arc<Mutex<Inner>>,
}

impl AsyncRead for Pipe {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        assert!(
            buf.remaining() > 0,
            "attempted read with zero length buffer... wut?"
        );

        let mut me = self.inner.lock().unwrap();

        if me.tx.is_empty() {
            if me.closed {
                return Poll::Ready(Ok(()));
            }

            me.tx_task = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = cmp::min(buf.remaining(), me.tx.len());
        buf.put_slice(&me.tx[..n]);
        me.tx.drain(..n);

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Pipe {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut me = self.inner.lock().unwrap();
        me.rx.extend(buf);

        if let Some(task) = me.rx_task.take() {
            task.wake();
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct Inner {
    /// Data written by the test case to the lib.
    rx: Vec<u8>,

    /// Notify when data is ready to be received.
    rx_task: Option<Waker>,

    /// Data written by the library to be read by the test case.
    tx: Vec<u8>,

    /// Notify when data is written. This notifies the test case waiters.
    tx_task: Option<Waker>,

    /// Number of bytes that can be written before `write` returns `Poll::Pending`.
    tx_rem: usize,

    /// Task to notify when write capacity becomes available.
    tx_rem_task: Option<Waker>,

    /// True when the pipe is closed.
    closed: bool,

    /// Trigger an `UnexpectedEof` error on read
    unexpected_eof: bool,
}

/// Create a new mock and handle
pub fn new() -> (Mock, Handle) {
    new_with_write_capacity(usize::MAX)
}

/// Create a new mock and handle allowing up to `cap` bytes to be written.
pub fn new_with_write_capacity(cap: usize) -> (Mock, Handle) {
    let inner = Arc::new(Mutex::new(Inner {
        rx: vec![],
        rx_task: None,
        tx: vec![],
        tx_task: None,
        tx_rem: cap,
        tx_rem_task: None,
        closed: false,
        unexpected_eof: false,
    }));

    let mock = Mock {
        pipe: Pipe {
            inner: inner.clone(),
        },
    };

    let handle = Handle {
        codec: two_plz::Codec::new(Pipe {
            inner,
        }),
        pending_frame: None,
    };

    (mock, handle)
}

pub async fn idle_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await
}
