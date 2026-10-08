use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use support::prelude::*;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

struct FaultIo {
    inner: mock::Mock,
    fail: Arc<AtomicBool>,
    write: bool,
}

impl AsyncRead for FaultIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.write && self.fail.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for FaultIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write && self.fail.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn queued_requests_are_released_on_transport_errors() {
    for write in [false, true] {
        let (io, mut peer) = mock::new();
        let fail = Arc::new(AtomicBool::new(false));
        let (queued_tx, queued_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let server = async move {
            let mut server = ServerBuilder::new()
                .handshake(FaultIo {
                    inner: io,
                    fail: fail.clone(),
                    write,
                })
                .await
                .unwrap();
            poll_fn(|cx| server.poll_closed(cx))
                .drive(queued_rx)
                .await
                .unwrap();
            assert_eq!(server.num_wired_streams(), 2);
            fail.store(true, Ordering::SeqCst);
            if write {
                server.abrupt_shutdown(Reason::INTERNAL_ERROR);
            }
            assert!(
                poll_fn(|cx| server.poll_closed(cx))
                    .await
                    .is_err()
            );
            assert_eq!(server.num_wired_streams(), 0);
            let _ = poll_fn(|cx| server.poll_closed(cx)).await;
            assert_eq!(server.num_wired_streams(), 0);
            done_tx.send(()).unwrap();
        };
        let peer = async move {
            peer.assert_server_handshake().await;
            for id in [1, 3] {
                peer.send_frame(
                    frames::headers(id)
                        .request("GET", "https", "a.b", "/")
                        .eos(),
                )
                .await;
            }
            peer.send_frame(frames::ping(*b"queued!!"))
                .await;
            peer.recv_frame(frames::ping(*b"queued!!").pong())
                .await;
            queued_tx.send(()).unwrap();
            done_rx.await.unwrap();
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            join(server, peer),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn terminal_poll_discards_unaccepted_requests() {
    for protocol_error in [false, true] {
        let (io, mut peer) = mock::new();
        let server = async move {
            let mut server = ServerBuilder::new()
                .handshake(io)
                .await
                .unwrap();
            let result = poll_fn(|cx| server.poll_closed(cx)).await;
            assert_eq!(result.is_err(), protocol_error);
            assert_eq!(server.num_wired_streams(), 0);
            // Cleanup must also be safe when the terminal driver is polled again.
            let _ = poll_fn(|cx| server.poll_closed(cx)).await;
            assert_eq!(server.num_wired_streams(), 0);
        };
        let peer = async move {
            peer.assert_server_handshake().await;
            for id in [1, 3] {
                peer.send_frame(
                    frames::headers(id)
                        .request("GET", "https", "a.b", "/")
                        .eos(),
                )
                .await;
            }
            if protocol_error {
                peer.send_frame(frames::data(5, "idle"))
                    .await;
                peer.recv_frame(frames::go_away(3).protocol_error())
                    .await;
            }
        };
        join(server, peer).await;
    }
}

#[tokio::test]
async fn active_poll_keeps_queue_and_abrupt_shutdown_releases_it() {
    let (io, mut peer) = mock::new();
    let (queued_tx, queued_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = async move {
        let mut server = ServerBuilder::new()
            .handshake(io)
            .await
            .unwrap();
        poll_fn(|cx| server.poll_closed(cx))
            .drive(queued_rx)
            .await
            .unwrap();
        assert_eq!(server.num_wired_streams(), 2);
        let (_, mut accepted) = server.accept().await.unwrap().unwrap();
        server.abrupt_shutdown(Reason::INTERNAL_ERROR);
        poll_fn(|cx| server.poll_closed(cx))
            .await
            .unwrap();
        // The accepted handle stays owned, but must observe terminal closure.
        assert!(
            accepted
                .send_response(build_test_response())
                .is_err()
        );
        drop(accepted);
        assert_eq!(server.num_wired_streams(), 0);
        closed_tx.send(()).unwrap();
    };
    let peer = async move {
        peer.assert_server_handshake().await;
        for id in [1, 3] {
            peer.send_frame(
                frames::headers(id)
                    .request("GET", "https", "a.b", "/")
                    .eos(),
            )
            .await;
        }
        peer.send_frame(frames::ping(*b"queued!!"))
            .await;
        peer.recv_frame(frames::ping(*b"queued!!").pong())
            .await;
        queued_tx.send(()).unwrap();
        peer.recv_frame(frames::go_away(3).internal_error())
            .await;
        closed_rx.await.unwrap();
    };
    join(server, peer).await;
}
