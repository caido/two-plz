use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use support::prelude::*;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use two_plz::Tunnel;

#[derive(Default)]
struct GateState {
    blocked: bool,
    failed: bool,
    task: Option<Waker>,
}
#[derive(Clone, Default)]
struct Gate(Arc<Mutex<GateState>>);
impl Gate {
    fn block(&self) {
        self.0.lock().unwrap().blocked = true;
    }
    fn release(&self, failed: bool) {
        let mut state = self.0.lock().unwrap();
        state.blocked = false;
        state.failed = failed;
        if let Some(task) = state.task.take() {
            task.wake();
        }
    }
    async fn reached(&self) {
        poll_fn(|cx| {
            if self.0.lock().unwrap().task.is_some() {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }
}
struct GatedIo {
    io: DuplexStream,
    gate: Gate,
}
impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.gate.0.lock().unwrap();
        if state.blocked {
            state.task = Some(cx.waker().clone());
            Poll::Pending
        } else if state.failed {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "gated flush failed",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn pair() -> (Tunnel, Tunnel, Gate, Driver, Driver) {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let gate = Gate::default();
    let (client, server) = tokio::join!(
        ClientBuilder::new().handshake(GatedIo {
            io: client_io,
            gate: gate.clone()
        }),
        ServerBuilder::new()
            .enable_connect_protocol()
            .handshake(server_io),
    );
    let (mut connection, mut sender) = client.unwrap();
    let mut server = server.unwrap();
    poll_fn(|cx| {
        assert!(
            Pin::new(&mut connection)
                .poll(cx)
                .is_pending()
        );
        if connection.is_extended_connect_protocol_enabled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    let client_driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = Request::builder()
        .method(Method::CONNECT)
        .extension("websocket".into())
        .uri(build_test_uri())
        .build();
    let (response, send) = sender
        .send_request_streaming(request, false)
        .unwrap();
    let (_, recv, mut respond) = server
        .accept_streaming()
        .await
        .unwrap()
        .unwrap();
    let server_send = respond
        .send_response_streaming(build_test_response(), false)
        .unwrap();
    let server_tunnel = Tunnel::new(recv, server_send).unwrap();
    let server_driver = Driver(tokio::spawn(async move {
        let _ = poll_fn(|cx| server.poll_closed(cx)).await;
    }));
    let (_, recv) = response.await.unwrap();
    (
        Tunnel::new(recv, send).unwrap(),
        server_tunnel,
        gate,
        client_driver,
        server_driver,
    )
}

async fn check(shutdown: bool, failed: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut client, _server, gate, _client_driver, _server_driver) =
            pair().await;
        // Finish the handshake's writes before closing the transport flush gate.
        client
            .write_all(b"before gate")
            .await
            .unwrap();
        client.flush().await.unwrap();
        gate.block();
        if !shutdown {
            client
                .write_all(b"flush me")
                .await
                .unwrap();
        }
        let mut operation = Box::pin(async {
            if shutdown {
                client.shutdown().await
            } else {
                client.flush().await
            }
        });
        poll_fn(|cx| {
            assert!(operation.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        gate.reached().await;
        // DATA (or empty END_STREAM) has reached the transport, but flush is gated.
        poll_fn(|cx| {
            assert!(operation.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        gate.release(failed);
        let result = operation.await;
        assert_eq!(result.is_err(), failed);
    })
    .await
    .expect("gated tunnel flush stalled");
}

#[tokio::test]
async fn flush_waits_for_transport() {
    check(false, false).await;
}
#[tokio::test]
async fn empty_shutdown_waits_for_transport() {
    check(true, false).await;
}
#[tokio::test]
async fn flush_reports_transport_error() {
    check(false, true).await;
}
#[tokio::test]
async fn empty_shutdown_reports_transport_error() {
    check(true, true).await;
}
