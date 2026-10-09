use http_plz::Response;
use tracing::trace;

use crate::frame::StreamId;
use crate::proto::streams::action::Actions;
use crate::proto::streams::counts::Counts;
use crate::proto::streams::recv::PartialResponse;
use crate::{frame::Reason, proto::streams::Resolve};
use std::task::{Context, Poll};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

use crate::proto::streams::{
    inner::Inner,
    store::{Key, Ptr},
};

pub(crate) struct OpaqueStreamRef {
    pub inner: Arc<Mutex<Inner>>,
    pub key: Key,
}

impl OpaqueStreamRef {
    pub fn new(inner: Arc<Mutex<Inner>>, stream: &mut Ptr) -> OpaqueStreamRef {
        stream.ref_inc();
        OpaqueStreamRef {
            inner,
            key: stream.key(),
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.inner.lock().unwrap().store[self.key].id
    }

    pub fn poll_response(
        &mut self,
        cx: &Context,
    ) -> Poll<Result<Response, PartialResponse>> {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        let mut stream = me.store.resolve(self.key);
        me.actions
            .recv
            .poll_response(cx, &mut stream)
    }

    pub fn poll_streaming_response(
        &mut self,
        cx: &Context,
    ) -> Poll<Result<Response, crate::error::OpError>> {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        let mut stream = me.store.resolve(self.key);
        me.actions
            .recv
            .poll_streaming_response(cx, &mut stream)
    }

    pub fn poll_body(
        &mut self,
        cx: &Context,
    ) -> Poll<Option<Result<crate::message::BodyFrame, crate::error::OpError>>>
    {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        let mut stream = me.store.resolve(self.key);
        let result = me
            .actions
            .recv
            .poll_body(cx, &mut stream);
        if result.is_ready()
            && let Some(task) = me.actions.task.take()
        {
            task.wake();
        }
        result
    }

    pub fn abandon_body(&mut self) {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        let stream = me.store.resolve(self.key);
        me.counts
            .transition(stream, |counts, stream| {
                me.actions
                    .recv
                    .discard_streaming_body(stream);
                if !stream.state.is_recv_end_stream() {
                    me.actions.send.schedule_implicit_reset(
                        stream,
                        Reason::CANCEL,
                        counts,
                        &mut me.actions.task,
                    );
                }
            });
        if let Some(task) = me.actions.task.take() {
            task.wake();
        }
    }

    pub fn take_partial_response(&mut self) -> Option<Response> {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        let mut stream = me.store.resolve(self.key);
        me.actions
            .recv
            .take_partial_response(&mut stream)
    }
}

impl Clone for OpaqueStreamRef {
    fn clone(&self) -> Self {
        // Increment the ref count
        let mut inner = self.inner.lock().unwrap();
        inner.store.resolve(self.key).ref_inc();
        inner.refs += 1;

        OpaqueStreamRef {
            inner: self.inner.clone(),
            key: self.key,
        }
    }
}

impl Drop for OpaqueStreamRef {
    fn drop(&mut self) {
        let mut me = match self.inner.lock() {
            Ok(inner) => inner,
            Err(_) => {
                if ::std::thread::panicking() {
                    tracing::trace!("StreamRef::drop; mutex poisoned");
                    return;
                } else {
                    panic!("StreamRef::drop; mutex poisoned");
                }
            }
        };

        let me = &mut *me;
        me.refs -= 1;
        let mut stream = me.store.resolve(self.key);
        trace!("drop_stream_ref| {:?}", stream.id);

        // decrement the stream's ref count by 1.
        stream.ref_dec();

        let actions = &mut me.actions;

        // If the stream is not referenced and it is already closed (does not
        // have to go through logic below of canceling the stream), we should
        // notify the task (connection) so that it can close properly
        if stream.ref_count == 0
            && stream.is_closed()
            && let Some(task) = actions.task.take()
        {
            trace!("stream already closed");
            task.wake();
        }

        // release capacity
        if stream.ref_count == 0 && stream.connection_window_allocated > 0 {
            let _ = actions
                .send
                .recv_connection_window_update(
                    10,
                    stream.store_mut(),
                    &mut me.counts,
                );
        }
        // Other handles may still be consuming the receive queue.
        if stream.ref_count == 0 {
            if stream.streaming_recv {
                actions
                    .recv
                    .discard_streaming_body(&mut stream);
            } else {
                actions
                    .recv
                    .clear_stream_queue(&mut stream);
            }
        }

        me.counts
            .transition(stream, |counts, stream| {
                maybe_cancel(stream, actions, counts);

                if stream.ref_count == 0 {
                    // We won't be able to reach our push promises anymore
                    let mut ppp = stream.pending_push_promises.take();
                    while let Some(promise) = ppp.pop(stream.store_mut()) {
                        counts.transition(promise, |counts, stream| {
                            maybe_cancel(stream, actions, counts);
                        });
                    }
                }
            });
    }
}

fn maybe_cancel(stream: &mut Ptr, actions: &mut Actions, counts: &mut Counts) {
    if stream.is_canceled_interest() && !stream.is_pending_send {
        // Server is allowed to early respond without fully consuming the
        // client input stream But per the RFC, must send a
        // RST_STREAM(NO_ERROR) in such cases.
        // https://www.rfc-editor.org/rfc/rfc7540#section-8.1 Some other http2
        // implementation may interpret other error code as fatal if not
        // respected (i.e: nginx https://trac.nginx.org/nginx/ticket/2376)
        let reason = if counts.role().is_server()
            && stream.state.is_send_closed()
            && stream.state.is_recv_streaming()
        {
            Reason::NO_ERROR
        } else {
            Reason::CANCEL
        };

        actions.send.schedule_implicit_reset(
            stream,
            reason,
            counts,
            &mut actions.task,
        );
        actions
            .recv
            .enqueue_reset_expiration(stream, counts);
    }
}

impl fmt::Debug for OpaqueStreamRef {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::sync::TryLockError::*;

        match self.inner.try_lock() {
            Ok(me) => {
                let stream = &me.store[self.key];
                fmt.debug_struct("OpaqueStreamRef")
                    .field("stream_id", &stream.id)
                    .field("ref_count", &stream.ref_count)
                    .finish()
            }
            Err(Poisoned(_)) => fmt
                .debug_struct("OpaqueStreamRef")
                .field("inner", &"<Poisoned>")
                .finish(),
            Err(WouldBlock) => fmt
                .debug_struct("OpaqueStreamRef")
                .field("inner", &"<Locked>")
                .finish(),
        }
    }
}
