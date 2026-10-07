use bytes::Bytes;
use http_plz::{Message, Request};
use tokio::io::AsyncWrite;
use tracing::trace;

use crate::{
    Codec,
    codec::{SendError, UserError},
    frame::{self, Frame},
    message::TwoTwoFrame,
    proto::{
        config::ConnectionConfig,
        ping_pong::PingHandler,
        streams::{
            buffer::Buffer,
            inner::Inner,
            opaque_streams_ref::OpaqueStreamRef,
            send_buffer::SendBuffer,
            store::{Ptr, Resolve},
            stream::Stream,
            streams_ref::StreamRef,
        },
    },
};

use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use crate::frame::{Reason, Settings, StreamId};
use crate::{proto::ProtoError, role::Role};

#[derive(Debug)]
pub(crate) struct Streams<B> {
    /// Holds most of the connection and stream related state for processing
    /// HTTP/2 frames associated with streams.
    inner: Arc<Mutex<Inner>>,

    /// This is the queue of frames to be written to the wire. This is split out
    /// to avoid requiring a `B` generic on all public API types even if `B` is
    /// not technically required.
    ///
    /// Currently, splitting this out requires a second `Arc` + `Mutex`.
    /// However, it should be possible to avoid this duplication with a little
    /// bit of unsafe code. This optimization has been postponed until it has
    /// been shown to be necessary.
    send_buffer: Arc<SendBuffer<B>>,
    // TODO: Why ?
    //_p: ::std::marker::PhantomData<P>,
}

impl Streams<Bytes> {
    pub fn new(role: Role, config: ConnectionConfig) -> Self {
        Streams {
            inner: Inner::new(role, config),
            send_buffer: Arc::new(SendBuffer::new()),
        }
    }

    // ===== Send =====
    pub fn send_request(
        &mut self,
        request: Request,
        is_spa: bool,
    ) -> Result<OpaqueStreamRef, SendError> {
        use super::stream::ContentLength;
        use header_plz::Method;
        // TODO(hyper): There is a hazard with assigning a stream ID before the
        // prioritize layer. If prioritization reorders new streams, this
        // implicitly closes the earlier stream IDs.
        //
        // See: hyperium/h2#11

        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        let mut send_buffer = self.send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        me.actions.ensure_no_conn_error()?;
        me.actions
            .send
            .ensure_next_stream_id()?;

        if me.counts.role().is_server() {
            // Servers cannot open streams. PushPromise must first be reserved.
            return Err(UserError::UnexpectedFrameType.into());
        }

        let mut request = request;
        let body = request.take_body();
        let trailers = request.take_trailers();
        let (head, headers) = request.into_message_head();
        crate::message::validate_extended_connect_request(&head)?;
        if head.extension().is_some()
            && !me
                .actions
                .send
                .is_extended_connect_protocol_enabled()
        {
            return Err(UserError::Rejected.into());
        }
        let request = Message::new(head, headers, body, trailers);

        let stream_id = me.actions.send.open()?;

        let mut stream = Stream::new(
            stream_id,
            me.actions.send.init_window_sz(),
            me.actions.recv.init_window_sz(),
        );

        stream.is_connect = *request.method() == Method::CONNECT;
        if *request.method() == Method::HEAD {
            stream.content_length = ContentLength::Head;
        }

        let mut stream = me.store.insert(stream.id, stream);

        let mut frames = TwoTwoFrame::from((stream.id, request));
        let data_frame = frames.take_data().or_else(|| {
            is_spa.then(|| {
                frames.header.unset_end_stream();
                frame::Data::new(stream_id, Bytes::new())
            })
        });
        let trailer_frame = frames.take_trailer();

        if let Err(e) = me.actions.send.send_headers(
            frames.header,
            send_buffer,
            &mut stream,
            &mut me.counts,
            &mut me.actions.task,
        ) {
            stream.unlink();
            stream.remove();
            return Err(e.into());
        }

        queue_body_trailer(
            &mut stream,
            data_frame,
            trailer_frame,
            send_buffer,
        );

        // Given that the stream has been initialized, it should not be in the
        // closed state.
        debug_assert!(!stream.state.is_closed());

        // TODO(hyper): ideally, OpaqueStreamRefs::new would do this, but we're
        // holding the lock, so it can't.
        me.refs += 1;

        Ok(OpaqueStreamRef::new(self.inner.clone(), &mut stream))
    }

    pub fn send_request_streaming(
        &mut self,
        request: Request,
        end_stream: bool,
        is_spa: bool,
    ) -> Result<StreamRef, SendError> {
        use super::stream::ContentLength;
        use header_plz::Method;
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        me.actions.ensure_no_conn_error()?;
        me.actions
            .send
            .ensure_streaming_supported()?;
        me.actions
            .send
            .ensure_next_stream_id()?;
        if me.counts.role().is_server() {
            return Err(UserError::UnexpectedFrameType.into());
        }
        if is_spa {
            return Err(UserError::Rejected.into());
        }
        let mut request = request;
        let body = request.take_body();
        let trailers = request.take_trailers();
        let (head, headers) = request.into_message_head();
        crate::message::validate_extended_connect_request(&head)?;
        if head.extension().is_some()
            && !me
                .actions
                .send
                .is_extended_connect_protocol_enabled()
        {
            return Err(UserError::Rejected.into());
        }
        let is_head = *head.method() == Method::HEAD;
        let is_connect = *head.method() == Method::CONNECT;
        // Validate the streaming head before allocating a stream ID.
        if body.is_some() || trailers.is_some() {
            return Err(UserError::UnexpectedFrameType.into());
        }
        let request = Message::new(head, headers, body, trailers);
        let id = me.actions.send.open()?;
        let mut frames = TwoTwoFrame::from((id, request));
        if !end_stream {
            frames.header.unset_end_stream();
        }
        let mut stream = Stream::new(
            id,
            me.actions.send.init_window_sz(),
            me.actions.recv.init_window_sz(),
        );
        stream.streaming_recv = true;
        stream.is_connect = is_connect;
        if is_head {
            stream.content_length = ContentLength::Head;
        }
        let mut stream = me.store.insert(id, stream);
        let mut buffer = self.send_buffer.inner.lock().unwrap();
        if let Err(error) = me.actions.send.send_headers(
            frames.header,
            &mut buffer,
            &mut stream,
            &mut me.counts,
            &mut me.actions.task,
        ) {
            stream.unlink();
            stream.remove();
            return Err(error.into());
        }
        if !end_stream {
            me.actions.send.start_streaming(id);
        }
        me.refs += 1;
        Ok(StreamRef::new(
            self.inner.clone(),
            &mut stream,
            self.send_buffer.clone(),
        ))
    }

    pub fn set_streaming_accept(&mut self) {
        self.inner
            .lock()
            .unwrap()
            .actions
            .recv
            .streaming = true;
    }

    // ===== Recv =====
    pub fn next_accept(&mut self) -> Option<StreamRef> {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        me.actions
            .recv
            .next_accept(&mut me.store)
            .map(|key| {
                let stream = &mut me.store.resolve(key);
                tracing::trace!(
                    "next_incoming| id={:?}, state={:?}",
                    stream.id,
                    stream.state
                );
                // TODO(hyper): ideally, OpaqueStreamRefs::new would do this, but
                // we're holding the lock, so it can't.
                me.refs += 1;

                // Pending-accepted remotely-reset streams are counted.
                if stream.state.is_remote_reset() {
                    me.counts.dec_num_remote_reset_streams();
                }

                StreamRef::new(
                    self.inner.clone(),
                    stream,
                    self.send_buffer.clone(),
                )
            })
    }

    pub fn next_push(&mut self) -> Option<(Request, OpaqueStreamRef)> {
        let inner = self.next_accept()?;
        let request = {
            let mut me = self.inner.lock().unwrap();
            me.store
                .resolve(inner.opaque.key)
                .promised_request
                .take()?
        };
        Some((request, inner.opaque))
    }

    pub fn recv_push_promise(
        &mut self,
        promise: frame::PushPromise,
    ) -> Result<(), ProtoError> {
        self.inner
            .lock()
            .unwrap()
            .recv_push_promise(&self.send_buffer, promise)
    }

    // ===== FRAMES =====

    // ===== Data =====
    pub fn recv_data(&mut self, frame: frame::Data) -> Result<(), ProtoError> {
        let mut me = self.inner.lock().unwrap();
        me.recv_data(&self.send_buffer, frame)
    }

    // ===== Header =====
    pub fn recv_header(
        &mut self,
        frame: frame::Headers,
    ) -> Result<(), ProtoError> {
        let mut me = self.inner.lock().unwrap();
        me.recv_headers(&self.send_buffer, frame)
    }

    // ===== Reset =====
    pub fn recv_reset(
        &mut self,
        frame: frame::Reset,
    ) -> Result<(), ProtoError> {
        let mut me = self.inner.lock().unwrap();
        me.recv_reset(&self.send_buffer, frame)
    }

    pub fn send_reset(
        &mut self,
        id: StreamId,
        reason: Reason,
    ) -> Result<(), crate::proto::error::GoAway> {
        let mut me = self.inner.lock().unwrap();
        me.send_reset(&self.send_buffer, id, reason)
    }

    // ===== Settings =====
    pub fn apply_local_settings(
        &mut self,
        settings: &Settings,
    ) -> Result<(), ProtoError> {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;

        me.actions
            .recv
            .apply_local_settings(settings, &mut me.store)
    }

    pub fn apply_remote_settings(
        &mut self,
        settings: &Settings,
    ) -> Result<(), ProtoError> {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;

        let mut send_buffer = self.send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;

        me.counts
            .apply_remote_settings(settings);
        me.actions.send.apply_remote_settings(
            settings,
            send_buffer,
            &mut me.store,
            &mut me.counts,
            &mut me.actions.task,
        )
    }

    // ===== Goaway ====
    pub fn recv_go_away(
        &mut self,
        frame: &frame::GoAway,
    ) -> Result<(), ProtoError> {
        let mut me = self.inner.lock().unwrap();
        me.recv_go_away(&self.send_buffer, frame)
    }

    pub fn send_go_away(&mut self, last_processed_id: StreamId) {
        let mut me = self.inner.lock().unwrap();
        me.actions
            .recv
            .go_away(last_processed_id);
    }

    // ===== Window Update =====
    pub fn poll_window_update<T>(
        &mut self,
        cx: &mut Context,
        dst: &mut Codec<T, Bytes>,
    ) -> Poll<std::io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        let mut me = self.inner.lock().unwrap();
        me.poll_window_update(cx, dst)
    }

    pub fn recv_window_update(
        &mut self,
        frame: frame::WindowUpdate,
    ) -> Result<(), ProtoError> {
        let mut me = self.inner.lock().unwrap();
        me.recv_window_update(&self.send_buffer, frame)
    }

    // ===== polling =====
    pub fn poll_complete<T>(
        &mut self,
        cx: &mut Context,
        dst: &mut Codec<T, Bytes>,
    ) -> Poll<std::io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        let mut me = self.inner.lock().unwrap();
        me.poll_complete(&self.send_buffer, cx, dst)
    }

    // ===== EOF =====
    pub fn recv_eof(&mut self, clear_pending_accept: bool) -> Result<(), ()> {
        let mut me = self.inner.lock().map_err(|_| ())?;
        me.recv_eof(&self.send_buffer, clear_pending_accept)
    }

    // ===== misc =====
    pub fn clear_expired_reset_streams(&mut self) {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        me.actions
            .recv
            .clear_expired_reset_streams(&mut me.store, &mut me.counts);
    }

    pub fn has_streams(&self) -> bool {
        let me = self.inner.lock().unwrap();
        me.counts.has_streams()
    }

    pub fn has_streams_or_other_references(&self) -> bool {
        let me = self.inner.lock().unwrap();
        me.counts.has_streams() || me.refs > 1
    }

    pub fn last_processed_id(&self) -> StreamId {
        self.inner
            .lock()
            .unwrap()
            .actions
            .recv
            .last_processed_id()
    }

    pub fn send_buffer_is_empty(&self) -> bool {
        self.send_buffer.is_empty()
    }

    pub fn send_pending_refusal<T>(
        &mut self,
        cx: &mut Context,
        dst: &mut Codec<T, Bytes>,
    ) -> Poll<std::io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        me.actions
            .recv
            .send_pending_refusal(cx, dst)
    }

    /// Notify all streams that a connection-level error happened.
    pub fn handle_error(&self, e: ProtoError) -> StreamId {
        let mut me = self.inner.lock().unwrap();
        me.handle_error(&self.send_buffer, e)
    }

    // ===== Test only ====
    pub(crate) fn num_active_streams(&self) -> usize {
        let me = self.inner.lock().unwrap();
        me.store.num_active_streams()
    }

    pub(crate) fn num_wired_streams(&self) -> usize {
        let me = self.inner.lock().unwrap();
        me.store.num_wired_streams()
    }

    pub(crate) fn is_extended_connect_protocol_enabled(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .actions
            .send
            .is_extended_connect_protocol_enabled()
    }

    pub(crate) fn max_send_streams(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .counts
            .max_send_streams()
    }

    // ===== SPA =====
    pub(crate) fn poll_spa<T>(
        &self,
        cx: &mut Context<'_>,
        dst: &mut Codec<T, Bytes>,
        ping_handler: &mut PingHandler,
    ) -> Poll<std::io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        let mut send_buffer = self.send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        me.actions.send.poll_spa(
            cx,
            send_buffer,
            &mut me.store,
            dst,
            ping_handler,
        )
    }

    pub(crate) fn recvd_ping(&self, payload: &[u8; 8], cx: &mut Context) {
        let mut me = self.inner.lock().unwrap();
        let me = &mut *me;
        me.actions.send.recvd_ping(payload, cx)
    }
}

impl<B> Clone for Streams<B> {
    fn clone(&self) -> Self {
        self.inner.lock().unwrap().refs += 1;
        Streams {
            inner: self.inner.clone(),
            send_buffer: self.send_buffer.clone(),
            //_p: ::std::marker::PhantomData,
        }
    }
}

impl<B> Drop for Streams<B> {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.refs -= 1;
            if inner.refs == 1
                && let Some(task) = inner.actions.task.take()
            {
                task.wake();
            }
        }
    }
}

pub fn queue_body_trailer(
    stream: &mut Ptr,
    data_frame: Option<frame::Data<Bytes>>,
    trailer_frame: Option<frame::Headers>,
    send_buffer: &mut Buffer<Frame>,
) {
    if let Some(frame) = data_frame {
        trace!("added| data");
        stream.remaining_data_len = Some(frame.payload().len());
        stream
            .pending_send
            .push_back(send_buffer, frame.into());
    }

    if let Some(frame) = trailer_frame {
        trace!("added| trailer");
        stream
            .pending_send
            .push_back(send_buffer, frame.into());
    }
}
