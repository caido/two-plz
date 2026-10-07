use http_plz::{Request, Response};
use std::sync::{Arc, Mutex};
use tracing::Level;

use bytes::Bytes;
use tracing::{span, trace};

use crate::{
    codec::UserError,
    error::Reason,
    frame::StreamId,
    message::TwoTwoFrame,
    proto::{
        error::Initiator,
        streams::{
            inner::Inner,
            opaque_streams_ref::OpaqueStreamRef,
            send_buffer::SendBuffer,
            store::{Ptr, Resolve},
            streams::queue_body_trailer,
        },
    },
};

/// Reference to the stream state
#[derive(Debug)]
pub(crate) struct StreamRef {
    pub opaque: OpaqueStreamRef,
    pub send_buffer: Arc<SendBuffer<Bytes>>,
}

impl StreamRef {
    pub fn new(
        inner: Arc<Mutex<Inner>>,
        ptr: &mut Ptr,
        send_buffer: Arc<SendBuffer<Bytes>>,
    ) -> Self {
        StreamRef {
            opaque: OpaqueStreamRef::new(inner, ptr),
            send_buffer,
        }
    }

    pub fn take_request(&self) -> Request {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        let mut stream = me.store.resolve(self.opaque.key);
        me.actions
            .recv
            .take_request(&mut stream)
    }

    pub fn push_request(
        &mut self,
        mut request: Request,
    ) -> Result<StreamRef, crate::codec::SendError> {
        use super::{send::Send, stream::Stream};
        use crate::message::IntoPseudo;
        crate::frame::PushPromise::validate_request(&request)
            .map_err(|_| UserError::MalformedHeaders)?;
        Send::check_headers(request.headers())?;
        if request.take_body().is_some() || request.take_trailers().is_some() {
            return Err(UserError::MalformedHeaders.into());
        }
        let (head, fields) = request.into_message_head();
        if head.extension().is_some()
            || head
                .uri()
                .authority()
                .is_none_or(str::is_empty)
            || head.uri().path().is_empty()
        {
            return Err(UserError::MalformedHeaders.into());
        }
        let is_head = head.method() == &header_plz::Method::HEAD;
        let pseudo = head.into_pseudo();
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        me.actions.ensure_no_conn_error()?;
        me.actions.send.ensure_push_enabled()?;
        if !me.counts.role().is_server() {
            return Err(UserError::UnexpectedFrameType.into());
        }
        let parent = me.store.resolve(self.opaque.key);
        // Promises require an open or half-closed (remote) client stream.
        if !parent.id.is_client_initiated()
            || parent.state.is_send_closed()
            || parent.state.is_idle()
            || parent.state.is_scheduled_reset()
        {
            return Err(UserError::InactiveStreamId.into());
        }
        let parent_id = parent.id;
        let id = me.actions.send.open()?;
        let mut stream = Stream::new(
            id,
            me.actions.send.init_window_sz(),
            me.actions.recv.init_window_sz(),
        );
        stream.state.reserve_local()?;
        stream.is_push_head = is_head;
        let mut stream = me.store.insert(id, stream);
        me.refs += 1;
        let result = StreamRef::new(
            self.opaque.inner.clone(),
            &mut stream,
            self.send_buffer.clone(),
        );
        me.actions.send.queue_push(
            crate::frame::PushPromise::new(parent_id, id, pseudo, fields),
            &mut stream,
            &mut me.actions.task,
        );
        Ok(result)
    }

    pub fn send_response(
        &mut self,
        response: Response,
    ) -> Result<(), UserError> {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        let stream = me.store.resolve(self.opaque.key);
        let actions = &mut me.actions;
        let span = span!(Level::TRACE, "send response|", "{:?}| ", stream.id);
        let _enter = span.enter();

        if stream.state.is_remote_reset() {
            trace!("remote reset");
            if let Some(task) = actions.task.take() {
                task.wake()
            }
            return Err(UserError::InactiveStreamId);
        }

        let mut send_buffer = self.send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        let mut response_frames = TwoTwoFrame::from((stream.id, response));
        let data_frame = response_frames.take_data();
        let trailer_frame = response_frames.take_trailer();
        if stream.is_push_head
            && (data_frame.is_some() || trailer_frame.is_some())
        {
            return Err(UserError::MalformedHeaders);
        }

        me.counts
            .transition(stream, |counts, stream| {
                actions.send.send_headers(
                    response_frames.header,
                    send_buffer,
                    stream,
                    counts,
                    &mut actions.task,
                )
            })?;
        trace!("added| header");

        let mut stream = me.store.resolve(self.opaque.key);

        queue_body_trailer(
            &mut stream,
            data_frame,
            trailer_frame,
            send_buffer,
        );

        trace!("response queued");
        Ok(())
    }

    pub fn send_response_streaming(
        &mut self,
        response: Response,
        end_stream: bool,
    ) -> Result<(), crate::codec::SendError> {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        me.actions.ensure_no_conn_error()?;
        me.actions
            .send
            .ensure_streaming_supported()?;
        let stream = me.store.resolve(self.opaque.key);
        if stream.is_push_head && !end_stream {
            return Err(UserError::MalformedHeaders.into());
        }
        let mut frames = TwoTwoFrame::from((stream.id, response));
        if frames.take_data().is_some() || frames.take_trailer().is_some() {
            return Err(UserError::UnexpectedFrameType.into());
        }
        if !end_stream {
            frames.header.unset_end_stream();
        }
        let mut buffer = self.send_buffer.inner.lock().unwrap();
        me.counts
            .transition(stream, |counts, stream| {
                me.actions.send.send_headers(
                    frames.header,
                    &mut buffer,
                    stream,
                    counts,
                    &mut me.actions.task,
                )?;
                if !end_stream {
                    me.actions
                        .send
                        .start_streaming(stream.id);
                }
                Ok::<_, UserError>(())
            })?;
        Ok(())
    }

    /// Advances `data` as bounded chunks are queued. Pending callers must retain
    /// the remaining bytes and poll again with the same END_STREAM value.
    pub fn poll_send_data(
        &mut self,
        cx: &mut std::task::Context<'_>,
        data: &mut Bytes,
        end_stream: bool,
    ) -> std::task::Poll<Result<(), crate::codec::SendError>> {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        me.actions.ensure_no_conn_error()?;
        let mut stream = me.store.resolve(self.opaque.key);
        let mut buffer = self.send_buffer.inner.lock().unwrap();
        me.actions
            .send
            .poll_send_data(
                cx,
                data,
                end_stream,
                &mut buffer,
                &mut stream,
                &mut me.actions.task,
            )
            .map(|result| result.map_err(Into::into))
    }

    /// Wait until this stream's queued DATA has been handed to the codec.
    pub fn poll_flush(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), crate::codec::SendError>> {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        me.actions.ensure_no_conn_error()?;
        let stream = me.store.resolve(self.opaque.key);
        me.actions
            .send
            .poll_flush(&stream, cx)
            .map(|result| result.map_err(Into::into))
    }

    pub fn send_trailers(
        &mut self,
        trailers: header_plz::HeaderMap,
    ) -> Result<(), crate::codec::SendError> {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        me.actions.ensure_no_conn_error()?;
        let mut stream = me.store.resolve(self.opaque.key);
        let mut buffer = self.send_buffer.inner.lock().unwrap();
        me.actions.send.send_trailers(
            trailers,
            &mut buffer,
            &mut stream,
            &mut me.actions.task,
        )?;
        Ok(())
    }

    pub fn abandon_send(&mut self) {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;
        let mut stream = me.store.resolve(self.opaque.key);
        if me
            .actions
            .send
            .is_streaming_send_open(stream.id)
            && !stream.state.is_send_closed()
        {
            me.actions.send.schedule_implicit_reset(
                &mut stream,
                Reason::CANCEL,
                &mut me.counts,
                &mut me.actions.task,
            );
        }
    }

    pub fn send_reset(&mut self, reason: Reason) {
        let mut me = self.opaque.inner.lock().unwrap();
        let me = &mut *me;

        let stream = me.store.resolve(self.opaque.key);
        let mut send_buffer = self.send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;

        match me.actions.send_reset(
            stream,
            reason,
            Initiator::User,
            &mut me.counts,
            send_buffer,
        ) {
            Ok(()) => (),
            Err(crate::proto::error::GoAway {
                ..
            }) => {
                // this should never happen, because Initiator::User resets do
                // not count toward the local limit.
                // we could perhaps make this state impossible, if we made the
                // initiator argument a generic, and so this could return
                // Infallible instead of an impossible GoAway, but oh well.
                unreachable!("Initiator::User should not error sending reset");
            }
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.opaque.stream_id()
    }
}

impl Clone for StreamRef {
    fn clone(&self) -> Self {
        StreamRef {
            opaque: self.opaque.clone(),
            send_buffer: self.send_buffer.clone(),
        }
    }
}
