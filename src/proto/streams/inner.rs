use crate::Codec;
use crate::frame;
use crate::frame::Reason;
use crate::frame::StreamId;
use crate::proto::MAX_WINDOW_SIZE;
use crate::proto::ProtoError;
use crate::proto::WindowSize;
use crate::proto::error::Initiator;
use crate::proto::streams::send_buffer::SendBuffer;
use crate::proto::{
    config::ConnectionConfig,
    streams::{
        Store,
        action::Actions,
        counts::Counts,
        recv::{Open, RecvHeaderBlockError},
        store::{Entry, Key, Resolve},
        stream::Stream,
    },
};
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use crate::role::Role;
use bytes::Bytes;
use tokio::io::AsyncWrite;
use tracing::Level;
use tracing::error;
use tracing::span;
use tracing::trace;

/// Fields needed to manage state related to managing the set of streams. This
/// is mostly split out to make ownership happy.
#[derive(Debug)]
pub struct Inner {
    /// Tracks send & recv stream concurrency.
    pub counts: Counts,

    /// Connection level state and performs actions on streams
    pub actions: Actions,

    /// Stores stream state
    pub store: Store,

    /// The number of stream refs to this shared state.
    pub refs: usize,
}

impl Inner {
    pub fn new(role: Role, config: ConnectionConfig) -> Arc<Mutex<Inner>> {
        Arc::new(Mutex::new(Inner {
            counts: Counts::new(role.clone(), &config),
            actions: Actions::new(role, config),
            store: Store::new(),
            refs: 1,
        }))
    }

    // ===== Data =====
    pub fn recv_data(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        frame: frame::Data,
    ) -> Result<(), ProtoError> {
        let id = frame.stream_id();
        let span = span!(Level::TRACE, "recv data| ", "{:?}| ", id);
        let _enter = span.enter();
        let stream = match self.store.find_mut(&id) {
            Some(stream) => stream,
            None => {
                // The GOAWAY process has begun. All streams with a greater ID
                // than specified as part of GOAWAY should be ignored.
                if id > self.actions.recv.max_stream_id() {
                    return Ok(());
                }

                if self
                    .actions
                    .is_forgotten_stream(&self.counts.role(), id)
                {
                    trace!("forgotten stream");
                    let sz = frame.payload().len();
                    // This should have been enforced at the codec::FramedRead
                    // layer, so this is just a sanity check.
                    assert!(sz <= MAX_WINDOW_SIZE as usize);
                    let sz = sz as WindowSize;
                    // consume connection flow control
                    self.actions
                        .recv
                        .check_connection_window_update = true;
                    self.actions
                        .recv
                        .dec_connection_window(sz)?;
                    return Err(ProtoError::library_reset(
                        id,
                        Reason::STREAM_CLOSED,
                    ));
                }

                return Err(ProtoError::library_go_away(
                    Reason::PROTOCOL_ERROR,
                ));
            }
        };

        let actions = &mut self.actions;
        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        let role = self.counts.role();

        self.counts
            .transition(stream, |counts, stream| {
                let res = actions
                    .recv
                    .recv_data(frame, stream, &role);
                if let Err(e) = res.as_ref() {
                    error!("{:?}", e);
                }
                actions.reset_on_recv_stream_err(
                    send_buffer,
                    stream,
                    counts,
                    res,
                )
            })
    }

    pub fn recv_push_promise(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        promise: frame::PushPromise,
    ) -> Result<(), ProtoError> {
        let parent_id = promise.stream_id();
        let id = promise.promised_id();
        let role = self.counts.role();
        if !role.is_client() || !self.actions.recv.is_push_enabled {
            return Err(ProtoError::library_go_away(Reason::PROTOCOL_ERROR));
        }
        if parent_id.is_zero() || !parent_id.is_client_initiated() {
            return Err(ProtoError::library_go_away(Reason::PROTOCOL_ERROR));
        }
        let cancel = if let Some(parent) = self.store.find_mut(&parent_id) {
            let cancel = parent.state.is_local_error();
            if (!cancel
                && !parent.state.is_recv_headers()
                && !parent.state.is_recv_streaming())
                || parent.state.is_idle()
            {
                return Err(ProtoError::library_go_away(
                    Reason::PROTOCOL_ERROR,
                ));
            }
            cancel
        } else if self
            .actions
            .is_forgotten_stream(&role, parent_id)
        {
            // Reset retention is bounded. A promise can still be in flight
            // after its parent has been forgotten; reserve and cancel it.
            true
        } else {
            return Err(ProtoError::library_go_away(Reason::PROTOCOL_ERROR));
        };
        if id.is_zero() || self.store.find_mut(&id).is_some() {
            return Err(ProtoError::library_go_away(Reason::PROTOCOL_ERROR));
        }
        self.actions.recv.can_open(
            id,
            Open::_PushPromise,
            &mut self.counts,
            &role,
        )?;
        // Bound reservations independently of active-stream concurrency. Include
        // promises handed to callers that have not received response headers.
        let mut reserved = 0;
        self.store.for_each(|stream| {
            if stream.is_remote_push
                && (stream.is_pending_accept
                    || (!stream.is_counted && !stream.state.is_closed()))
            {
                reserved += 1;
            }
        });
        if reserved >= 1024 {
            return Err(ProtoError::library_go_away(
                Reason::ENHANCE_YOUR_CALM,
            ));
        }
        let oversized = promise.is_over_size() || promise.is_malformed();
        let (pseudo, fields) = promise.into_parts();
        let valid_pseudo = pseudo
            .authority
            .as_ref()
            .is_some_and(|a| !a.is_empty())
            && pseudo.protocol.is_none();
        let request = crate::message::frames_to_request(pseudo, fields, id);
        let valid = !oversized
            && valid_pseudo
            && request.as_ref().is_ok_and(|r| {
                frame::PushPromise::validate_request(r).is_ok()
            });
        let mut stream = Stream::new(
            id,
            self.actions.send.init_window_sz(),
            self.actions.recv.init_window_sz(),
        );
        stream.state.reserve_remote()?;
        stream.is_remote_push = true;
        stream.streaming_recv = true;
        if valid && !cancel {
            let request = request.unwrap();
            if request.method() == &header_plz::Method::HEAD {
                stream.content_length = super::stream::ContentLength::Head;
            }
            stream.promised_request = Some(request);
            let mut stream = self.store.insert(id, stream);
            self.actions
                .recv
                .queue_push(&mut stream);
        } else {
            let stream = self.store.insert(id, stream);
            let mut buffer = send_buffer.inner.lock().unwrap();
            let actions = &mut self.actions;
            self.counts
                .transition(stream, |counts, stream| {
                    actions.send.send_reset(
                        if cancel {
                            Reason::CANCEL
                        } else {
                            Reason::PROTOCOL_ERROR
                        },
                        Initiator::Library,
                        stream,
                        &mut buffer,
                        counts,
                        &mut actions.task,
                    );
                    actions
                        .recv
                        .enqueue_reset_expiration(stream, counts);
                });
        }
        Ok(())
    }

    // ===== Headers =====
    pub fn recv_headers(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        frame: frame::Headers,
    ) -> Result<(), ProtoError> {
        let id = frame.stream_id();
        let span = span!(Level::TRACE, "recv headers| ", "{:?}| ", id);
        let _enter = span.enter();
        let role = self.counts.role();

        // The GOAWAY process has begun. All streams with a greater ID than
        // specified as part of GOAWAY should be ignored.
        if id > self.actions.recv.max_stream_id() {
            trace!("> max stream id| ignoring");
            return Ok(());
        }

        // Insert stream in store
        let key = match self.insert_or_create(id, &role)? {
            Some(key) => key,
            None => return Ok(()),
        };

        let stream = self.store.resolve(key);

        if stream.state.is_local_error() {
            // Locally reset streams must ignore frames "for some time".
            // This is because the remote may have sent trailers before
            // receiving the RST_STREAM frame.
            trace!("local_error");
            return Ok(());
        }

        let actions = &mut self.actions;
        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;

        self.counts
            .transition(stream, |counts, stream| {
                let res = if stream.state.is_recv_headers() {
                    match actions
                        .recv
                        .recv_headers(frame, stream, counts)
                    {
                        Ok(()) => Ok(()),
                        Err(RecvHeaderBlockError::Oversize(resp)) => {
                            error!("header oversized");
                            // server => 431 error
                            if let Some(resp) = resp {
                                let sent = actions.send.send_headers(
                                    resp,
                                    send_buffer,
                                    stream,
                                    counts,
                                    &mut actions.task,
                                );
                                debug_assert!(
                                    sent.is_ok(),
                                    "oversize response should not fail"
                                );
                                actions.send.schedule_implicit_reset(
                                    stream,
                                    Reason::PROTOCOL_ERROR,
                                    counts,
                                    &mut actions.task,
                                );
                                actions
                                    .recv
                                    .enqueue_reset_expiration(stream, counts);
                                Ok(())
                            } else {
                                // client => ProtoError
                                Err(ProtoError::library_reset(
                                    stream.id,
                                    Reason::PROTOCOL_ERROR,
                                ))
                            }
                        }
                        Err(RecvHeaderBlockError::State(e)) => Err(e),
                    }
                } else {
                    trace!("trailers");
                    // Trailers
                    if !frame.is_end_stream() {
                        // Receiving trailers that don't set EOS is a "malformed"
                        // message. Malformed messages are a stream error.
                        error!("trailers without EOS");
                        return Err(ProtoError::library_reset(
                            stream.id,
                            Reason::PROTOCOL_ERROR,
                        ));
                    }
                    actions
                        .recv
                        .recv_trailers(frame, stream, &role)
                };
                actions.reset_on_recv_stream_err(
                    send_buffer,
                    stream,
                    counts,
                    res,
                )
            })
    }

    #[inline(always)]
    fn insert_or_create(
        &mut self,
        id: StreamId,
        role: &Role,
    ) -> Result<Option<Key>, ProtoError> {
        let key = match self.store.find_entry(id) {
            Entry::Occupied(entry) => {
                trace!("occupied");
                entry.key()
            }
            Entry::Vacant(entry) => {
                trace!("vacant");
                // Client: it's possible to send a request, and then send
                // a RST_STREAM while the response HEADERS were in transit.
                //
                // Server: we can't reset a stream before having received
                // the request headers, so don't allow.
                if !role.is_server() {
                    // This may be response headers for a stream we've already
                    // forgotten about...
                    if self
                        .actions
                        .is_forgotten_stream(role, id)
                    {
                        error!("insert_or_create| forgotten stream");
                        return Err(ProtoError::library_reset(
                            id,
                            Reason::STREAM_CLOSED,
                        ));
                    }
                }

                // check if the stream Id is the exepected and within the limit
                // of total no of recv streams
                match self.actions.recv.can_open(
                    id,
                    Open::Headers,
                    &mut self.counts,
                    role,
                )? {
                    Some(stream_id) => {
                        trace!("open| Y");
                        let stream = Stream::new(
                            stream_id,
                            self.actions.send.init_window_sz(),
                            self.actions.recv.init_window_sz(),
                        );
                        entry.insert(stream)
                    }
                    None => {
                        trace!("open| N");
                        return Ok(None);
                    }
                }
            }
        };
        Ok(Some(key))
    }

    // ===== Reset =====
    pub fn recv_reset(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        frame: frame::Reset,
    ) -> Result<(), ProtoError> {
        let id = frame.stream_id();
        let span = span!(Level::TRACE, "recv reset| ", "{:?}| ", id);
        let _enter = span.enter();
        if id.is_zero() {
            error!("invalid stream ID 0");
            return Err(ProtoError::library_go_away(Reason::PROTOCOL_ERROR));
        }

        // The GOAWAY process has begun. All streams with a greater ID than
        // specified as part of GOAWAY should be ignored.
        if id > self.actions.recv.max_stream_id() {
            trace!("> max stream id| ignoring");
            return Ok(());
        }

        let stream = match self.store.find_mut(&id) {
            Some(stream) => stream,
            None => {
                // TODO(hyper): Are there other error cases?
                self.actions
                    .ensure_not_idle(self.counts.role(), id)
                    .map_err(ProtoError::library_go_away)?;
                return Ok(());
            }
        };

        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        let actions = &mut self.actions;

        self.counts
            .transition(stream, |counts, stream| {
                // notify stream
                actions
                    .recv
                    .recv_reset(frame, stream, counts)?;

                // clear stream queue
                // reclaim capacity
                actions
                    .send
                    .handle_error(send_buffer, stream, counts);
                assert!(stream.state.is_closed());
                Ok(())
            })
    }

    pub fn send_reset(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        id: StreamId,
        reason: Reason,
    ) -> Result<(), crate::proto::error::GoAway> {
        let span = span!(Level::TRACE, "send reset| ", "{:?}| ", id);
        let _enter = span.enter();
        let key = match self.store.find_entry(id) {
            Entry::Occupied(e) => e.key(),
            Entry::Vacant(e) => {
                trace!("vacant");
                // Resetting a stream we don't know about? That could be OK...
                //
                // 1. As a server, we just received a request, but that request
                //    was bad, so we're resetting before even accepting it.
                //    This is totally fine.
                //
                // 2. The remote may have sent us a frame on new stream that
                //    it's *not* supposed to have done, and thus, we don't know
                //    the stream. In that case, sending a reset will "open" the
                //    stream in our store. Maybe that should be a connection
                //    error instead? At least for now, we need to update what
                //    our vision of the next stream is.
                if self.counts.role.is_local_init(id) {
                    // We normally would open this stream, so update our
                    // next-send-id record.
                    self.actions
                        .send
                        .maybe_reset_next_stream_id(id);
                } else {
                    // We normally would recv this stream, so update our
                    // next-recv-id record.
                    self.actions
                        .recv
                        .maybe_reset_next_stream_id(id);
                }
                let stream = Stream::new(id, 0, 0);
                e.insert(stream)
            }
        };

        let stream = self.store.resolve(key);
        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        self.actions.send_reset(
            stream,
            reason,
            Initiator::Library,
            &mut self.counts,
            send_buffer,
        )
    }

    // ===== Goaway =====
    pub fn recv_go_away(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        frame: &frame::GoAway,
    ) -> Result<(), ProtoError> {
        let actions = &mut self.actions;
        let counts = &mut self.counts;
        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;

        let last_stream_id = frame.last_stream_id();

        actions
            .send
            .recv_go_away(last_stream_id)?;

        let err = ProtoError::remote_go_away(
            frame.debug_data().clone(),
            frame.reason(),
        );

        self.store.for_each(|stream| {
            // GOAWAY's cutoff refers only to streams initiated by us, not
            // remotely initiated pushed responses that can finish draining.
            if counts.role().is_local_init(stream.id)
                && stream.id > last_stream_id
            {
                counts.transition(stream, |counts, stream| {
                    // notify receivers
                    actions
                        .recv
                        .handle_error(&err, &mut *stream);
                    // clear pending buffer
                    actions
                        .send
                        .handle_error(send_buffer, stream, counts);
                })
            }
        });
        actions.conn_error = Some(err);
        Ok(())
    }

    // ===== WINDOW UPDATE =====
    pub fn recv_window_update(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        frame: frame::WindowUpdate,
    ) -> Result<(), ProtoError> {
        let id = frame.stream_id();
        let size = frame.size_increment();

        let span = span!(Level::TRACE, "recv window update| ", "{:?}| ", id);
        let _enter = span.enter();

        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;

        if id.is_zero() {
            self.actions
                .send
                .recv_connection_window_update(
                    size,
                    &mut self.store,
                    &mut self.counts,
                )
                .map_err(ProtoError::library_go_away)?;
        } else {
            // The remote may send window updates for streams that the local now
            // considers closed. It's ok...
            if let Some(mut stream) = self.store.find_mut(&id) {
                let res = self
                    .actions
                    .send
                    .recv_stream_window_update(
                        size,
                        send_buffer,
                        &mut stream,
                        &mut self.counts,
                        &mut self.actions.task,
                    )
                    .map_err(|reason| ProtoError::library_reset(id, reason));

                return self.actions.reset_on_recv_stream_err(
                    send_buffer,
                    &mut stream,
                    &mut self.counts,
                    res,
                );
            } else {
                trace!("vacant");
                self.actions
                    .ensure_not_idle(self.counts.role(), id)
                    .map_err(ProtoError::library_go_away)?;
            }
        }

        Ok(())
    }

    // ===== Misc =====
    pub fn handle_error(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        err: ProtoError,
    ) -> StreamId {
        let actions = &mut self.actions;
        let counts = &mut self.counts;
        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;

        let last_processed_id = actions.recv.last_processed_id();

        self.store.for_each(|stream| {
            counts.transition(stream, |counts, stream| {
                // notify receivers
                actions
                    .recv
                    .handle_error(&err, &mut *stream);
                // TODO: should reclaim capacity ?
                // clear pending buffer
                actions
                    .send
                    .handle_error(send_buffer, stream, counts);
            })
        });
        if counts.role().is_client() {
            actions.clear_queues(true, &mut self.store, counts);
        }
        actions.conn_error = Some(err);
        last_processed_id
    }

    // ===== EOF =====
    pub fn recv_eof(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        clear_pending_accept: bool,
    ) -> Result<(), ()> {
        let actions = &mut self.actions;
        let counts = &mut self.counts;
        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        if actions.conn_error.is_none() {
            actions.conn_error = Some(
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "connection closed because of a broken pipe",
                )
                .into(),
            );
        }

        self.store.for_each(|stream| {
            counts.transition(stream, |counts, stream| {
                // notify
                actions.recv.recv_eof(stream);

                // clear queues and reclaim capacity
                actions
                    .send
                    .handle_error(send_buffer, stream, counts);
            })
        });

        // clear send and recv queues
        actions.clear_queues(
            clear_pending_accept || counts.role().is_client(),
            &mut self.store,
            counts,
        );
        Ok(())
    }

    // ===== Poll ======
    pub fn poll_window_update<T>(
        &mut self,
        cx: &mut Context,
        dst: &mut Codec<T, Bytes>,
    ) -> Poll<std::io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        if !self
            .actions
            .recv
            .check_connection_window_update
        {
            return Poll::Ready(Ok(()));
        }

        if let Some(size) = self.should_send_connection_window_update() {
            trace!("polling conn window update");
            ready!(dst.poll_ready(cx))?;
            let frame = frame::WindowUpdate::new(StreamId::ZERO, size);
            dst.buffer(frame.into())
                .expect("invalid WINDOW_UPDATE frame");
            let _ = self
                .actions
                .recv
                .inc_connection_window(size);
        }

        while let Some((stream_id, size)) =
            self.should_send_stream_window_update()
        {
            trace!("polling stream window update| {:?}", stream_id);
            ready!(dst.poll_ready(cx))?;
            let frame = frame::WindowUpdate::new(stream_id, size);
            dst.buffer(frame.into())
                .expect("invalid WINDOW_UPDATE frame");
            if let Some(mut stream) = self.store.find_mut(&stream_id) {
                let _ = stream.recv_flow.inc_window(size);
            } else {
                error!("send window update not found| {:?}  ", stream_id);
            }
        }

        self.actions
            .recv
            .check_connection_window_update = false;
        self.actions
            .recv
            .check_stream_window_update = None;

        Poll::Ready(Ok(()))
    }

    pub fn should_send_connection_window_update(
        &mut self,
    ) -> Option<WindowSize> {
        self.actions
            .recv
            .should_send_connection_window_update()
            .map(|s| s as WindowSize)
    }

    pub fn should_send_stream_window_update(
        &mut self,
    ) -> Option<(StreamId, WindowSize)> {
        let mut update = None;
        let _ = self.store.try_for_each(|stream| {
            if update.is_none() && !stream.state.is_recv_end_stream() {
                update = stream
                    .recv_flow
                    .should_send_window_update()
                    .map(|size| (stream.id, size));
            }
            Ok::<_, ()>(())
        });
        update
    }

    pub fn poll_complete<T>(
        &mut self,
        send_buffer: &SendBuffer<Bytes>,
        cx: &mut Context,
        dst: &mut Codec<T, Bytes>,
    ) -> Poll<std::io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        let mut send_buffer = send_buffer.inner.lock().unwrap();
        let send_buffer = &mut *send_buffer;
        trace!("polling streams");
        ready!(self.actions.send.poll_complete(
            cx,
            send_buffer,
            &mut self.store,
            &mut self.counts,
            dst,
        ))?;

        // Nothing else to do, track the task
        trace!("task parked");
        self.actions.task = Some(cx.waker().clone());
        Poll::Ready(Ok(()))
    }
}
