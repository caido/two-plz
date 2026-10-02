use bytes::Bytes;
use header_plz::{HeaderMap, const_headers};
use tokio::io::AsyncWrite;
use tracing::error;
use tracing::trace;
use tracing::trace_span;

use crate::Codec;
use crate::codec::UserError;
use crate::frame;
use crate::frame::DEFAULT_INITIAL_WINDOW_SIZE;
use crate::frame::Frame;
use crate::frame::Reason;
use crate::proto::ProtoError;
use crate::proto::config::ConnectionConfig;
use crate::proto::error::Initiator;
use crate::proto::ping_pong::PingHandler;
use crate::proto::streams::Resolve;
use crate::proto::streams::Store;
use crate::proto::streams::buffer::Buffer;
use crate::proto::streams::counts::Counts;
use crate::proto::streams::flow_control::FlowControl;
use crate::proto::streams::store::Ptr;
use crate::proto::streams::store::Queue;
use crate::proto::streams::stream;
use crate::spa::SpaTracker;
use std::cmp::Ordering;
use std::cmp::min;
use std::io;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::{frame::StreamId, frame::StreamIdOverflow, proto::WindowSize};

#[derive(Debug)]
pub struct Send {
    /// Initial window size of locally initiated streams
    init_stream_window_sz: WindowSize,

    /// connection level
    flow: FlowControl,

    /// Stream ID of the last stream opened.
    last_opened_id: StreamId,

    /// Any streams with a higher ID are ignored.
    ///
    /// This starts as MAX, but is lowered when a GOAWAY is received.
    ///
    /// > After sending a GOAWAY frame, the sender can discard frames for
    /// > streams initiated by the receiver with identifiers higher than
    /// > the identified last stream.
    max_stream_id: StreamId,

    // TODO: make this configurable
    // hyper Builder::StreamId
    /// Stream identifier to use for next initialized stream.
    pub next_stream_id: Result<StreamId, StreamIdOverflow>,

    /// Queue of streams waiting for socket capacity to send a frame.
    pending_send: Queue<stream::NextSend>,

    /// Queue of streams waiting for window capacity to produce data.
    pending_capacity: Queue<stream::NextSendCapacity>,

    /// Queue of streams waiting for capacity due to max concurrency
    pending_open: Queue<stream::NextOpen>,

    is_push_enabled: bool,
    is_extended_connect_protocol_enabled: bool,
    /// spa
    spa_tracker: Option<SpaTracker>,
}

impl Send {
    pub fn new(config: &mut ConnectionConfig) -> Send {
        Send {
            init_stream_window_sz: config
                .peer_settings
                .initial_window_size()
                .unwrap_or(DEFAULT_INITIAL_WINDOW_SIZE),
            flow: FlowControl::new(DEFAULT_INITIAL_WINDOW_SIZE),
            last_opened_id: StreamId::ZERO,
            max_stream_id: StreamId::MAX,
            next_stream_id: Ok(1.into()),
            pending_send: Queue::new(),
            pending_capacity: Queue::new(),
            pending_open: Queue::new(),
            is_push_enabled: config
                .peer_settings
                .is_push_enabled()
                .unwrap_or_default(),
            is_extended_connect_protocol_enabled: config
                .peer_settings
                .is_extended_connect_protocol_enabled()
                .unwrap_or_default(),
            spa_tracker: config.spa_tracker.take(),
        }
    }

    /// Queue a frame to be sent to the remote
    pub fn queue_frame(
        &mut self,
        frame: Frame,
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
        task: &mut Option<Waker>,
    ) {
        trace!("queue frame| {:?}", stream.id);
        stream
            .pending_send
            .push_back(buffer, frame);
        self.schedule_send(stream, task);
    }

    /// Schedule a stream to be sent
    pub fn schedule_send(
        &mut self,
        stream: &mut Ptr,
        task: &mut Option<Waker>,
    ) {
        // If the stream is waiting to be opened, nothing more to do.
        if stream.is_send_ready() {
            trace!("schedule send| {:?}", stream.id);
            // Queue the stream
            self.pending_send.push(stream);
            if let Some(task) = task.take() {
                task.wake();
            }
        }
    }

    // ===== Headers =====
    pub fn send_headers(
        &mut self,
        frame: frame::Headers,
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
        counts: &mut Counts,
        task: &mut Option<Waker>,
    ) -> Result<(), UserError> {
        Self::check_headers(frame.fields())?;
        let end_stream = frame.is_end_stream();

        // Update the state
        stream.state.send_open(end_stream)?;

        let mut pending_open = false;
        if counts
            .role()
            .is_local_init(frame.stream_id())
        // TODO(pp)
        //&& !stream.is_pending_push
        {
            pending_open = true;
            self.pending_open.push(stream);
        }

        // Queue the frame for sending
        //
        // This call expects that, since new streams are in the open queue, new
        // streams won't be pushed on pending_send.
        self.queue_frame(frame.into(), buffer, stream, task);

        // Need to notify the connection when pushing onto pending_open since
        // queue_frame only notifies for pending_send.
        if pending_open && let Some(task) = task.take() {
            task.wake();
        }
        Ok(())
    }

    fn check_headers(fields: &HeaderMap) -> Result<(), UserError> {
        // 8.1.2.2. Connection-Specific Header Fields
        if fields.has_key(const_headers::CONNECTION)
            || fields.has_key(const_headers::TRANSFER_ENCODING)
            || fields.has_key(const_headers::UPGRADE)
            || fields.has_key("keep-alive")
            || fields.has_key("proxy-connection")
        {
            tracing::debug!("illegal connection-specific headers found");
            return Err(UserError::MalformedHeaders);
        } else if let Some(te) = fields.value_of_key(const_headers::TE)
            && te != "trailers".as_bytes()
        {
            tracing::debug!("illegal connection-specific headers found");
            return Err(UserError::MalformedHeaders);
        }
        Ok(())
    }

    // ===== reset =====
    pub fn send_reset(
        &mut self,
        reason: Reason,
        initiator: Initiator,
        stream: &mut Ptr,
        buffer: &mut Buffer<Frame<Bytes>>,
        counts: &mut Counts,
        task: &mut Option<Waker>,
    ) {
        let is_reset = stream.state.is_reset();
        let is_closed = stream.state.is_closed();
        let is_empty = stream.pending_send.is_empty();

        if is_reset {
            // Don't double reset
            trace!("already reset");
            return;
        }

        // Transition the state to reset no matter what.
        stream.set_reset(reason, initiator);

        // If closed AND the send queue is flushed, then the stream cannot be
        // reset explicitly, either. Implicit resets can still be queued.
        if is_closed && is_empty {
            trace!("already closed",);
            return;
        }

        // Clear all pending outbound frames.
        self.clear_stream_queue(buffer, stream);

        // add reset to the send queue
        trace!("send reset| {:?}", stream.id);
        let frame = frame::Reset::new(stream.id, reason);
        self.queue_frame(frame.into(), buffer, stream, task);
        self.reclaim_all_capacity(stream, counts);
    }

    pub fn schedule_implicit_reset(
        &mut self,
        stream: &mut Ptr,
        reason: Reason,
        counts: &mut Counts,
        task: &mut Option<Waker>,
    ) {
        trace!("scheduled reset| {:?}", stream.id);
        if stream.state.is_closed() {
            // Stream is already closed, nothing more to do
            return;
        }
        stream.state.set_scheduled_reset(reason);
        self.reclaim_all_capacity(stream, counts);
        self.schedule_send(stream, task);
    }

    // ===== settings =====
    pub fn apply_remote_settings(
        &mut self,
        settings: &frame::Settings,
        buffer: &mut Buffer<Frame<Bytes>>,
        store: &mut Store,
        counts: &mut Counts,
        task: &mut Option<Waker>,
    ) -> Result<(), ProtoError> {
        if let Some(val) = settings.is_push_enabled() {
            self.is_push_enabled = val
        }

        if let Some(val) = settings.is_extended_connect_protocol_enabled() {
            self.is_extended_connect_protocol_enabled = val;
        }

        // Applies an update to the remote endpoint's initial window size.
        //
        // Per RFC 7540 §6.9.2:
        //
        // In addition to changing the flow-control window for streams that are
        // not yet active, a SETTINGS frame can alter the initial flow-control
        // window size for streams with active flow-control windows (that is,
        // streams in the "open" or "half-closed (remote)" state). When the
        // value of SETTINGS_INITIAL_WINDOW_SIZE changes, a receiver MUST adjust
        // the size of all stream flow-control windows that it maintains by the
        // difference between the new value and the old value.
        //
        // A change to `SETTINGS_INITIAL_WINDOW_SIZE` can cause the available
        // space in a flow-control window to become negative. A sender MUST
        // track the negative flow-control window and MUST NOT send new
        // flow-controlled frames until it receives WINDOW_UPDATE frames that
        // cause the flow-control window to become positive.
        if let Some(new) = settings.initial_window_size() {
            let old = self.init_stream_window_sz;
            self.init_stream_window_sz = new;

            match new.cmp(&old) {
                Ordering::Less => {
                    // decrease the (remote) window on every open stream.
                    let dec = old - new;
                    let mut total_reclaimed = 0;
                    store.try_for_each(|mut stream| {
                        let stream = &mut *stream;
                        if stream.state.is_send_closed()
                        // TODO(ws)
                        // && stream.buffered_send_data == 0 {
                        {
                            return Ok(());
                        }
                        stream
                            .send_flow
                            .dec_window(dec)
                            .map_err(ProtoError::library_go_away)?;

                        // It's possible that decreasing the window causes
                        // `window_size` (the stream-specific window) to fall
                        // below `allocated` (the portion of the
                        // connection-level window that we have allocated to
                        // the stream). In this case, we should take that
                        // excess allocation away and reassign it to other
                        // streams.
                        let current_send_flow = stream.send_flow.available();
                        let allocated = stream.connection_window_allocated;
                        if allocated > current_send_flow {
                            let extra = allocated - current_send_flow;
                            total_reclaimed += extra;
                            stream.connection_window_allocated -= extra;
                        }
                        Ok::<_, ProtoError>(())
                    })?;
                    if total_reclaimed > 0 {
                        let _ = self.recv_connection_window_update(
                            total_reclaimed,
                            store,
                            counts,
                        );
                    }
                }
                Ordering::Greater => {
                    let inc = new - old;
                    store.try_for_each(|mut stream| {
                        self.recv_stream_window_update(
                            inc,
                            buffer,
                            &mut stream,
                            counts,
                            task,
                        )
                        .map_err(ProtoError::library_go_away)
                    })?;
                }
                Ordering::Equal => (),
            }
        }

        Ok(())
    }

    // ===== GoAway =====
    pub(super) fn recv_go_away(
        &mut self,
        last_stream_id: StreamId,
    ) -> Result<(), ProtoError> {
        if last_stream_id > self.max_stream_id {
            // The remote endpoint sent a `GOAWAY` frame indicating a stream
            // that we never sent, or that we have already terminated on account
            // of previous `GOAWAY` frame. In either case, that is illegal.
            // (When sending multiple `GOAWAY`s, "Endpoints MUST NOT increase
            // the value they send in the last stream identifier, since the
            // peers might already have retried unprocessed requests on another
            // connection.")
            error!(
                "recv_go_away| last_stream_id ({:?}) > max_stream_id ({:?})",
                last_stream_id, self.max_stream_id,
            );
            return Err(ProtoError::library_go_away(Reason::PROTOCOL_ERROR));
        }

        self.max_stream_id = last_stream_id;
        Ok(())
    }

    // ===== window update =====
    pub fn recv_connection_window_update(
        &mut self,
        inc: WindowSize,
        store: &mut Store,
        counts: &mut Counts,
    ) -> Result<(), Reason> {
        self.flow.inc_window(inc)?;
        self.assign_connection_capacity(store, counts);
        Ok(())
    }

    pub fn assign_connection_capacity<R>(
        &mut self,
        store: &mut R,
        counts: &mut Counts,
    ) where
        R: Resolve,
    {
        while self.flow.available() > 0 {
            let stream = match self.pending_capacity.pop(store) {
                Some(stream) => stream,
                None => return,
            };

            // Streams pending capacity may have been reset before capacity
            // became available. In that case, the stream won't want any
            // capacity, and so we shouldn't "transition" on it, but just evict
            // it and continue the loop.
            if !stream.state.is_send_streaming()
                || stream.remaining_data_len.is_none()
            {
                continue;
            }

            counts.transition(stream, |_, stream| {
                // Try to assign capacity to the stream. This will also
                // re-queue the stream if there isn't enough connection level
                // capacity to fulfill the capacity request.
                self.try_assign_capacity(stream);
            })
        }
    }

    fn try_assign_capacity(&mut self, stream: &mut Ptr) {
        let span = trace_span!("try assign capacity| ", ?stream.id);
        let _ = span.enter();
        // Window updates can arrive before a locally initiated stream opens.
        // Opening schedules its headers; data capacity is allocated after that.
        if stream.is_pending_open {
            return;
        }
        let stream_available = stream.send_flow.available();
        trace!("stream flow| {stream_available}");

        // check stream capacity
        // when, connection window update
        if stream_available == 0 {
            trace!("stream flow unavailable");
            return;
        }

        // check connection capacity on
        // 1. stream window update
        // 2. settings initial frame size change
        if self.flow.available() == 0 {
            trace!("conn flow unavailable");
            self.pending_capacity.push(stream);
            return;
        }

        let remaining_data_len = if let Some(rem) = stream.remaining_data_len {
            if self.spa_tracker.is_some() {
                rem.saturating_sub(1)
            } else {
                rem
            }
        } else {
            trace!("no remaining data");
            return;
        };
        trace!("remaining| {remaining_data_len}");

        //     | connection_flow_available
        // min | stream_flow_available
        //     | remaining_data_len
        let allocated = min(
            min(self.flow.available(), stream_available),
            remaining_data_len as u32,
        );

        trace!("allocated| {allocated}");
        self.pending_send.push(stream);

        // no flow is needed, possibly empty data frame ?
        if allocated == 0 {
            return;
        }

        // increase stream allocated connection window
        stream.connection_window_allocated += allocated;
        // reduce connection window
        let _ = self.flow.dec_window(allocated);
    }

    pub fn recv_stream_window_update(
        &mut self,
        sz: WindowSize,
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
        counts: &mut Counts,
        task: &mut Option<Waker>,
    ) -> Result<(), Reason> {
        if stream.state.is_send_closed() {
            return Ok(());
        }

        self.spa_recv_window_update(stream.id);

        if let Err(e) = stream.send_flow.inc_window(sz) {
            self.send_reset(
                Reason::FLOW_CONTROL_ERROR,
                Initiator::Library,
                stream,
                buffer,
                counts,
                task,
            );

            return Err(e);
        }
        self.try_assign_capacity(stream);
        Ok(())
    }

    fn reclaim_all_capacity(
        &mut self,
        stream: &mut Ptr<'_>,
        counts: &mut Counts,
    ) {
        let allocated = stream.connection_window_allocated;
        if allocated > 0 {
            let _ = self.recv_connection_window_update(
                allocated,
                stream.store_mut(),
                counts,
            );
        }
    }

    // ===== Misc ====
    pub fn init_window_sz(&self) -> WindowSize {
        self.init_stream_window_sz
    }

    pub fn open(&mut self) -> Result<StreamId, UserError> {
        let stream_id = self.ensure_next_stream_id()?;
        self.next_stream_id = stream_id.next_id();
        Ok(stream_id)
    }

    pub fn ensure_next_stream_id(&self) -> Result<StreamId, UserError> {
        self.next_stream_id
            .map_err(|_| UserError::OverflowedStreamId)
    }

    pub fn handle_error(
        &mut self,
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
        counts: &mut Counts,
    ) {
        self.clear_stream_queue(buffer, stream);
        self.reclaim_all_capacity(stream, counts);
    }

    // closes non existent ID's
    pub fn maybe_reset_next_stream_id(&mut self, id: StreamId) {
        if let Ok(next_id) = self.next_stream_id {
            // role::is_local_init should have been called beforehand
            debug_assert_eq!(
                id.is_server_initiated(),
                next_id.is_server_initiated()
            );
            if id >= next_id {
                self.next_stream_id = id.next_id();
            }
        }
    }

    pub(crate) fn is_extended_connect_protocol_enabled(&self) -> bool {
        self.is_extended_connect_protocol_enabled
    }

    /// ===== Clear =====
    pub fn clear_queues(&mut self, store: &mut Store, counts: &mut Counts) {
        self.clear_pending_capacity(store, counts);
        self.clear_pending_send(store, counts);
        self.clear_pending_open(store, counts);
    }

    pub fn clear_pending_capacity(
        &mut self,
        store: &mut Store,
        counts: &mut Counts,
    ) {
        let span = tracing::trace_span!("clear_pending_capacity");
        let _e = span.enter();
        while let Some(stream) = self.pending_capacity.pop(store) {
            counts.transition(stream, |_, stream| {
                tracing::trace!(?stream.id, "clear_pending_capacity");
            })
        }
    }

    pub fn clear_pending_send(
        &mut self,
        store: &mut Store,
        counts: &mut Counts,
    ) {
        while let Some(mut stream) = self.pending_send.pop(store) {
            let is_pending_reset = stream.is_pending_reset_expiration();
            if let Some(reason) = stream.state.get_scheduled_reset() {
                stream.set_reset(reason, Initiator::Library);
            }
            counts.transition_after(stream, is_pending_reset);
        }
    }

    pub fn clear_pending_open(
        &mut self,
        store: &mut Store,
        counts: &mut Counts,
    ) {
        while let Some(stream) = self.pending_open.pop(store) {
            let is_pending_reset = stream.is_pending_reset_expiration();
            counts.transition_after(stream, is_pending_reset);
        }
    }

    /// Clear the send queue for a stream
    pub fn clear_stream_queue(
        &mut self,
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
    ) {
        let span = trace_span!("clear_queue| ", ?stream.id);
        let _e = span.enter();
        while let Some(frame) = stream.pending_send.pop_front(buffer) {
            trace!("dropping| {:?}", frame);
        }
        stream.remaining_data_len = None;
    }

    // ===== polling =====
    fn pop_pending_open<'s>(
        &mut self,
        store: &'s mut Store,
        counts: &mut Counts,
    ) -> Option<Ptr<'s>> {
        // check for any pending open streams
        if counts.can_inc_num_send_streams()
            && let Some(mut stream) = self.pending_open.pop(store)
        {
            trace!("pop pending open| {:?}", stream.id);
            counts.inc_num_send_streams(&mut stream);
            // TODO(ws)
            //stream.notify_send();
            return Some(stream);
        }
        None
    }

    fn try_allocate_stream_flow(
        &mut self,
        stream: &mut Ptr,
        rem: usize,
        available: u32,
    ) -> bool {
        // check allocated conn flow
        if !(stream.connection_window_allocated == 0 && rem > 0) {
            return true;
        }

        // try to get more capacity
        let allocated = min(min(self.flow.available(), available), rem as u32);
        if allocated == 0 {
            return false;
        }
        stream.connection_window_allocated += allocated;
        let _ = self.flow.dec_window(allocated);
        true
    }

    fn set_data_frame_eos(
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
        to_ret: &mut frame::Data<Bytes>,
        remaining: Option<frame::Data<Bytes>>,
    ) {
        if stream.remaining_data_len.is_none() {
            trace!("data| completed");
            if stream.pending_send.is_empty() {
                stream.state.send_close();
                trace!("data| eos");
                to_ret.set_end_stream(true);
            } else {
                trace!("trailer| remaining");
                stream.is_sending_trailer = true;
            }
        } else if let Some(frame) = remaining {
            trace!("data| remaining");
            stream
                .pending_send
                .push_front(buffer, frame.into());
        }
    }

    fn pop_data_frame(
        &mut self,
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
        max_frame_size: usize,
        mut frame: frame::Data<Bytes>,
    ) -> Option<Frame<Bytes>> {
        let rem = stream.remaining_data_len?;
        trace!("rem| {rem}");

        if rem == 0 {
            stream.remaining_data_len = None;
            return if let Some(s) = self.spa_tracker.as_mut() {
                s.add_pending_spa(stream.id);
                trace!("empty data| added to send");
                stream
                    .pending_send
                    .push_front(buffer, frame.into());
                None
            } else {
                Self::set_data_frame_eos(buffer, stream, &mut frame, None);
                Some(frame.into())
            };
        }

        let stream_available = stream.send_flow.available();
        trace!("stream flow| {stream_available}");

        // stream level flow control
        if stream_available == 0 && rem > 0 {
            trace!("no stream flow");
            // increase the number of streams pending for stream level window
            // update in SpaTracker
            if let Some(s) = self.spa_tracker.as_mut() {
                trace!("spa| added to pending stream flow");
                s.add_pending_stream_update(stream.id);
            }

            // The stream has no more capacity, this can happen if the remote
            // reduced the stream window. In this case, we need to buffer the
            // frame and wait for a window update...
            stream
                .pending_send
                .push_front(buffer, frame.into());
            return None;
        }

        // connection level flow control
        if !self.try_allocate_stream_flow(stream, rem, stream_available) {
            trace!("no conn capacity| added to pending capacity");
            stream
                .pending_send
                .push_front(buffer, frame.into());
            self.pending_capacity.push(stream);
            return None;
        }

        //     | remaining_data_len
        // min | max_frame_size
        //     | stream flow available
        //     | connection flow allocated
        let mut len = min(
            min(min(rem, max_frame_size) as u32, stream_available),
            stream.connection_window_allocated,
        );

        trace!("min data| {len}");
        // There *must* be be enough connection level capacity at this point.
        debug_assert!(len <= stream.connection_window_allocated);

        // spa check
        // if there will be no body after building body frame of len, then
        // reduce the len by 1 to perform spa
        if self.spa_tracker.is_some() && len as usize == rem {
            len -= 1;
        }

        // consume
        //      - stream flow control
        //      - connection window allocated
        //      - remaining_data_len
        let _res = stream.send_flow.dec_window(len as u32);
        stream.connection_window_allocated -= len as u32;
        stream.remaining_data_len = stream
            .remaining_data_len
            .map(|rem| rem - len as usize)
            .filter(|&rem| rem > 0);

        trace!("remaining data len| {:?}", stream.remaining_data_len);
        trace!(
            "stream remaining allocated| {0}",
            stream.connection_window_allocated
        );
        trace!("stream flow| {stream_available}");

        // split the buf
        let data = frame
            .payload_mut()
            .split_to(len as usize);
        let mut data_frame = frame::Data::new(stream.id, data);
        Self::set_data_frame_eos(buffer, stream, &mut data_frame, Some(frame));
        Some(Frame::Data(data_frame))
    }

    fn pop_frame(
        &mut self,
        buffer: &mut Buffer<Frame<Bytes>>,
        store: &mut Store,
        max_frame_size: usize,
        counts: &mut Counts,
    ) -> Option<Frame<Bytes>> {
        loop {
            match self.pending_send.pop(store) {
                Some(mut stream) => {
                    let span = trace_span!("pop frame| ", ?stream.id);
                    let _ = span.enter();
                    // It's possible that this stream, besides having data to
                    // send, is also queued to send a reset, and thus is
                    // already in the queue to wait for "some time" after a
                    // reset.
                    //
                    // To be safe, we just always ask the stream.
                    let is_pending_reset =
                        stream.is_pending_reset_expiration();

                    // local reference dropped
                    if stream.state.is_remote_reset()
                        || Some(Reason::CANCEL)
                            == stream.state.get_scheduled_reset()
                    {
                        trace!("reset| {}", stream.state.is_remote_reset());
                        self.clear_stream_queue(buffer, &mut stream);
                    }

                    let frame = match stream.pending_send.pop_front(buffer) {
                        Some(Frame::Data(frame)) => {
                            if let Some(frame) = self.pop_data_frame(
                                buffer,
                                &mut stream,
                                max_frame_size,
                                frame,
                            ) {
                                frame
                            } else {
                                continue;
                            }
                        }
                        Some(Frame::Headers(header)) => {
                            // if data frame is present, try assign capacity
                            if stream.remaining_data_len.is_some() {
                                trace!(
                                    "popping header| remaining data| {}",
                                    stream.remaining_data_len.unwrap()
                                );
                                self.try_assign_capacity(&mut stream);
                            }
                            if stream.is_sending_trailer {
                                stream.state.send_close();
                            }
                            Frame::Headers(header)
                        }
                        Some(frame) => frame.map(|_| {
                            unreachable!(
                                "Frame::map closure will only be called \
                                 on DATA frames."
                            )
                        }),
                        None => {
                            if let Some(reason) =
                                stream.state.get_scheduled_reset()
                            {
                                stream.set_reset(reason, Initiator::Library);
                                let frame =
                                    frame::Reset::new(stream.id, reason);
                                Frame::Reset(frame)
                            } else {
                                // If the stream receives a RESET from the
                                // peer, it may have had data buffered to be
                                // sent, but all the frames are cleared in
                                // clear_queue(). Instead of doing O(N)
                                // traversal through queue to remove, lets just
                                // ignore the stream here.
                                debug_assert!(stream.state.is_closed());
                                counts.transition_after(
                                    stream,
                                    is_pending_reset,
                                );
                                continue;
                            }
                        }
                    };
                    if stream.state.is_idle() {
                        self.last_opened_id = stream.id;
                    }
                    // spa check
                    if let Some(spa) = self.spa_tracker.as_mut()
                        && stream.remaining_data_len == Some(1)
                    {
                        debug_assert!(!stream.pending_send.is_empty());
                        spa.add_pending_spa(stream.id);
                    } else if !stream.pending_send.is_empty()
                        || stream.state.is_scheduled_reset()
                    {
                        // TODO(hyper): Only requeue the sender if it is ready
                        // to send the next frame. i.e. don't requeue it if the
                        // next frame is a data frame and the stream does not
                        // have any more capacity.
                        self.pending_send.push(&mut stream);
                    }
                    counts.transition_after(stream, is_pending_reset);
                    return Some(frame);
                }
                None => return None,
            }
        }
    }

    pub fn poll_complete<T>(
        &mut self,
        cx: &mut Context,
        buffer: &mut Buffer<Frame<Bytes>>,
        store: &mut Store,
        counts: &mut Counts,
        dst: &mut Codec<T, Bytes>,
    ) -> Poll<io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        ready!(dst.poll_ready(cx))?;
        let max_frame_len = dst.max_send_frame_size();
        loop {
            if let Some(mut stream) = self.pop_pending_open(store, counts) {
                self.pending_send
                    .push_front(&mut stream);
            }

            match self.pop_frame(buffer, store, max_frame_len, counts) {
                Some(frame) => {
                    trace!("frame buffered| {:?}", frame);
                    dst.buffer(frame)
                        .expect("invalid frame");
                    ready!(dst.poll_ready(cx))?;
                }
                None => {
                    ready!(dst.flush(cx))?;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    // ===== SPA =====
    pub fn can_perform_spa(&self) -> bool {
        trace!("send| {}", self.pending_send.is_empty());
        trace!("capacity| {}", self.pending_capacity.is_empty());
        trace!("open| {}", self.pending_open.is_empty());
        self.pending_send.is_empty()
            && self.pending_capacity.is_empty()
            && self.pending_open.is_empty()
    }

    pub fn spa_recv_window_update(&mut self, stream_id: StreamId) {
        if let Some(spa) = self.spa_tracker.as_mut() {
            spa.remove_pending_stream_update(stream_id);
        }
    }

    pub fn poll_spa<T>(
        &mut self,
        cx: &mut Context,
        buffer: &mut Buffer<Frame<Bytes>>,
        store: &mut Store,
        dst: &mut Codec<T, Bytes>,
        ping_handler: &mut PingHandler,
    ) -> Poll<io::Result<()>>
    where
        T: AsyncWrite + Unpin,
    {
        let can_perform_spa = self.can_perform_spa();
        let Some(spa_tracker) = self.spa_tracker.as_mut() else {
            return Poll::Ready(Ok(()));
        };

        if spa_tracker.are_pending_stream_window_update() || !can_perform_spa {
            trace!(
                "pending| window_update: {}| perform_spa: {}",
                spa_tracker.are_pending_stream_window_update(),
                can_perform_spa
            );
            return Poll::Ready(Ok(()));
        }

        // TODO: wait for connection window update ?
        //if self.flow.available() < size as u32 {
        //    error!("not enough stream flow control");
        //}
        //

        let mut is_done = false;
        let poll_result = spa_tracker.poll(
            cx,
            buffer,
            store,
            dst,
            ping_handler,
            can_perform_spa,
            &mut is_done,
        );
        if is_done {
            self.spa_tracker.take();
        }
        ready!(poll_result)?;
        dst.flush(cx)
    }

    pub(crate) fn recvd_ping(&mut self, payload: &[u8; 8], cx: &mut Context) {
        if let Some(spa_tracker) = self.spa_tracker.as_mut() {
            spa_tracker.recvd_ping(cx, payload);
        };
    }
}
