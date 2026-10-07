use tracing::trace;

use crate::proto::error::Initiator;
use crate::proto::streams::buffer::Deque;
use crate::proto::streams::flow_control::FlowControl;
use crate::proto::streams::store::{self, Key, Next, Queue};
use crate::{frame::Reason, proto::streams::state::State};
use std::task::Waker;
use std::time::Instant;

use crate::{frame::StreamId, proto::WindowSize};

// ===== Queues =====
// recv
#[derive(Debug)]
pub(super) struct NextAccept;

#[derive(Debug)]
pub(super) struct NextComplete;

// send
#[derive(Debug)]
pub(super) struct NextSend;

#[derive(Debug)]
pub(super) struct NextSendCapacity;

#[derive(Debug)]
pub(super) struct NextOpen;

#[derive(Debug)]
pub(super) struct NextResetExpire;

#[derive(Debug)]
pub(crate) struct Stream {
    pub(crate) id: StreamId,
    pub state: State,

    /// Number of outstanding handles pointing to this stream
    pub ref_count: usize,

    /// Set to `true` when the stream is counted against the connection's max
    /// concurrent streams.
    pub is_counted: bool,
    /// A promised HEAD request cannot have an outbound response body.
    pub is_push_head: bool,
    pub promised_request: Option<http_plz::Request>,
    pub is_remote_push: bool,
    pub is_pending_push: bool,

    // ===== Send =====
    pub send_flow: FlowControl,
    pub remaining_data_len: Option<usize>,
    pub connection_window_allocated: WindowSize,

    /// Next Send
    pub next_pending_send: Option<Key>,
    pub is_pending_send: bool,
    pub pending_send: Deque, // frames

    /// Next capacity.
    pub next_pending_send_capacity: Option<Key>,
    pub is_pending_send_capacity: bool,

    /// Next Open
    pub next_open: Option<Key>,
    pub is_pending_open: bool,

    // ===== Recv =====
    pub curr_buf_len: usize,
    pub streaming_recv: bool,
    pub is_buf_limit_reached: bool,
    pub recv_flow: FlowControl,
    pub content_length: ContentLength,
    /// Locally initiated CONNECT requests ignore content-length on 2xx responses.
    pub is_connect: bool,
    /// When the RecvStream drop occurs, no data should be received.
    /// TODO: why?
    pub _is_recv: bool,
    /// Task tracking receiving frames
    pub recv_task: Option<Waker>,

    /// Next Accept
    pub next_pending_accept: Option<Key>,
    pub is_pending_accept: bool,
    pub pending_recv: Deque, // Events

    // Next Complete
    pub next_pending_complete: Option<Key>,
    pub is_pending_complete: bool,

    // ===== Reset =====
    /// The time when this stream may have been locally reset.
    pub reset_at: Option<Instant>,
    pub next_reset_expire: Option<Key>,

    /// ===== Push Promise =====
    /// The stream's pending push promises
    pub(super) pending_push_promises: Queue<NextAccept>,

    // ===== trailers =====
    pub is_sending_trailer: bool,
}

impl Stream {
    pub fn new(
        id: StreamId,
        init_send_window: WindowSize,
        init_recv_window: WindowSize,
    ) -> Stream {
        Stream {
            id,
            state: State::default(),
            ref_count: 0,
            is_counted: false,
            is_push_head: false,
            promised_request: None,
            is_remote_push: false,
            is_pending_push: false,
            // === send ===
            send_flow: FlowControl::new(init_send_window),
            remaining_data_len: None,
            connection_window_allocated: 0,
            // next send
            next_pending_send: None,
            is_pending_send: false,
            pending_send: Deque::new(),
            // next capacity
            next_pending_send_capacity: None,
            is_pending_send_capacity: false,
            // next open
            next_open: None,
            is_pending_open: false,
            // === recv ===
            curr_buf_len: 0,
            streaming_recv: false,
            is_buf_limit_reached: false,
            recv_flow: FlowControl::new(init_recv_window),
            content_length: ContentLength::Omitted,
            is_connect: false,
            _is_recv: true,
            recv_task: None,
            // next accept
            next_pending_accept: None,
            is_pending_accept: false,
            pending_recv: Deque::new(),
            // next complete
            next_pending_complete: None,
            is_pending_complete: false,
            // === reset ===
            reset_at: None,
            next_reset_expire: None,
            // push promise
            pending_push_promises: Queue::new(),
            // trailers
            is_sending_trailer: false,
        }
    }

    // ====== Ref count =====
    /// Increment the stream's ref count
    pub fn ref_inc(&mut self) {
        assert!(self.ref_count < usize::MAX);
        self.ref_count += 1;
    }

    /// Decrements the stream's ref count
    pub fn ref_dec(&mut self) {
        assert!(self.ref_count > 0);
        self.ref_count -= 1;
    }

    // ===== State ======

    /// Returns true when the consumer of the stream has dropped all handles
    /// (indicating no further interest in the stream) and the stream state is
    /// not actually closed.
    ///
    /// In this case, a reset should be sent.
    pub fn is_canceled_interest(&self) -> bool {
        self.ref_count == 0 && !self.state.is_closed()
    }

    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
            && self.pending_send.is_empty()
            && self.remaining_data_len.is_none()
    }

    /// Returns true if the stream is no longer in use
    pub fn is_released(&self) -> bool {
        // The stream is closed and fully flushed
        self.is_closed() &&
            // There are no more outstanding references to the stream
            self.ref_count == 0 &&
            // The stream is not in any queue
            // send queues
            !self.is_pending_push && !self.is_pending_send && !self.is_pending_send_capacity && !self.is_pending_open &&
            // recv queues
            !self.is_pending_accept && !self.is_pending_complete
            && self.reset_at.is_none()
    }

    pub fn is_send_ready(&self) -> bool {
        !self.is_pending_open
    }

    // ===== Reset =====

    /// Returns true if stream is currently being held for some time because of
    /// a local reset.
    pub fn is_pending_reset_expiration(&self) -> bool {
        self.reset_at.is_some()
    }

    pub(super) fn set_reset(&mut self, reason: Reason, initiator: Initiator) {
        self.state
            .set_reset(self.id, reason, initiator);
        self.notify_recv();
    }

    // ===== Content Length =====
    pub fn content_length(&self) -> Option<u64> {
        if let ContentLength::Remaining(total, _) = self.content_length {
            Some(total)
        } else {
            None
        }
    }

    pub fn ensure_content_length_zero(&self) -> Result<(), ()> {
        match self.content_length {
            ContentLength::Remaining(_, 0) => Ok(()),
            ContentLength::Remaining(..) => Err(()),
            _ => Ok(()),
        }
    }

    /// Returns `Err` when the decrement cannot be completed due to overflow.
    pub fn dec_content_length(&mut self, len: usize) -> Result<(), ()> {
        match self.content_length {
            ContentLength::Remaining(_, ref mut rem) => {
                match rem.checked_sub(len as u64) {
                    Some(val) => *rem = val,
                    None => return Err(()),
                }
            }
            ContentLength::Head if len != 0 => {
                return Err(());
            }
            _ => {}
        }
        Ok(())
    }

    // ===== task =====
    pub fn notify_recv(&mut self) {
        if let Some(task) = self.recv_task.take() {
            trace!("notified");
            task.wake();
        }
    }
}

// ===== Queue =====
// recv
impl Next for NextAccept {
    fn next(stream: &Stream) -> Option<store::Key> {
        stream.next_pending_accept
    }

    fn set_next(stream: &mut Stream, key: Option<store::Key>) {
        stream.next_pending_accept = key;
    }

    fn take_next(stream: &mut Stream) -> Option<store::Key> {
        stream.next_pending_accept.take()
    }

    fn is_queued(stream: &Stream) -> bool {
        stream.is_pending_accept
    }

    fn set_queued(stream: &mut Stream, val: bool) {
        stream.is_pending_accept = val;
    }
}

impl Next for NextComplete {
    fn next(stream: &Stream) -> Option<Key> {
        stream.next_pending_complete
    }

    fn set_next(stream: &mut Stream, key: Option<Key>) {
        stream.next_pending_complete = key
    }

    fn take_next(stream: &mut Stream) -> Option<Key> {
        stream.next_pending_complete.take()
    }

    fn is_queued(stream: &Stream) -> bool {
        stream.is_pending_complete
    }

    fn set_queued(stream: &mut Stream, val: bool) {
        stream.is_pending_complete = val
    }
}

// send
impl Next for NextSend {
    fn next(stream: &Stream) -> Option<store::Key> {
        stream.next_pending_send
    }

    fn set_next(stream: &mut Stream, key: Option<store::Key>) {
        stream.next_pending_send = key;
    }

    fn take_next(stream: &mut Stream) -> Option<store::Key> {
        stream.next_pending_send.take()
    }

    fn is_queued(stream: &Stream) -> bool {
        stream.is_pending_send
    }

    fn set_queued(stream: &mut Stream, val: bool) {
        if val {
            // ensure that stream is not queued for being opened
            // if it's being put into queue for sending data
            debug_assert!(!stream.is_pending_open);
        }
        stream.is_pending_send = val;
    }
}

impl Next for NextSendCapacity {
    fn next(stream: &Stream) -> Option<store::Key> {
        stream.next_pending_send_capacity
    }

    fn set_next(stream: &mut Stream, key: Option<store::Key>) {
        stream.next_pending_send_capacity = key;
    }

    fn take_next(stream: &mut Stream) -> Option<store::Key> {
        stream.next_pending_send_capacity.take()
    }

    fn is_queued(stream: &Stream) -> bool {
        stream.is_pending_send_capacity
    }

    fn set_queued(stream: &mut Stream, val: bool) {
        stream.is_pending_send_capacity = val;
    }
}

impl Next for NextOpen {
    fn next(stream: &Stream) -> Option<store::Key> {
        stream.next_open
    }

    fn set_next(stream: &mut Stream, key: Option<store::Key>) {
        stream.next_open = key;
    }

    fn take_next(stream: &mut Stream) -> Option<store::Key> {
        stream.next_open.take()
    }

    fn is_queued(stream: &Stream) -> bool {
        stream.is_pending_open
    }

    fn set_queued(stream: &mut Stream, val: bool) {
        if val {
            // ensure that stream is not queued for being sent
            // if it's being put into queue for opening the stream
            debug_assert!(!stream.is_pending_send);
        }
        stream.is_pending_open = val;
    }
}

impl Next for NextResetExpire {
    fn next(stream: &Stream) -> Option<store::Key> {
        stream.next_reset_expire
    }

    fn set_next(stream: &mut Stream, key: Option<store::Key>) {
        stream.next_reset_expire = key;
    }

    fn take_next(stream: &mut Stream) -> Option<store::Key> {
        stream.next_reset_expire.take()
    }

    fn is_queued(stream: &Stream) -> bool {
        stream.reset_at.is_some()
    }

    fn set_queued(stream: &mut Stream, val: bool) {
        if val {
            stream.reset_at = Some(Instant::now());
        } else {
            stream.reset_at = None;
        }
    }
}

#[derive(Debug)]
pub enum ContentLength {
    Head,
    Omitted,
    Remaining(u64, u64),
}

impl ContentLength {
    pub fn is_head(&self) -> bool {
        matches!(*self, Self::Head)
    }
}
