use crate::{
    frame,
    proto::{config::ConnectionConfig, streams::store::Ptr},
    role::Role,
};
use tracing::{Level, span, trace};

#[derive(Debug)]
pub struct Counts {
    /// role
    pub role: Role,

    /// Maximum number of locally initiated streams
    max_send_streams: usize,

    /// Current number of locally initiated streams
    num_send_streams: usize,

    /// Maximum number of remote initiated streams
    max_recv_streams: usize,

    /// Current number of remote initiated streams
    num_recv_streams: usize,

    /// Maximum number of pending locally reset streams
    max_local_reset_streams: usize,

    /// Current number of pending locally reset streams
    num_local_reset_streams: usize,

    /// Max number of "pending accept" streams that were remotely reset
    max_remote_reset_streams: usize,

    /// Current number of "pending accept" streams that were remotely reset
    num_remote_reset_streams: usize,

    /// Maximum number of locally reset streams due to protocol error across
    /// the lifetime of the connection.
    ///
    /// When this gets exceeded, we issue GOAWAYs.
    max_local_error_reset_streams: Option<usize>,

    /// Total number of locally reset streams due to protocol error across the
    /// lifetime of the connection.
    num_local_error_reset_streams: usize,
}

impl Counts {
    pub fn new(role: Role, config: &ConnectionConfig) -> Self {
        Counts {
            role,
            max_send_streams: config
                .peer_settings
                .max_concurrent_streams()
                .map(|v| v as usize)
                .unwrap_or(usize::MAX),
            num_send_streams: 0,
            max_recv_streams: config
                .local_settings
                .max_concurrent_streams()
                .map(|v| v as usize)
                .unwrap_or(usize::MAX),
            num_recv_streams: 0,
            max_local_reset_streams: config.local_reset_stream_max,
            num_local_reset_streams: 0,
            max_local_error_reset_streams: config
                .local_max_error_reset_streams,
            num_local_error_reset_streams: 0,
            max_remote_reset_streams: config.remote_reset_stream_max,
            num_remote_reset_streams: 0,
        }
    }

    pub fn has_streams(&self) -> bool {
        self.num_send_streams != 0 || self.num_recv_streams != 0
    }

    // ===== local error resets ====
    pub fn max_local_error_resets(&self) -> Option<usize> {
        self.max_local_error_reset_streams
    }

    /// Returns true if we can issue another local reset due to protocol error.
    pub fn can_inc_num_local_error_resets(&self) -> bool {
        if let Some(max) = self.max_local_error_reset_streams {
            max > self.num_local_error_reset_streams
        } else {
            true
        }
    }

    pub fn inc_num_local_error_resets(&mut self) {
        assert!(self.can_inc_num_local_error_resets());

        // Increment the number of remote initiated streams
        self.num_local_error_reset_streams += 1;
    }

    // ===== recv =====
    /// Returns true if the receive stream concurrency can be incremented
    pub fn can_inc_num_recv_streams(&self) -> bool {
        self.max_recv_streams > self.num_recv_streams
    }

    /// Increments the number of concurrent receive streams.
    ///
    /// # Panics
    ///
    /// Panics on failure as this should have been validated before hand.
    pub fn inc_num_recv_streams(&mut self, stream: &mut Ptr) {
        assert!(self.can_inc_num_recv_streams());
        assert!(!stream.is_counted);

        // Increment the number of remote initiated streams
        self.num_recv_streams += 1;
        stream.is_counted = true;
    }

    // ===== send =====
    pub(crate) fn max_send_streams(&self) -> usize {
        self.max_send_streams
    }

    /// Returns true if the send stream concurrency can be incremented
    pub fn can_inc_num_send_streams(&self) -> bool {
        self.max_send_streams > self.num_send_streams
    }

    /// Increments the number of concurrent send streams.
    ///
    /// # Panics
    ///
    /// Panics on failure as this should have been validated before hand.
    pub fn inc_num_send_streams(&mut self, stream: &mut Ptr) {
        assert!(self.can_inc_num_send_streams());
        assert!(!stream.is_counted);
        self.num_send_streams += 1;
        stream.is_counted = true;
    }

    // ===== Reset pending accept =====
    pub fn can_inc_num_reset_streams(&self) -> bool {
        self.max_local_reset_streams > self.num_local_reset_streams
    }

    fn dec_num_reset_streams(&mut self) {
        assert!(self.num_local_reset_streams > 0);
        self.num_local_reset_streams -= 1;
    }

    /// Increments the number of pending reset streams.
    ///
    /// # Panics
    ///
    /// Panics on failure as this should have been validated before hand.
    pub fn inc_num_reset_streams(&mut self) {
        assert!(self.can_inc_num_reset_streams());
        self.num_local_reset_streams += 1;
    }

    // ===== Remote Reset =====
    pub(crate) fn max_remote_reset_streams(&self) -> usize {
        self.max_remote_reset_streams
    }

    pub fn can_inc_num_remote_reset_streams(&self) -> bool {
        self.max_remote_reset_streams > self.num_remote_reset_streams
    }

    /// Increments the number of pending reset streams.
    ///
    /// # Panics
    ///
    /// Panics on failure as this should have been validated before hand.
    pub fn inc_num_remote_reset_streams(&mut self) {
        assert!(self.can_inc_num_remote_reset_streams());
        self.num_remote_reset_streams += 1;
    }

    pub fn dec_num_remote_reset_streams(&mut self) {
        assert!(self.num_remote_reset_streams > 0);
        self.num_remote_reset_streams -= 1;
    }

    // ===== SETTINGS =====
    pub fn apply_remote_settings(&mut self, settings: &frame::Settings) {
        if let Some(max) = settings.max_concurrent_streams() {
            self.max_send_streams = max as usize;
        }
    }

    // ===== Misc =====
    fn dec_num_streams(&mut self, stream: &mut Ptr) {
        assert!(stream.is_counted);

        if self.role.is_local_init(stream.id) {
            assert!(self.num_send_streams > 0);
            self.num_send_streams -= 1;
            stream.is_counted = false;
        } else {
            assert!(self.num_recv_streams > 0);
            self.num_recv_streams -= 1;
            stream.is_counted = false;
        }
    }

    pub fn role(&self) -> Role {
        self.role.clone()
    }

    /// Run a block of code that could potentially transition a stream's state.
    ///
    /// If the stream state transitions to closed, this function will perform
    /// all necessary cleanup.
    ///
    /// TODO(hyper): Is this function still needed?
    pub fn transition<F, U>(&mut self, mut stream: Ptr, f: F) -> U
    where
        F: FnOnce(&mut Self, &mut Ptr) -> U,
    {
        // TODO(hyper): Does this need to be computed before performing the action?
        let is_pending_reset = stream.is_pending_reset_expiration();

        // Run the action
        let ret = f(self, &mut stream);

        self.transition_after(stream, is_pending_reset);

        ret
    }

    // TODO(hyper): move this to macro?
    pub fn transition_after(
        &mut self,
        mut stream: Ptr,
        is_reset_counted: bool,
    ) {
        let span =
            span!(Level::TRACE, "transition after|", "{:?}| ", stream.id);
        let _enter = span.enter();
        if stream.is_closed() {
            trace!("closed");
            if !stream.is_pending_reset_expiration() {
                trace!("unlinked");
                stream.unlink();
                if is_reset_counted {
                    self.dec_num_reset_streams();
                }
            }

            if !stream.state.is_scheduled_reset() && stream.is_counted {
                // Decrement the number of active streams.
                self.dec_num_streams(&mut stream);
            } else {
                trace!(
                    "scheduled reset| {}",
                    stream.state.is_scheduled_reset()
                );
                trace!("counted| {}", stream.is_counted);
            }
        }

        // Release the stream if it requires releasing
        if stream.is_released() {
            stream.remove();
            trace!("removed");
        } else {
            trace!("stream not released");
        }
    }
}

impl Drop for Counts {
    fn drop(&mut self) {
        use std::thread;

        if !thread::panicking() {
            debug_assert!(!self.has_streams());
        }
    }
}
