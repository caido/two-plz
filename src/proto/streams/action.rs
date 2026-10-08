use bytes::Bytes;
use tracing::warn;

use crate::frame::Frame;
use crate::frame::Reason;
use crate::frame::StreamId;
use crate::proto::error::Initiator;
use crate::proto::streams::Ptr;
use crate::proto::streams::buffer::Buffer;
use crate::proto::streams::counts::Counts;
use crate::proto::streams::store::Store;
use crate::{
    proto::{
        ProtoError,
        config::ConnectionConfig,
        streams::{Recv, Send},
    },
    role::Role,
};
use std::task::Waker;

#[derive(Debug)]
pub struct Actions {
    /// Manages state transitions initiated by receiving frames
    pub recv: Recv,

    /// Manages state transitions initiated by sending frames
    pub send: Send,

    /// Task that calls `poll_complete`.
    pub task: Option<Waker>,

    /// If the connection errors, a copy is kept for any StreamRefs.
    pub conn_error: Option<ProtoError>,
}

impl Actions {
    pub fn new(role: Role, mut config: ConnectionConfig) -> Self {
        let mut send = Send::new(&mut config);
        send.next_stream_id = Ok(if role.is_server() {
            2.into()
        } else {
            #[cfg(feature = "test-util")]
            {
                config.initial_stream_id
            }
            #[cfg(not(feature = "test-util"))]
            {
                1.into()
            }
        });
        Actions {
            recv: Recv::new(&config, &role),
            send,
            task: None,
            conn_error: None,
        }
    }

    // ===== RESET =====
    pub fn send_reset(
        &mut self,
        stream: Ptr,
        reason: Reason,
        initiator: Initiator,
        counts: &mut Counts,
        send_buffer: &mut Buffer<Frame<Bytes>>,
    ) -> Result<(), crate::proto::error::GoAway> {
        counts.transition(stream, |counts, stream| {
            if initiator.is_library() {
                if counts.can_inc_num_local_error_resets() {
                    counts.inc_num_local_error_resets();
                } else {
                    warn!(
                        "local reset limit reached| {:?}",
                        counts.max_local_error_resets().unwrap()
                    );
                    return Err(crate::proto::error::GoAway {
                        reason: Reason::ENHANCE_YOUR_CALM,
                        debug_data: "too_many_internal_resets".into(),
                    });
                }
            }

            self.send.send_reset(
                reason,
                initiator,
                stream,
                send_buffer,
                counts,
                &mut self.task,
            );
            self.recv
                .enqueue_reset_expiration(stream, counts);
            // if a RecvStream is parked, ensure it's notified
            stream.notify_recv();
            Ok(())
        })
    }

    pub fn reset_on_recv_stream_err(
        &mut self,
        buffer: &mut Buffer<Frame<Bytes>>,
        stream: &mut Ptr,
        counts: &mut Counts,
        res: Result<(), ProtoError>,
    ) -> Result<(), ProtoError> {
        if let Err(ProtoError::Reset(stream_id, reason, initiator)) = res {
            debug_assert_eq!(stream_id, stream.id);

            if counts.can_inc_num_local_error_resets() {
                counts.inc_num_local_error_resets();
                // Reset the stream.
                self.send.send_reset(
                    reason,
                    initiator,
                    stream,
                    buffer,
                    counts,
                    &mut self.task,
                );
                self.recv
                    .enqueue_reset_expiration(stream, counts);
                // if a RecvStream is parked, ensure it's notified
                stream.notify_recv();
                Ok(())
            } else {
                warn!(
                    "local reset limit reached| {:?}",
                    counts.max_local_error_resets().unwrap()
                );
                Err(ProtoError::library_go_away_data(
                    Reason::ENHANCE_YOUR_CALM,
                    "too_many_internal_resets",
                ))
            }
        } else {
            res
        }
    }

    /// Check whether the stream was present in the past
    pub fn ensure_not_idle(
        &mut self,
        role: Role,
        id: StreamId,
    ) -> Result<(), Reason> {
        let next_id = if role.is_local_init(id) {
            self.send.next_stream_id
        } else {
            self.recv.next_stream_id
        };

        if let Ok(next) = next_id
            && id >= next
        {
            return Err(Reason::PROTOCOL_ERROR);
        }
        Ok(())
    }

    /// Check if we possibly could have processed and since forgotten this stream.
    ///
    /// If we send a RST_STREAM for a stream, we will eventually "forget" about
    /// the stream to free up memory. It's possible that the remote peer had
    /// frames in-flight, and by the time we receive them, our own state is
    /// gone. We *could* tear everything down by sending a GOAWAY, but it
    /// is more likely to be latency/memory constraints that caused this,
    /// and not a bad actor. So be less catastrophic, the spec allows
    /// us to send another RST_STREAM of STREAM_CLOSED.
    pub fn is_forgotten_stream(&self, role: &Role, id: StreamId) -> bool {
        if id.is_zero() {
            return false;
        }

        let next = if role.is_local_init(id) {
            self.send.next_stream_id
        } else {
            self.recv.next_stream_id
        };

        if let Ok(next_id) = next {
            debug_assert_eq!(
                id.is_server_initiated(),
                next_id.is_server_initiated(),
            );
            id < next_id
        } else {
            true
        }
    }

    pub fn ensure_no_conn_error(&self) -> Result<(), ProtoError> {
        if let Some(ref err) = self.conn_error {
            Err(err.clone())
        } else {
            Ok(())
        }
    }

    // ===== EOF =====
    pub fn clear_queues(
        &mut self,
        clear_pending_accept: bool,
        store: &mut Store,
        counts: &mut Counts,
    ) {
        // reset + pending_accept
        self.recv
            .clear_queues(clear_pending_accept, store, counts);

        // pending_capacity + pending_send + pending_open
        self.send.clear_queues(store, counts);
    }
}
