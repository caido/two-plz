use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::frame;
use crate::frame::Reason;
use crate::proto::ProtoError;
use crate::proto::error::Initiator;
use crate::proto::go_away::GoAway;
use crate::proto::ping_pong::PingAction;
use crate::proto::settings::SettingsAction;
use crate::proto::settings::SettingsHandler;
use crate::proto::streams::StreamRef;
use crate::proto::streams::Streams;
use crate::role::Role;

use bytes::Bytes;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::trace;
use tracing::trace_span;

use crate::frame::Settings;
use crate::{
    codec::{Codec, UserError},
    frame::{Frame, StreamId},
    proto::{config::ConnectionConfig, ping_pong::PingHandler},
};

#[derive(Debug)]
enum ConnectionState {
    /// Currently open in a sane state
    Open,

    /// The codec must be flushed
    Closing(Reason, Initiator),

    /// In a closed state
    Closed(Reason, Initiator),
}

#[derive(Debug)]
enum PeerPrefaceState {
    AwaitingSettings,
    ReceivedSettings,
}

pub struct Connection<T> {
    codec: Codec<T, Bytes>,
    pub(crate) streams: Streams<Bytes>,
    settings_handler: SettingsHandler,
    peer_preface_state: PeerPrefaceState,
    ping_handler: PingHandler,
    goaway_handler: GoAway,
    role: Role,
    state: ConnectionState,
    span: tracing::Span,
    /// An error to report back once complete.
    ///
    /// This exists separately from State in order to support
    /// graceful shutdown.
    error: Option<frame::GoAway>,
    local_error: Option<ProtoError>,
    is_spa: bool,
}

impl<T> Connection<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(
        role: Role,
        mut config: ConnectionConfig,
        codec: Codec<T, Bytes>,
    ) -> Self {
        let settings_handler =
            SettingsHandler::new(config.local_settings.clone());
        let is_spa = config.spa_tracker.is_some();
        let mut ping_handler = PingHandler::new();
        if let Some(spa_tracker) = config
            .spa_tracker
            .as_mut()
            .filter(|m| m.is_enhanced_ping())
        {
            spa_tracker.add_enhanced_ping_first_ping(&mut ping_handler);
        }

        let peer_preface_state = if role.is_client() {
            PeerPrefaceState::AwaitingSettings
        } else {
            PeerPrefaceState::ReceivedSettings
        };

        Connection {
            state: ConnectionState::Open,
            settings_handler,
            peer_preface_state,
            ping_handler,
            goaway_handler: GoAway::new(),
            codec,
            role: role.clone(),
            span: trace_span!("conn", "{}", role.as_str()),
            is_spa,
            streams: Streams::new(role.clone(), config),
            error: None,
            local_error: None,
        }
    }

    // ===== SERVER =====
    pub(crate) fn next_accept(&mut self) -> Option<StreamRef> {
        self.streams.next_accept()
    }

    // ===== CLIENT =====
    /// Closes the connection by transitioning to a GOAWAY state
    /// if there are no streams or references
    pub fn maybe_close_connection_if_no_streams(&mut self) {
        // If we poll() and realize that there are no streams or references
        // then we can close the connection by transitioning to GOAWAY
        if !self
            .streams
            .has_streams_or_other_references()
        {
            self.go_away_now(Reason::NO_ERROR);
        }
    }

    /// Checks if there are any streams or references left
    pub fn has_streams_or_other_references(&self) -> bool {
        // If we poll() and realize that there are no streams or references
        // then we can close the connection by transitioning to GOAWAY
        self.streams
            .has_streams_or_other_references()
    }

    // ===== COMMON =====
    pub fn buffer(&mut self, item: Frame<Bytes>) -> Result<(), UserError> {
        self.codec.buffer(item)
    }

    // ======== FRAMES ============
    pub fn recv_frame(
        &mut self,
        frame: Frame,
        cx: &mut Context,
    ) -> Result<(), ProtoError> {
        if matches!(
            self.peer_preface_state,
            PeerPrefaceState::AwaitingSettings
        ) {
            match &frame {
                Frame::Settings(settings) if !settings.is_ack() => {
                    self.peer_preface_state =
                        PeerPrefaceState::ReceivedSettings;
                }
                _ => {
                    return Err(ProtoError::library_go_away(
                        Reason::PROTOCOL_ERROR,
                    ));
                }
            }
        }

        match frame {
            Frame::Data(data) => self.streams.recv_data(data),
            Frame::Headers(headers) => self.streams.recv_header(headers),
            // Priority information does not change stream state. Scheduling based
            // on the deprecated dependency tree is not implemented.
            Frame::Priority(_) => Ok(()),
            Frame::Reset(reset) => self.streams.recv_reset(reset),
            Frame::Settings(settings) => self.recv_settings(settings),
            Frame::PushPromise(_push_promise) => todo!(), // hyper
            Frame::Ping(ping) => {
                use PingAction::*;
                match self.ping_handler.handle(ping) {
                    MustAck => (),
                    Unknown => {
                        if self.is_spa
                            && let Some(payload) =
                                self.ping_handler.pending_ping_payload()
                        {
                            self.streams.recvd_ping(&payload, cx);
                        }
                    }
                    Shutdown => {
                        let last_processed_id =
                            self.streams.last_processed_id();
                        self.go_away_graceful(
                            last_processed_id,
                            Reason::NO_ERROR,
                        );
                    }
                }
                Ok(())
            }
            Frame::GoAway(go_away) => {
                // This should prevent starting new streams,
                // but should allow continuing to process current streams
                // until they are all EOS. Once they are, State should
                // transition to GoAway.
                self.streams.recv_go_away(&go_away)?;
                self.error = Some(go_away);
                Ok(())
            }
            Frame::WindowUpdate(wupdate) => {
                self.streams.recv_window_update(wupdate)
            }
        }?;
        Ok(())
    }

    // ===== Settings =====
    pub fn recv_settings(
        &mut self,
        settings: Settings,
    ) -> Result<(), ProtoError> {
        if let SettingsAction::ApplyLocal(local_settings) =
            self.settings_handler.recv(settings)?
        {
            self.apply_local_settings(local_settings)?;
        }
        Ok(())
    }

    pub fn apply_local_settings(
        &mut self,
        settings: Settings,
    ) -> Result<(), ProtoError> {
        if let Some(max) = settings.max_frame_size() {
            self.codec
                .set_max_recv_frame_size(max as usize);
        }

        if let Some(max) = settings.max_header_list_size() {
            self.codec
                .set_max_recv_header_list_size(max as usize);
        }

        if let Some(val) = settings.header_table_size() {
            self.codec
                .set_recv_header_table_size(val as usize);
        }
        self.streams
            .apply_local_settings(&settings)
    }

    // ==== GOAWAY =====
    // send goaway - shutdown
    fn go_away_graceful(&mut self, id: StreamId, e: Reason) {
        let frame = frame::GoAway::new(id, e);
        // sets recv.last_processed_id
        self.streams.send_go_away(id);
        self.goaway_handler
            .enqueue_go_away(frame);
    }

    // send goaway - immediate
    fn go_away_now(&mut self, e: Reason) {
        let last_processed_id = self.streams.last_processed_id();
        let frame = frame::GoAway::new(last_processed_id, e);
        self.goaway_handler.go_away_now(frame);
    }

    // send_goaway - immediate with data
    fn go_away_now_data(&mut self, e: Reason, data: Bytes) {
        let last_processed_id = self.streams.last_processed_id();
        let frame = frame::GoAway::with_debug_data(last_processed_id, e, data);
        self.goaway_handler.go_away_now(frame);
    }

    pub(crate) fn go_away_from_user(&mut self, e: Reason) {
        let last_processed_id = self.streams.last_processed_id();
        let frame = frame::GoAway::new(last_processed_id, e);
        self.goaway_handler
            .go_away_from_user(frame);
        // Notify all streams of reason we're abruptly closing.
        self.streams
            .handle_error(ProtoError::user_go_away(e));
    }

    // ===== Misc =====
    pub(crate) fn is_extended_connect_protocol_enabled(&self) -> bool {
        self.streams
            .is_extended_connect_protocol_enabled()
    }

    pub(crate) fn max_send_streams(&self) -> usize {
        self.streams.max_send_streams()
    }

    pub fn num_wired_streams(&self) -> usize {
        self.streams.num_wired_streams()
    }

    // ===== POLLING =====
    pub fn poll(&mut self, cx: &mut Context) -> Poll<Result<(), ProtoError>> {
        let span = self.span.clone();
        let _e = span.enter();

        loop {
            trace!(connection.state = ?self.state);
            match self.state {
                ConnectionState::Open => {
                    let result = match self.poll2(cx) {
                        Poll::Ready(result) => result,
                        Poll::Pending => {
                            ready!(
                                self.streams
                                    .poll_complete(cx, &mut self.codec,)
                            )?;
                            //// check for spa
                            ready!(self.poll_spa(cx))?;
                            if self.can_go_away() {
                                self.go_away_now(Reason::NO_ERROR);
                                continue;
                            }
                            return Poll::Pending;
                        }
                    };
                    self.handle_poll2_result(result)?
                }
                ConnectionState::Closing(reason, initiator) => {
                    trace!("closing after flush");
                    ready!(self.codec.shutdown(cx))?;
                    self.state = ConnectionState::Closed(reason, initiator);
                }
                ConnectionState::Closed(reason, initiator) => {
                    return Poll::Ready(self.take_error(reason, initiator));
                }
            }
        }
    }

    #[inline(always)]
    fn can_go_away(&self) -> bool {
        (self.error.is_some()
            || self
                .goaway_handler
                .should_close_on_idle())
            && !self.streams.has_streams()
    }

    fn poll2(&mut self, cx: &mut Context) -> Poll<Result<(), ProtoError>> {
        // This happens outside of the loop to prevent needing to do a clock
        // check and then comparison of the queue possibly multiple times a
        // second (and thus, the clock wouldn't have changed enough to matter).
        self.streams
            .clear_expired_reset_streams();

        loop {
            // First, ensure that the `Connection` is able to receive a frame
            //
            // The order here matters:
            // - poll_go_away may buffer a graceful shutdown GOAWAY frame
            // - If it has, we've also added a PING to be sent in poll_ready
            if let Some(reason) = ready!(self.poll_go_away(cx)?) {
                if self.goaway_handler.should_close_now() {
                    if self.goaway_handler.is_user_initiated() {
                        // A user initiated abrupt shutdown shouldn't return
                        // the same error back to the user.
                        return Poll::Ready(Ok(()));
                    } else {
                        return Poll::Ready(Err(ProtoError::library_go_away(
                            reason,
                        )));
                    }
                }
                // Only NO_ERROR should be waiting for idle
                debug_assert_eq!(
                    reason,
                    Reason::NO_ERROR,
                    "graceful GOAWAY should be NO_ERROR"
                );
            }
            ready!(self.poll_control_frames(cx))?;
            // read a frame
            match ready!(Pin::new(&mut self.codec).poll_next(cx)?) {
                Some(frame) => {
                    self.recv_frame(frame, cx)?;
                }
                None => {
                    trace!("codec closed");
                    if matches!(
                        self.peer_preface_state,
                        PeerPrefaceState::AwaitingSettings
                    ) {
                        return Poll::Ready(Err(ProtoError::library_go_away(
                            Reason::PROTOCOL_ERROR,
                        )));
                    }
                    self.streams
                        .recv_eof(false)
                        .expect("mutex poisoned");
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    /// Send any pending GOAWAY frames.
    ///
    /// This will return `Some(reason)` if the connection should be closed
    /// afterwards. If this is a graceful shutdown, this returns `None`.
    fn poll_go_away(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Option<std::io::Result<Reason>>> {
        self.goaway_handler
            .send_pending_go_away(cx, &mut self.codec)
    }

    fn handle_poll2_result(
        &mut self,
        result: Result<(), ProtoError>,
    ) -> Result<(), ProtoError> {
        match result {
            // The connection has shutdown normally
            Ok(()) => {
                self.state = ConnectionState::Closing(
                    Reason::NO_ERROR,
                    Initiator::Library,
                );
                Ok(())
            }
            // Attempting to read a frame resulted in a connection level
            // error. This is handled by setting a GOAWAY frame followed by
            // terminating the connection.
            Err(ProtoError::GoAway(debug_data, reason, initiator)) => {
                self.poll2_go_away(reason, debug_data, initiator);
                Ok(())
            }
            // Attempting to read a frame resulted in a stream level error.
            // This is handled by resetting the frame then trying to read
            // another frame.
            Err(ProtoError::Reset(id, reason, initiator)) => {
                debug_assert_eq!(initiator, Initiator::Library);
                match self.streams.send_reset(id, reason) {
                    Ok(()) => (),
                    // only possible - Too many internal resets,
                    //                 ENHANCE_YOUR_CALM
                    Err(crate::proto::error::GoAway {
                        debug_data,
                        reason,
                    }) => {
                        self.poll2_go_away(
                            reason,
                            debug_data,
                            Initiator::Library,
                        );
                    }
                }
                Ok(())
            }
            // Attempting to read a frame resulted in an I/O error. All
            // active streams must be reset.
            //
            // TODO(hyper): Are I/O errors recoverable?
            Err(ProtoError::Io(kind, inner)) => {
                let e = ProtoError::Io(kind, inner);
                // Reset and Notify all active streams
                self.streams.handle_error(e.clone());
                // Some client implementations drop the connections without
                // notifying its peer Attempting to read after the client
                // dropped the connection results in UnexpectedEof If as a
                // server, we don't have anything more to send, just close the
                // connection without error
                //
                // See https://github.com/hyperium/hyper/issues/3427
                if self.streams.send_buffer_is_empty()
                    && matches!(kind, std::io::ErrorKind::UnexpectedEof)
                    && (self.role.is_server()
                        || self
                            .error
                            .as_ref()
                            .map(|f| f.reason() == Reason::NO_ERROR)
                            == Some(true))
                {
                    self.state = ConnectionState::Closed(
                        Reason::NO_ERROR,
                        Initiator::Library,
                    );
                    return Ok(());
                }
                Err(e)
            }
        }
    }

    fn poll2_go_away(
        &mut self,
        reason: Reason,
        debug_data: Bytes,
        initiator: Initiator,
    ) {
        let e = ProtoError::GoAway(debug_data.clone(), reason, initiator);
        // We may have already sent a GOAWAY for this error,
        // if so, don't send another, just flush and close up.
        if self
            .goaway_handler
            .going_away()
            .is_some_and(|frame| frame.reason() == reason)
        {
            self.state = ConnectionState::Closing(reason, initiator);
            return;
        }

        // Reset and notify all active streams, and retain the error for the
        // connection future. Deferred peer-preface failures no longer surface
        // from Builder::handshake.
        self.streams.handle_error(e.clone());
        self.local_error = Some(e);
        self.go_away_now_data(reason, debug_data);
    }

    fn poll_control_frames(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Result<(), ProtoError>> {
        let span = tracing::trace_span!("poll| control frames");
        let _e = span.enter();
        ready!(
            self.ping_handler
                .poll_pending(cx, &mut self.codec)
        )?;
        ready!(
            self.settings_handler
                .poll_remote_settings(cx, &mut self.codec, &mut self.streams)
        )?;
        ready!(
            self.settings_handler
                .poll_local_settings(cx, &mut self.codec)
        )?;
        ready!(
            self.streams
                .send_pending_refusal(cx, &mut self.codec)
        )?;
        ready!(
            self.streams
                .poll_window_update(cx, &mut self.codec)
        )?;
        Poll::Ready(Ok(()))
    }

    fn take_error(
        &mut self,
        ours: Reason,
        initiator: Initiator,
    ) -> Result<(), ProtoError> {
        if let Some(error) = self.local_error.take() {
            return Err(error);
        }

        let (debug_data, theirs) = self
            .error
            .take()
            .as_ref()
            .map_or((Bytes::new(), Reason::NO_ERROR), |frame| {
                (frame.debug_data().clone(), frame.reason())
            });

        match (ours, theirs) {
            (Reason::NO_ERROR, Reason::NO_ERROR) => Ok(()),
            (ours, Reason::NO_ERROR) => {
                Err(ProtoError::GoAway(Bytes::new(), ours, initiator))
            }
            // If both sides reported an error, give their
            // error back to th user. We assume our error
            // was a consequence of their error, and less
            // important.
            (_, theirs) => Err(ProtoError::remote_go_away(debug_data, theirs)),
        }
    }

    pub(crate) fn go_away_gracefully(&mut self) {
        if self.goaway_handler.is_going_away() {
            // No reason to start a new one.
            return;
        }
        self.go_away_graceful(StreamId::MAX, Reason::NO_ERROR);
        self.ping_handler.ping_shutdown();
    }

    fn poll_spa(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Result<(), std::io::Error>> {
        if !self.is_spa {
            return Poll::Ready(Ok(()));
        }
        self.streams
            .poll_spa(cx, &mut self.codec, &mut self.ping_handler)
    }
}

impl<T> Drop for Connection<T> {
    fn drop(&mut self) {
        // Ignore errors as this indicates that the mutex is poisoned.
        let _ = self.streams.recv_eof(true);
    }
}
