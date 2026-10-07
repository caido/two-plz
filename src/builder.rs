use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::proto::config::ConnectionConfig;
use crate::proto::{
    DEFAULT_LOCAL_RESET_COUNT_MAX, DEFAULT_REMOTE_RESET_COUNT_MAX,
    DEFAULT_RESET_STREAM_MAX, DEFAULT_RESET_STREAM_SECS,
};
use crate::role::Role;
use crate::spa::Mode;
use crate::{Codec, frame};
use crate::{
    frame::StreamId,
    preface::{PrefaceConn, PrefaceError},
};
use std::time::Duration;

pub trait BuildConnection {
    type Connection<T>: Sized;

    fn role_opts() -> Self;

    fn is_server() -> bool;

    fn is_client() -> bool;

    fn take_spa_mode(&mut self) -> Option<Mode>;

    fn is_spa(&self) -> bool;

    fn init_stream_id() -> StreamId;

    fn build<T>(
        role: Role,
        config: ConnectionConfig,
        codec: Codec<T, Bytes>,
    ) -> Self::Connection<T>
    where
        T: AsyncRead + AsyncWrite + Unpin;
}

pub struct Builder<R> {
    /// connection level flow control window size.
    pub initial_connection_window_size: Option<u32>,

    /// Maximum number of locally reset streams due to protocol error across
    /// the lifetime of the connection.
    ///
    /// When this gets exceeded, we issue GOAWAYs.
    pub local_max_error_reset_streams: Option<usize>,

    /// Time to keep locally reset streams around before reaping.
    pub reset_stream_duration: Duration,

    /// Maximum number of locally reset streams to keep at a time.
    pub local_reset_stream_max: usize,

    /// Maximum number of remote reset streams to keep at a time.
    pub remote_reset_stream_max: usize,

    pub role: R,

    /// Initial `Settings` frame to send as part of the handshake.
    pub settings: frame::Settings,

    /// Max recv body size limit
    pub max_recv_buffer_size: usize,
}

impl<R> Default for Builder<R>
where
    R: BuildConnection,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R> Builder<R>
where
    R: BuildConnection,
{
    pub fn new() -> Self {
        let mut settings = frame::Settings::default();
        // This client does not accept pushed responses. Servers must not send
        // SETTINGS_ENABLE_PUSH, which only controls server-to-client push.
        if R::is_client() {
            settings.set_enable_push(false);
        }
        Builder {
            initial_connection_window_size: None,
            local_reset_stream_max: DEFAULT_RESET_STREAM_MAX,
            local_max_error_reset_streams: Some(DEFAULT_LOCAL_RESET_COUNT_MAX),
            remote_reset_stream_max: DEFAULT_REMOTE_RESET_COUNT_MAX,
            reset_stream_duration: Duration::from_secs(
                DEFAULT_RESET_STREAM_SECS,
            ),
            settings,
            role: R::role_opts(),
            max_recv_buffer_size: usize::MAX,
        }
    }

    // ===== Flow Control =====

    // connection level
    pub fn initial_connection_window_size(mut self, size: u32) -> Self {
        self.initial_connection_window_size = Some(size);
        self
    }

    // stream level
    pub fn initial_window_size(mut self, size: u32) -> Self {
        self.settings
            .set_initial_window_size(Some(size));
        self
    }

    // ====== Settings frame =====

    /// Indicates the size (in octets) of the largest HTTP/2 frame payload that
    /// the configured client is able to accept.
    ///
    /// The sender may send data frames that are **smaller** than this value,
    /// but any data larger than `max` will be broken up into multiple `DATA`
    /// frames.
    ///
    /// The value **must** be between 16,384 and 16,777,215. The default value
    /// is 16,384.
    pub fn max_frame_size(mut self, max: u32) -> Self {
        self.settings
            .set_max_frame_size(Some(max));
        self
    }

    /// Sets the max size of received header frames.
    ///
    /// This advisory setting informs a peer of the maximum size of header list
    /// that the sender is prepared to accept, in octets. The value is based on
    /// the uncompressed size of header fields, including the length of the name
    /// and value in octets plus an overhead of 32 octets for each header field.
    ///
    /// This setting is also used to limit the maximum amount of data that is
    /// buffered to decode HEADERS frames.
    pub fn max_header_list_size(mut self, max: u32) -> Self {
        self.settings
            .set_max_header_list_size(Some(max));
        self
    }

    /// Sets the maximum number of concurrent streams.
    ///
    /// The maximum concurrent streams setting only controls the maximum number
    /// of streams that can be initiated by the remote peer. In other words,
    /// when this setting is set to 100, this does not limit the number of
    /// concurrent streams that can be created by the caller.
    ///
    /// It is recommended that this value be no smaller than 100, so as to not
    /// unnecessarily limit parallelism. However, any value is legal, including
    /// 0. If `max` is set to 0, then the remote will not be permitted to
    /// initiate streams.
    ///
    /// Note that streams in the reserved state, i.e., push promises that have
    /// been reserved but the stream has not started, do not count against this
    /// setting.
    ///
    /// Also note that if the remote *does* exceed the value set here, it is not
    /// a protocol level error. Instead, the `h2` library will immediately reset
    /// the stream.
    ///
    /// See [Section 5.1.2] in the HTTP/2 spec for more details.
    ///
    /// [Section 5.1.2]: https://http2.github.io/http2-spec/#rfc.section.5.1.2
    pub fn max_concurrent_streams(mut self, max: u32) -> Self {
        self.settings
            .set_max_concurrent_streams(Some(max));
        self
    }

    /// Sets the header table size.
    ///
    /// This setting informs the peer of the maximum size of the header compression
    /// table used to encode header blocks, in octets. The encoder may select any value
    /// equal to or less than the header table size specified by the sender.
    ///
    /// The default value is 4,096.
    pub fn header_table_size(mut self, size: u32) -> Self {
        self.settings
            .set_header_table_size(Some(size));
        self
    }

    // ===== Reset =====

    /// Sets the maximum number of concurrent locally reset streams.
    ///
    /// When a stream is explicitly reset, the HTTP/2 specification requires
    /// that any further frames received for that stream must be ignored for
    /// "some time".
    ///
    /// In order to satisfy the specification, internal state must be maintained
    /// to implement the behavior. This state grows linearly with the number of
    /// streams that are locally reset.
    ///
    /// The `max_concurrent_reset_streams` setting configures sets an upper
    /// bound on the amount of state that is maintained. When this max value is
    /// reached, the oldest reset stream is purged from memory.
    ///
    /// Once the stream has been fully purged from memory, any additional frames
    /// received for that stream will result in a connection level protocol
    /// error, forcing the connection to terminate.
    ///
    /// The default value is currently 50.
    pub fn max_concurrent_reset_streams(mut self, max: usize) -> Self {
        self.local_reset_stream_max = max;
        self
    }

    /// Sets the duration to remember locally reset streams.
    ///
    /// When a stream is explicitly reset, the HTTP/2 specification requires
    /// that any further frames received for that stream must be ignored for
    /// "some time".
    ///
    /// In order to satisfy the specification, internal state must be maintained
    /// to implement the behavior. This state grows linearly with the number of
    /// streams that are locally reset.
    ///
    /// The `reset_stream_duration` setting configures the max amount of time
    /// this state will be maintained in memory. Once the duration elapses, the
    /// stream state is purged from memory.
    ///
    /// Once the stream has been fully purged from memory, any additional frames
    /// received for that stream will result in a connection level protocol
    /// error, forcing the connection to terminate.
    ///
    /// The default value is currently 1 second.
    pub fn reset_stream_duration(mut self, dur: Duration) -> Self {
        self.reset_stream_duration = dur;
        self
    }

    /// Sets the maximum number of local resets due to protocol errors made by
    /// the remote end.
    ///
    /// Invalid frames and many other protocol errors will lead to resets being
    /// generated for those streams.
    /// Too many of these often indicate a malicious client, and there are
    /// attacks which can abuse this to DOS servers. This limit protects
    /// against these DOS attacks by limiting the amount of resets we can be
    /// forced to generate.
    ///
    /// When the number of local resets exceeds this threshold, the client will
    /// close the connection.
    ///
    /// If you really want to disable this, supply [`Option::None`] here.
    /// Disabling this is not recommended and may expose you to DOS attacks.
    ///
    /// The default value is currently 1024, but could change.
    pub fn max_local_error_reset_streams(
        mut self,
        max: Option<usize>,
    ) -> Self {
        self.local_max_error_reset_streams = max;
        self
    }

    /// Sets the maximum number of pending-accept remotely-reset streams.
    ///
    /// Streams that have been received by the peer, but not accepted by the
    /// user, can also receive a RST_STREAM. This is a legitimate pattern: one
    /// could send a request and then shortly after, realize it is not needed,
    /// sending a CANCEL.
    ///
    /// However, since those streams are now "closed", they don't count towards
    /// the max concurrent streams. So, they will sit in the accept queue,
    /// using memory.
    ///
    /// When the number of remotely-reset streams sitting in the pending-accept
    /// queue reaches this maximum value, a connection error with the code of
    /// `ENHANCE_YOUR_CALM` will be sent to the peer, and returned by the
    /// `Future`.
    ///
    pub fn max_pending_accept_reset_streams(mut self, max: usize) -> Self {
        self.remote_reset_stream_max = max;
        self
    }

    pub fn enable_connect_protocol(mut self) -> Self {
        self.settings
            .set_enable_connect_protocol(Some(1));
        self
    }

    // TODO
    // Sets the first stream ID to something other than 1.
    //pub fn initial_stream_id(&mut self, stream_id: u32) -> &mut Self {
    //    self.stream_id = stream_id.into();
    //    assert!(self.stream_id.is_client_initiated(), "stream id must be odd");
    //    self
    //}

    pub fn max_recv_buffer_size(mut self, size: usize) -> Self {
        self.max_recv_buffer_size = size;
        self
    }

    /// Starts an HTTP/2 connection.
    ///
    /// For clients, this returns after the connection preface and initial
    /// `SETTINGS` frame have been flushed. The returned connection future
    /// receives and validates the peer's initial `SETTINGS`, so requests may be
    /// queued before the peer preface arrives. Servers complete the full peer
    /// preface exchange before this returns.
    pub async fn handshake<T>(
        mut self,
        io: T,
    ) -> Result<R::Connection<T>, PrefaceError>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let role = if R::is_client() {
            Role::Client
        } else {
            Role::Server
        };

        let (stream, remote_settings) =
            PrefaceConn::handshake(io, role.clone(), self.settings.clone())
                .await?;

        let config = self.build_config(remote_settings);
        Ok(R::build(role, config, stream))
    }

    fn build_config(
        &mut self,
        peer_settings: frame::Settings,
    ) -> ConnectionConfig {
        ConnectionConfig {
            initial_connection_window_size: self
                .initial_connection_window_size,
            local_max_error_reset_streams: self.local_max_error_reset_streams,
            reset_stream_duration: self.reset_stream_duration,
            local_reset_stream_max: self.local_reset_stream_max,
            remote_reset_stream_max: self.remote_reset_stream_max,
            local_settings: self.settings.clone(),
            peer_settings,
            spa_tracker: self
                .role
                .take_spa_mode()
                .map(Into::into),
            max_recv_buffer_size: self.max_recv_buffer_size,
        }
    }
}
