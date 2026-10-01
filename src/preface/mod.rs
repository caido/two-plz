use crate::codec::Codec;
use crate::error::Reason;
use crate::frame::{self, Frame, Settings, StreamId};
use crate::proto::ProtoError;
use crate::role::Role;
use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tracing::trace;
mod error;
pub use error::*;

/*
 client MUST => preface
 client MUST => SETTINGS
                SETTINGS     <= server MUST
 client MAY  => Frames
 client MUST => SETTINGS ack
                SETTINGS ack <= server MUST

client state
    1. Send Preface (24 bytes)
    2. Send SETTINGS
    3. Start sending requests
    4. Read first peer frame → MUST be server SETTINGS
    5. Send SETTINGS ACK

server state
    1. Send SETTINGS
    2. Flush SETTINGS to socket
    3. Read Preface (24 bytes)
    4. Read first frame → MUST be client SETTINGS
    5. Send SETTINGS ACK
    6. Start receiving/responding to requests
*/

const PREFACE: [u8; 24] = *b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub struct PrefaceConn<T>(T);

impl<T> PrefaceConn<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn handshake(
        io: T,
        role: Role,
        local_settings: Settings,
    ) -> Result<(Codec<T, Bytes>, Settings), PrefaceError> {
        match role {
            Role::Client => Self::client_handshake(io, local_settings).await,
            Role::Server => Self::server_handshake(io, local_settings).await,
        }
    }

    async fn client_handshake(
        mut io: T,
        local_settings: Settings,
    ) -> Result<(Codec<T, Bytes>, Settings), PrefaceError> {
        // 1. Send preface
        io.write_all(&PREFACE)
            .await
            .ctx("write preface")?;

        let mut codec = Codec::new(io);

        // 2. Send local SETTINGS
        codec
            .buffer(local_settings.into())
            .ctx("buffer local settings")?;

        Self::flush(&mut codec)
            .await
            .ctx("flush local settings")?;

        // The connection is now write-ready. The connection driver receives,
        // validates, applies, and acknowledges the peer's initial SETTINGS.
        Ok((codec, Settings::default()))
    }

    async fn server_handshake(
        io: T,
        local_settings: Settings,
    ) -> Result<(Codec<T, Bytes>, Settings), PrefaceError> {
        let mut codec = Codec::new(io);

        // 1. Send local SETTINGS
        codec
            .buffer(local_settings.clone().into())
            .ctx("buffer local settings")?;
        Self::flush(&mut codec)
            .await
            .ctx("flush local settings")?;

        // 2. Read preface (24 bytes)
        Self::read_preface(codec.get_mut()).await?;

        // 3. Read peer SETTINGS
        let remote_settings = Self::read_peer_settings(&mut codec).await?;

        // 4. Apply peer settings
        Self::apply_peer_settings(&mut codec, &remote_settings);

        // 5. Send SETTINGS ACK
        codec
            .buffer(Settings::ack().into())
            .ctx("buffer settings ack")?;

        Self::flush(&mut codec)
            .await
            .ctx("flush settings ack")?;

        Ok((codec, remote_settings))
    }

    async fn read_preface(io: &mut T) -> Result<(), PrefaceError> {
        trace!("[+] read preface");
        let mut buf = [0u8; 24];
        io.read_exact(&mut buf)
            .await
            .ctx("read preface")?;

        if buf != PREFACE {
            return Err(PrefaceError::InvalidPreface);
        }
        Ok(())
    }

    async fn read_peer_settings(
        codec: &mut Codec<T, Bytes>,
    ) -> Result<Settings, PrefaceError> {
        trace!("[+] read peer settings");
        let frame = codec
            .next()
            .await
            .ok_or(PrefaceError::Eof)?;

        match frame {
            // Success case - received SETTINGS
            Ok(Frame::Settings(settings)) => Ok(settings),

            // Wrong frame type
            Ok(other) => Err(PrefaceError::WrongFrame(other.kind())),

            // GOAWAY from peer - send response before failing
            Err(ProtoError::GoAway(_, reason, _)) => {
                Self::send_goaway_response(codec, reason).await?;
                Err(PrefaceError::Proto {
                    context: "peer sent GOAWAY",
                    source: ProtoError::library_go_away(reason),
                })
            }

            // Other protocol errors
            Err(e) => Err(PrefaceError::Proto {
                context: "decode settings frame",
                source: e,
            }),
        }
    }

    fn apply_peer_settings(codec: &mut Codec<T, Bytes>, settings: &Settings) {
        trace!("[+] applying peer settings| {:?}", settings);
        if let Some(val) = settings.header_table_size() {
            codec.set_send_header_table_size(val as usize);
        }
        if let Some(val) = settings.max_frame_size() {
            codec.set_max_send_frame_size(val as usize);
        }
    }

    async fn send_goaway_response(
        codec: &mut Codec<T, Bytes>,
        reason: Reason,
    ) -> Result<(), PrefaceError> {
        let goaway = frame::GoAway::new(StreamId::ZERO, reason);
        codec
            .buffer(goaway.into())
            .ctx("buffer GOAWAY response")?;
        Self::flush(codec)
            .await
            .ctx("flush GOAWAY response")
    }

    async fn flush(codec: &mut Codec<T, Bytes>) -> Result<(), std::io::Error> {
        use futures::future::poll_fn;
        poll_fn(|cx| codec.flush(cx)).await
    }
}
