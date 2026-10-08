use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::io::AsyncWrite;
use tracing::{error, trace};

use crate::{
    Codec,
    frame::Reason,
    frame::Settings,
    proto::{self, ProtoError, streams::Streams},
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_limits_are_applied_only_by_their_ack() {
        let mut settings = Settings::default();
        settings.set_header_table_size(Some(128));
        let mut handler = SettingsHandler::new(settings);
        assert!(matches!(
            handler
                .recv(Settings::default())
                .unwrap(),
            SettingsAction::Ok
        ));
        match handler.recv(Settings::ack()).unwrap() {
            SettingsAction::ApplyLocal(local) => {
                assert_eq!(local.header_table_size(), Some(128))
            }
            SettingsAction::Ok => panic!("local ACK must apply local limits"),
        }
        assert!(handler.recv(Settings::ack()).is_err());
        // The current protocol serializes local SETTINGS. Model successive
        // sends through the private waiting state to verify each ACK separately.
        for limit in [0, 256, 64, 128] {
            let mut local = Settings::default();
            local.set_header_table_size(Some(limit));
            handler.local = Local::WaitingAck(local);
            match handler.recv(Settings::ack()).unwrap() {
                SettingsAction::ApplyLocal(applied) => {
                    assert_eq!(applied.header_table_size(), Some(limit))
                }
                SettingsAction::Ok => panic!("ACK lost its local settings"),
            }
        }
    }
}

pub enum SettingsAction {
    /// send a SETTINGS ACK for remote SETTINGS
    Ok,
    /// SETTINGS ACK received from peer apply local settings
    ApplyLocal(Settings),
}

#[derive(Debug)]
pub(crate) struct SettingsHandler {
    /// Our local SETTINGS sync state with the remote.
    local: Local,
    /// Received SETTINGS frame pending processing. The ACK must be written to
    /// the socket first then the settings applied **before** receiving any
    /// further frames.
    remote: Option<Settings>,
}

#[derive(Debug)]
enum Local {
    /// We want to send these SETTINGS to the remote when the socket is ready.
    _ToSend(Settings),
    /// We have sent these SETTINGS and are waiting for the remote to ACK
    /// before we apply them.
    WaitingAck(Settings),
    /// Our local settings are in sync with the remote.
    Synced,
}

impl SettingsHandler {
    pub(crate) fn new(local: Settings) -> Self {
        SettingsHandler {
            // initial local SETTINGS were flushed during the handshake process
            // and is waiting for ACK from peer
            local: Local::WaitingAck(local),
            remote: None,
        }
    }

    pub fn recv(
        &mut self,
        frame: Settings,
    ) -> Result<SettingsAction, proto::ProtoError> {
        if frame.is_ack() {
            match &self.local {
                Local::WaitingAck(settings) => {
                    let ret = SettingsAction::ApplyLocal(settings.clone());
                    self.local = Local::Synced;
                    Ok(ret)
                }
                Local::_ToSend(..) | Local::Synced => {
                    // We haven't sent any SETTINGS frames to be ACKed, so
                    // this is very bizarre! Remote is either buggy or malicious.
                    error!("received unexpected settings ack");
                    Err(proto::ProtoError::library_go_away(
                        Reason::PROTOCOL_ERROR,
                    ))
                }
            }
        } else {
            self.remote = Some(frame);
            Ok(SettingsAction::Ok)
        }
    }

    pub fn poll_remote_settings<T, B>(
        &mut self,
        cx: &mut Context,
        dst: &mut Codec<T, B>,
        streams: &mut Streams<Bytes>,
    ) -> Poll<Result<(), ProtoError>>
    where
        T: AsyncWrite + Unpin,
        B: Buf,
    {
        if let Some(settings) = self.remote.clone() {
            if !dst.poll_ready(cx)?.is_ready() {
                return Poll::Pending;
            }
            // Create an ACK settings frame
            let frame = Settings::ack();
            // Buffer the settings frame
            dst.buffer(frame.into())
                .expect("invalid settings frame");
            trace!("ACK sent| applying settings");
            streams.apply_remote_settings(&settings)?;

            if let Some(val) = settings.header_table_size() {
                dst.set_send_header_table_size(val as usize);
            }

            if let Some(val) = settings.max_frame_size() {
                dst.set_max_send_frame_size(val as usize);
            }
        }
        self.remote = None;
        Poll::Ready(Ok(()))
    }

    pub fn poll_local_settings<T, B>(
        &mut self,
        cx: &mut Context,
        dst: &mut Codec<T, B>,
    ) -> Poll<Result<(), ProtoError>>
    where
        T: AsyncWrite + Unpin,
        B: Buf,
    {
        match &self.local {
            Local::_ToSend(settings) => {
                if !dst.poll_ready(cx)?.is_ready() {
                    return Poll::Pending;
                }
                // Buffer the settings frame
                dst.buffer(settings.clone().into())
                    .expect("invalid settings frame");
                trace!("local settings sent| waiting for ack| {:?}", settings);
                self.local = Local::WaitingAck(settings.clone());
            }
            Local::WaitingAck(..) | Local::Synced => {}
        }
        Poll::Ready(Ok(()))
    }
}
