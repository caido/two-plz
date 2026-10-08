use super::prelude::*;
use http_plz::Request;
use two_plz::client::{ResponseFuture, SendRequest};

/// Script the client preface, SETTINGS, and its ACK in wire order.
/// Reading the peer's ACK is deliberately left to the caller.
pub trait MockClientHandshake {
    fn client_handshake(&mut self) -> &mut Self;
    fn client_handshake_with_settings(&mut self, settings: &[u8])
    -> &mut Self;
}

impl MockClientHandshake for tokio_test::io::Builder {
    fn client_handshake(&mut self) -> &mut Self {
        self.client_handshake_with_settings(frames::SETTINGS)
    }

    fn client_handshake_with_settings(
        &mut self,
        settings: &[u8],
    ) -> &mut Self {
        self.write(MAGIC_PREFACE)
            .write(frames::NEW_SETTINGS)
            .read(settings)
            .write(frames::SETTINGS_ACK)
    }
}

/// Extend the `h2::client::SendRequest` type with convenience methods.
pub trait SendRequestExt {
    /// Convenience method to send a GET request and ignore the SendStream
    /// (since GETs don't need to send a body).
    fn get(&mut self, uri: &str) -> ResponseFuture;
}

impl SendRequestExt for SendRequest {
    fn get(&mut self, path: &str) -> ResponseFuture {
        let mut b = Uri::builder();
        b = b.path(path);
        let request = Request::builder()
            .method(Method::GET)
            .uri(b.build().unwrap())
            .build();
        self.send_request(request)
            .expect("send_request")
    }
}
