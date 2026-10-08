use std::fmt;

use super::prelude::*;
use bytes::Bytes;

use two_plz::frame::{self, Frame, StreamId, headers::Pseudo};

pub const NEW_SETTINGS: &[u8] = &[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0];
pub const SETTINGS: &[u8] = &[0, 0, 0, 4, 0, 0, 0, 0, 0];
pub const SETTINGS_ACK: &[u8] = &[0, 0, 0, 4, 1, 0, 0, 0, 0];

// ==== helper functions to easily construct h2 Frames ====

pub fn headers<T>(id: T) -> Mock<frame::Headers>
where
    T: Into<StreamId>,
{
    Mock(frame::Headers::new(
        id.into(),
        Pseudo::default(),
        HeaderMap::default(),
    ))
}

pub fn data<T, B>(id: T, buf: B) -> Mock<frame::Data>
where
    T: Into<StreamId>,
    B: AsRef<[u8]>,
{
    let buf = Bytes::copy_from_slice(buf.as_ref());
    Mock(frame::Data::new(id.into(), buf))
}

pub fn push_promise<T1, T2>(id: T1, promised: T2) -> Mock<frame::PushPromise>
where
    T1: Into<StreamId>,
    T2: Into<StreamId>,
{
    Mock(frame::PushPromise::new(
        id.into(),
        promised.into(),
        Pseudo::default(),
        HeaderMap::default(),
    ))
}

pub fn window_update<T>(id: T, sz: u32) -> frame::WindowUpdate
where
    T: Into<StreamId>,
{
    frame::WindowUpdate::new(id.into(), sz)
}

pub fn go_away<T>(id: T) -> Mock<frame::GoAway>
where
    T: Into<StreamId>,
{
    Mock(frame::GoAway::new(id.into(), frame::Reason::NO_ERROR))
}

pub fn reset<T>(id: T) -> Mock<frame::Reset>
where
    T: Into<StreamId>,
{
    Mock(frame::Reset::new(id.into(), frame::Reason::NO_ERROR))
}

pub fn new_settings() -> Mock<frame::Settings> {
    let mut settings = frame::Settings::default();
    settings.set_enable_push(false);
    Mock(settings)
}

pub fn settings() -> Mock<frame::Settings> {
    Mock(frame::Settings::default())
}

pub fn settings_ack() -> Mock<frame::Settings> {
    Mock(frame::Settings::ack())
}

pub fn ping(payload: [u8; 8]) -> Mock<frame::Ping> {
    Mock(frame::Ping::new(payload))
}

// === Generic helpers of all frame types

pub struct Mock<T>(T);

impl<T: fmt::Debug> fmt::Debug for Mock<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl<T> From<Mock<T>> for Frame
where
    T: Into<Frame>,
{
    fn from(src: Mock<T>) -> Self {
        src.0.into()
    }
}

// Headers helpers

impl Mock<frame::Headers> {
    pub fn request<M>(
        self,
        method: M,
        scheme: &str,
        authority: &str,
        path: &str,
    ) -> Self
    where
        M: TryInto<Method>,
        M::Error: fmt::Debug,
    {
        let method = method.try_into().unwrap();
        let mut b = Uri::builder();
        if !authority.is_empty() {
            b = b.authority(authority);
        }
        b = b.path(path);
        b = b.scheme(Scheme::try_from(scheme.as_bytes()).unwrap());
        let uri = b.build().unwrap();
        let (id, _, fields) = self.into_parts();
        let extensions = Default::default();
        let pseudo = Pseudo::request(method, uri, extensions);
        let frame = frame::Headers::new(id, pseudo, fields);
        Mock(frame)
    }

    pub fn method<M>(self, method: M) -> Self
    where
        M: TryInto<Method>,
        M::Error: fmt::Debug,
    {
        let method = method.try_into().unwrap();
        let (id, pseudo, fields) = self.into_parts();
        let frame = frame::Headers::new(
            id,
            Pseudo {
                method: Some(method),
                ..pseudo
            },
            fields,
        );
        Mock(frame)
    }

    pub fn pseudo(self, pseudo: Pseudo) -> Self {
        let (id, _, fields) = self.into_parts();
        let frame = frame::Headers::new(id, pseudo, fields);
        Mock(frame)
    }

    pub fn response<S>(self, status: S) -> Self
    where
        S: TryInto<StatusCode>,
        S::Error: fmt::Debug,
    {
        let status = status.try_into().unwrap();
        let (id, _, fields) = self.into_parts();
        let frame = frame::Headers::new(id, Pseudo::response(status), fields);
        Mock(frame)
    }

    pub fn fields(self, fields: HeaderMap) -> Self {
        let (id, pseudo, _) = self.into_parts();
        let frame = frame::Headers::new(id, pseudo, fields);
        Mock(frame)
    }

    pub fn field<K, V>(self, key: K, value: V) -> Self
    where
        V: AsRef<[u8]>,
        K: AsRef<[u8]>,
    {
        let (id, pseudo, mut fields) = self.into_parts();
        fields.insert(key, value);
        let frame = frame::Headers::new(id, pseudo, fields);
        Mock(frame)
    }

    pub fn status(self, value: StatusCode) -> Self {
        let (id, mut pseudo, fields) = self.into_parts();
        pseudo.set_status(value);
        Mock(frame::Headers::new(id, pseudo, fields))
    }

    pub fn scheme(self, value: &str) -> Self {
        let (id, mut pseudo, fields) = self.into_parts();
        let value = Scheme::try_from(value.as_bytes()).unwrap();
        pseudo.set_scheme(value);
        Mock(frame::Headers::new(id, pseudo, fields))
    }

    pub fn eos(mut self) -> Self {
        self.0.set_end_stream();
        self
    }

    pub fn into_fields(self) -> HeaderMap {
        self.0.into_parts().1
    }

    fn into_parts(self) -> (StreamId, Pseudo, HeaderMap) {
        assert!(!self.0.is_end_stream(), "eos flag will be lost");
        assert!(self.0.is_end_headers(), "unset eoh will be lost");
        let id = self.0.stream_id();
        let parts = self.0.into_parts();
        (id, parts.0, parts.1)
    }
}

impl From<Mock<frame::Headers>> for frame::Headers {
    fn from(src: Mock<frame::Headers>) -> Self {
        src.0
    }
}

// Data helpers

impl Mock<frame::Data> {
    pub fn padded(mut self) -> Self {
        self.0.set_padded();
        self
    }

    pub fn eos(mut self) -> Self {
        self.0.set_end_stream(true);
        self
    }
}

// GoAway helpers

impl Mock<frame::GoAway> {
    pub fn protocol_error(self) -> Self {
        self.reason(frame::Reason::PROTOCOL_ERROR)
    }

    pub fn internal_error(self) -> Self {
        self.reason(frame::Reason::INTERNAL_ERROR)
    }

    pub fn flow_control(self) -> Self {
        self.reason(frame::Reason::FLOW_CONTROL_ERROR)
    }

    pub fn frame_size(self) -> Self {
        self.reason(frame::Reason::FRAME_SIZE_ERROR)
    }

    pub fn calm(self) -> Self {
        self.reason(frame::Reason::ENHANCE_YOUR_CALM)
    }

    pub fn no_error(self) -> Self {
        self.reason(frame::Reason::NO_ERROR)
    }

    pub fn data<I>(self, debug_data: I) -> Self
    where
        I: Into<Bytes>,
    {
        Mock(frame::GoAway::with_debug_data(
            self.0.last_stream_id(),
            self.0.reason(),
            debug_data.into(),
        ))
    }

    pub fn reason(self, reason: frame::Reason) -> Self {
        Mock(frame::GoAway::with_debug_data(
            self.0.last_stream_id(),
            reason,
            self.0.debug_data().clone(),
        ))
    }
}

// ==== Reset helpers

impl Mock<frame::Reset> {
    pub fn protocol_error(self) -> Self {
        let id = self.0.stream_id();
        Mock(frame::Reset::new(id, frame::Reason::PROTOCOL_ERROR))
    }

    pub fn flow_control(self) -> Self {
        let id = self.0.stream_id();
        Mock(frame::Reset::new(id, frame::Reason::FLOW_CONTROL_ERROR))
    }

    pub fn refused(self) -> Self {
        let id = self.0.stream_id();
        Mock(frame::Reset::new(id, frame::Reason::REFUSED_STREAM))
    }

    pub fn cancel(self) -> Self {
        let id = self.0.stream_id();
        Mock(frame::Reset::new(id, frame::Reason::CANCEL))
    }

    pub fn stream_closed(self) -> Self {
        let id = self.0.stream_id();
        Mock(frame::Reset::new(id, frame::Reason::STREAM_CLOSED))
    }

    pub fn internal_error(self) -> Self {
        let id = self.0.stream_id();
        Mock(frame::Reset::new(id, frame::Reason::INTERNAL_ERROR))
    }

    pub fn reason(self, reason: frame::Reason) -> Self {
        let id = self.0.stream_id();
        Mock(frame::Reset::new(id, reason))
    }
}

// ==== Settings helpers

impl Mock<frame::Settings> {
    pub fn max_concurrent_streams(mut self, max: u32) -> Self {
        self.0
            .set_max_concurrent_streams(Some(max));
        self
    }

    pub fn max_frame_size(mut self, val: u32) -> Self {
        self.0.set_max_frame_size(Some(val));
        self
    }

    pub fn initial_window_size(mut self, val: u32) -> Self {
        self.0
            .set_initial_window_size(Some(val));
        self
    }

    pub fn max_header_list_size(mut self, val: u32) -> Self {
        self.0
            .set_max_header_list_size(Some(val));
        self
    }

    pub fn disable_push(mut self) -> Self {
        self.0.set_enable_push(false);
        self
    }

    pub fn enable_connect_protocol(mut self, val: u32) -> Self {
        self.0
            .set_enable_connect_protocol(Some(val));
        self
    }

    pub fn header_table_size(mut self, val: u32) -> Self {
        self.0.set_header_table_size(Some(val));
        self
    }
}

impl From<Mock<frame::Settings>> for frame::Settings {
    fn from(src: Mock<frame::Settings>) -> Self {
        src.0
    }
}

// ==== Ping helpers

impl Mock<frame::Ping> {
    pub fn pong(self) -> Self {
        let payload = self.0.into_payload();
        Mock(frame::Ping::pong(payload))
    }
}
