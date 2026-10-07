#![allow(dead_code)]
#![allow(
    clippy::new_without_default,
    clippy::large_enum_variant,
    clippy::result_large_err,
    clippy::len_without_is_empty,
    clippy::too_many_arguments
)]

macro_rules! proto_err {
    (conn: $($msg:tt)+) => {
        tracing::debug!("connection error PROTOCOL_ERROR -- {};", format_args!($($msg)+))
    };
    (stream: $($msg:tt)+) => {
        tracing::debug!("stream error PROTOCOL_ERROR -- {};", format_args!($($msg)+))
    };
}

macro_rules! ready {
    ($e:expr) => {
        match $e {
            ::std::task::Poll::Ready(r) => r,
            ::std::task::Poll::Pending => return ::std::task::Poll::Pending,
        }
    };
}

pub mod builder;
pub mod client;
pub mod codec;
pub mod error;
pub mod ext;
pub mod frame;
pub mod message;
pub mod preface;
pub mod proto;
pub mod role;
pub mod server;
pub use crate::proto::connection::Connection;
pub use codec::Codec;
pub use message::{BodyFrame, RecvBody, SendBody};
pub mod tunnel;
pub use tunnel::Tunnel;
pub mod hpack;
pub mod spa;
