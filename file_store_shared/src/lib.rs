mod error;
mod msg_decode;
mod timestamp;

pub use error::{DecodeError, EncodeError, Error, Result};
pub mod traits {
    pub use crate::msg_decode::MsgDecode;
    pub use crate::timestamp::{TimestampDecode, TimestampEncode};
}
