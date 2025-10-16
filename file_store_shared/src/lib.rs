pub mod error;
pub mod msg_decode;
pub mod timestamp;

pub use error::{Error, Result};
pub mod traits {
    pub use crate::msg_decode::MsgDecode;
    pub use crate::timestamp::{TimestampDecode, TimestampEncode};
}
