pub use file_store::traits::{MsgDecode, TimestampDecode, TimestampEncode};

pub trait MsgTimestamp<R> {
    fn timestamp(&self) -> R;
}
