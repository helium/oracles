pub use file_store_shared::traits::{MsgDecode, TimestampDecode, TimestampEncode};

pub trait MsgTimestamp<R> {
    fn timestamp(&self) -> R;
}
