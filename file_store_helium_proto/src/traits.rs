mod msg_verify;

pub use msg_verify::MsgVerify;

pub trait MsgTimestamp<R> {
    fn timestamp(&self) -> R;
}
