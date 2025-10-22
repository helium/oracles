mod file_sink_write;
mod msg_verify;

pub use file_sink_write::{
    FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, DEFAULT_ROLL_TIME,
};
pub use msg_verify::MsgVerify;

pub trait MsgTimestamp<R> {
    fn timestamp(&self) -> R;
}
