mod file_sink_write;

pub use file_sink_write::{
    FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, DEFAULT_ROLL_TIME,
};

pub trait MsgTimestamp<R> {
    fn timestamp(&self) -> R;
}
