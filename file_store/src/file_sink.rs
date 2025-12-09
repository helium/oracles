pub mod builder;
pub mod client;
pub mod gzipped_framed_file;
pub mod rolling_file_sink;
pub mod server;

pub const DEFAULT_SINK_ROLL_SECS: u64 = 3 * 60;

#[cfg(not(test))]
pub const SINK_CHECK_MILLIS: u64 = 60_000;
#[cfg(test)]
pub const SINK_CHECK_MILLIS: u64 = 50;

pub const MAX_FRAME_LENGTH: usize = 15_000_000;

pub mod manifest {
    use std::path::PathBuf;

    #[derive(thiserror::Error, Debug)]
    #[error("expected sink filename: {0}")]
    pub struct FileManifestError(PathBuf);

    #[derive(Debug, Default)]
    pub struct FileManifest(Vec<String>);

    impl FileManifest {
        pub fn into_inner(self) -> Vec<String> {
            self.0
        }

        pub fn push(&mut self, path_buf: PathBuf) -> Result<(), FileManifestError> {
            let filename = path_buf
                .file_name()
                .map(|os_str| os_str.to_string_lossy().to_string())
                .ok_or(FileManifestError(path_buf))?;

            self.0.push(filename);

            Ok(())
        }
    }
}

pub mod message {
    use tokio::sync::{mpsc, oneshot};

    use crate::file_sink::{manifest::FileManifest, server};

    #[derive(Debug)]
    pub enum Message<T> {
        Data(oneshot::Sender<server::Result>, T),
        Commit(oneshot::Sender<server::Result<FileManifest>>),
        Rollback(oneshot::Sender<server::Result<FileManifest>>),
    }

    pub type MessageSender<T> = mpsc::Sender<Message<T>>;
    pub type MessageReceiver<T> = mpsc::Receiver<Message<T>>;

    pub fn message_channel<T>(size: usize) -> (MessageSender<T>, MessageReceiver<T>) {
        mpsc::channel(size)
    }
}

#[cfg(test)]
mod file_sink_tests;
