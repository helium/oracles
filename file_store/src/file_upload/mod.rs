use crate::{error::ChannelError, Error, Result};
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::sync::mpsc;

pub mod file_uploader;
pub mod multi_file_upload;
pub mod single_file_upload;

pub use file_uploader::FileUploader;
pub use multi_file_upload::MultiFileUpload;
pub use single_file_upload::{FileUpload, FileUploadServer, UPLOAD_METRIC};

pub type MessageSender = mpsc::UnboundedSender<PathBuf>;
pub type MessageReceiver = mpsc::UnboundedReceiver<PathBuf>;

pub fn message_channel() -> (MessageSender, MessageReceiver) {
    mpsc::unbounded_channel()
}

pub async fn upload_file(tx: &MessageSender, file: &Path) -> Result {
    tx.send(file.to_path_buf())
        .map_err(|_| ChannelError::upload_closed(file))
}

fn invalid_input(message: impl Into<String>) -> Error {
    Error::from(std::io::Error::new(ErrorKind::InvalidInput, message.into()))
}
