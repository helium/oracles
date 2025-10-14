use crate::{Error, Result};
use futures::{StreamExt, TryFutureExt};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use task_manager::ManagedTask;
use tokio::{fs, sync::mpsc, time};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub type MessageSender = mpsc::UnboundedSender<PathBuf>;
pub type MessageReceiver = mpsc::UnboundedReceiver<PathBuf>;

pub fn message_channel() -> (MessageSender, MessageReceiver) {
    mpsc::unbounded_channel()
}

pub async fn upload_file(tx: &MessageSender, file: &Path) -> Result {
    tx.send(file.to_path_buf()).map_err(|_| Error::channel())
}

#[derive(Debug, Clone)]
pub struct FileUpload {
    pub sender: MessageSender,
}

pub struct FileUploadServer {
    messages: UnboundedReceiverStream<PathBuf>,
    client: crate::Client,
    bucket: String,
}

impl FileUpload {
    pub async fn new(client: crate::Client, bucket: String) -> (Self, FileUploadServer) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self { sender },
            FileUploadServer {
                messages: UnboundedReceiverStream::new(receiver),
                client,
                bucket,
            },
        )
    }

    pub async fn upload_file(&self, file: &Path) -> Result {
        self.sender
            .send(file.to_path_buf())
            .map_err(|_| Error::channel())
    }
}

impl ManagedTask for FileUploadServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
        task_manager::spawn(self.run(shutdown).err_into())
    }
}

impl FileUploadServer {
    pub async fn run(self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting file uploader {}", self.bucket);

        let client = &self.client;
        let bucket = &self.bucket;

        let uploads = self.messages.for_each_concurrent(5, |path| async move {
            let path_str = path.display();
            if !path.exists() {
                tracing::warn!("ignoring absent file {path_str}");
                return;
            }
            if !path.is_file() {
                tracing::warn!("ignoring non file {path_str}");
                return;
            }
            let mut retry = 0;
            const MAX_RETRIES: u8 = 5;
            const RETRY_WAIT: Duration = Duration::from_secs(10);
            while retry <= MAX_RETRIES {
                tracing::debug!("storing {path_str} in {bucket} retry {retry}");
                match crate::put_file(client, bucket, &path).await {
                    Ok(()) => {
                        match fs::remove_file(&path).await {
                            Ok(()) => {
                                tracing::info!("stored {path_str} in {bucket}");
                            }
                            Err(err) => {
                                tracing::error!(
                                    "failed to remove uploaded file {path_str}: {err:?}"
                                );
                            }
                        }
                        return;
                    }
                    Err(err) => {
                        tracing::error!(
                            "failed to store {path_str} in {bucket} retry: {retry}: {err:?}"
                        );
                        retry += 1;
                        time::sleep(RETRY_WAIT).await;
                    }
                }
            }
        });

        tokio::select! {
            _ = uploads => (),
            _ = shutdown.clone() => (),
        }

        tracing::info!("stopping file uploader {}", self.bucket);
        Ok(())
    }
}
