use crate::{error::ChannelError, BucketClient, Result};
use futures::StreamExt;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use task_manager::ManagedTask;
use tokio::{
    fs,
    sync::{mpsc, watch},
    time,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub type MessageSender = mpsc::UnboundedSender<PathBuf>;
pub type MessageReceiver = mpsc::UnboundedReceiver<PathBuf>;

pub fn message_channel() -> (MessageSender, MessageReceiver) {
    mpsc::unbounded_channel()
}

pub async fn upload_file(tx: &MessageSender, file: &Path) -> Result {
    tx.send(file.to_path_buf())
        .map_err(|_| ChannelError::upload_closed(file))
}

#[derive(Debug, Clone)]
pub struct FileUpload {
    pub sender: MessageSender,
    completion_rx: watch::Receiver<u64>,
}

pub struct FileUploadServer {
    messages: UnboundedReceiverStream<PathBuf>,
    client: crate::Client,
    bucket: String,
    completion_tx: std::sync::Arc<watch::Sender<u64>>,
}

impl FileUpload {
    pub async fn new(client: crate::Client, bucket: String) -> (Self, FileUploadServer) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (completion_tx, completion_rx) = watch::channel(0u64);
        let completion_tx = std::sync::Arc::new(completion_tx);
        (
            Self {
                sender,
                completion_rx,
            },
            FileUploadServer {
                messages: UnboundedReceiverStream::new(receiver),
                client,
                bucket,
                completion_tx,
            },
        )
    }

    pub async fn from_bucket_client(bucket_client: BucketClient) -> (Self, FileUploadServer) {
        Self::new(bucket_client.client.clone(), bucket_client.bucket.clone()).await
    }

    /// Creates a `FileUpload` from a raw sender with a no-op completion tracker.
    /// Useful in tests that inspect the raw upload channel directly.
    pub fn from_sender(sender: MessageSender) -> Self {
        let (_tx, rx) = watch::channel(0u64);
        Self {
            sender,
            completion_rx: rx,
        }
    }

    pub async fn upload_file(&self, file: &Path) -> Result {
        self.sender
            .send(file.to_path_buf())
            .map_err(|_| ChannelError::upload_closed(file))
    }

    /// Returns the total number of upload attempts that have finished
    /// (success, skipped, or exhausted retries).
    pub fn completed_uploads(&self) -> u64 {
        *self.completion_rx.borrow()
    }

    /// Waits until at least `n` upload attempts have completed in total
    /// (success, skipped, or exhausted retries).
    /// Intended for test use to synchronize on upload completion without
    /// blocking or changing production flows.
    pub async fn wait_for_uploads_at_least(&self, n: u64) {
        let mut rx = self.completion_rx.clone();
        let _ = rx.wait_for(|&count| count >= n).await;
    }
}

impl ManagedTask for FileUploadServer {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl FileUploadServer {
    pub async fn run(self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting file uploader {}", self.bucket);

        let client = &self.client;
        let bucket = &self.bucket;
        let completion_tx = &self.completion_tx;

        let uploads = self.messages.for_each_concurrent(5, |path| async move {
            let path_str = path.display();
            if !path.exists() {
                tracing::warn!("ignoring absent file {path_str}");
                completion_tx.send_modify(|n| *n += 1);
                return;
            }
            if !path.is_file() {
                tracing::warn!("ignoring non file {path_str}");
                completion_tx.send_modify(|n| *n += 1);
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
                        completion_tx.send_modify(|n| *n += 1);
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
            tracing::error!("failed to upload {path_str} after {MAX_RETRIES} retries");
            completion_tx.send_modify(|n| *n += 1);
        });

        tokio::select! {
            _ = uploads => (),
            _ = shutdown.clone() => (),
        }

        tracing::info!("stopping file uploader {}", self.bucket);
        Ok(())
    }
}
