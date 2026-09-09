use crate::{error::ChannelError, BucketClient, Result};
use futures::StreamExt;
use metrics::Label;
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
    /// Every bucket a file is written to. Always at least one; each gets the
    /// same key and the same bytes.
    buckets: Vec<BucketClient>,
    completion_tx: std::sync::Arc<watch::Sender<u64>>,
    max_retries: u8,
    retry_wait: Duration,
}

const DEFAULT_MAX_RETRIES: u8 = 5;
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(10);

/// Terminal upload outcome per file per bucket, labeled `bucket` and `status`
/// (`ok` | `error`).
///
/// One increment per file per bucket, once that bucket has either stored the
/// file or exhausted its retries — not one per attempt. A bucket that quietly
/// stops accepting files is otherwise invisible from the outside: the uploader
/// keeps the local copy and logs an error, while the service goes on serving
/// traffic and looking healthy. Alert on `status="error"` per bucket.
pub const UPLOAD_METRIC: &str = "file_store_upload";

const OK_LABEL: Label = Label::from_static_parts("status", "ok");
const ERROR_LABEL: Label = Label::from_static_parts("status", "error");

fn upload_counter(bucket: &str, status: Label) -> metrics::Counter {
    metrics::counter!(
        UPLOAD_METRIC,
        vec![Label::new("bucket", bucket.to_string()), status]
    )
}

impl FileUpload {
    pub async fn new(client: crate::Client, bucket: String) -> (Self, FileUploadServer) {
        Self::from_bucket_client(BucketClient { client, bucket }).await
    }

    pub async fn from_bucket_client(bucket_client: BucketClient) -> (Self, FileUploadServer) {
        Self::with_additional_buckets(bucket_client, vec![]).await
    }

    /// Uploads every file to `bucket` and, byte for byte and under the same
    /// key, to each of `additional_buckets`.
    ///
    /// A local file is only deleted once every bucket has it, so a bucket that
    /// is failing never costs the others their copy. Nothing re-queues a file
    /// whose upload was abandoned, so a file left behind stays in the cache
    /// directory until the process restarts — [`crate::file_sink::FileSink`]
    /// rescans that directory on startup and queues what it finds.
    pub async fn with_additional_buckets(
        bucket: BucketClient,
        additional_buckets: Vec<BucketClient>,
    ) -> (Self, FileUploadServer) {
        let mut buckets = Vec::with_capacity(1 + additional_buckets.len());
        buckets.push(bucket);
        buckets.extend(additional_buckets);

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
                buckets,
                completion_tx,
                max_retries: DEFAULT_MAX_RETRIES,
                retry_wait: DEFAULT_RETRY_WAIT,
            },
        )
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
    ///
    /// Counted per file, not per bucket: one file fanned out to three buckets
    /// counts once, when the last bucket is done with it.
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
    /// The buckets this server writes each file to, in configured order.
    pub fn buckets(&self) -> Vec<&str> {
        self.buckets.iter().map(|b| b.bucket.as_str()).collect()
    }

    /// Overrides how hard a single bucket is retried before the file is
    /// abandoned. Intended for tests, which cannot afford to sit through the
    /// production backoff to observe what happens when a bucket never accepts
    /// the file.
    pub fn with_retry_policy(mut self, max_retries: u8, retry_wait: Duration) -> Self {
        self.max_retries = max_retries;
        self.retry_wait = retry_wait;
        self
    }

    pub async fn run(self, shutdown: triggered::Listener) -> Result {
        let bucket_names = self.buckets().join(", ");
        tracing::info!("starting file uploader {bucket_names}");

        // Seed both series per bucket so a bucket that has never failed reports
        // zero rather than being absent. Without this an alert cannot tell "no
        // failures" from "this uploader is not reporting at all".
        for bucket in self.buckets() {
            upload_counter(bucket, OK_LABEL).increment(0);
            upload_counter(bucket, ERROR_LABEL).increment(0);
        }

        let Self {
            messages,
            buckets,
            completion_tx,
            max_retries,
            retry_wait,
        } = self;
        let buckets = &buckets;
        let completion_tx = &completion_tx;
        let bucket_names = &bucket_names;

        let uploads = messages.for_each_concurrent(5, |path| async move {
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

            // Fan the same file out to every bucket. Each bucket re-reads it
            // from disk, so all of them get identical bytes under an identical
            // key.
            let all_stored = futures::future::join_all(
                buckets
                    .iter()
                    .map(|bucket| put_with_retries(bucket, &path, max_retries, retry_wait)),
            )
            .await
            .into_iter()
            .all(|stored| stored);

            if all_stored {
                match fs::remove_file(&path).await {
                    Ok(()) => {
                        tracing::info!("stored {path_str} in {bucket_names}");
                    }
                    Err(err) => {
                        tracing::error!("failed to remove uploaded file {path_str}: {err:?}");
                    }
                }
            } else {
                // Keeping the file is what makes a partial fan-out recoverable:
                // an operator still has the bytes that one of the buckets never
                // received.
                tracing::error!("keeping {path_str}: not stored in every bucket");
            }

            completion_tx.send_modify(|n| *n += 1);
        });

        tokio::select! {
            _ = uploads => (),
            _ = shutdown.clone() => (),
        }

        tracing::info!("stopping file uploader {bucket_names}");
        Ok(())
    }
}

/// Uploads a single file to a single bucket, retrying on failure. Returns
/// whether the bucket ended up with the file.
async fn put_with_retries(
    bucket_client: &BucketClient,
    path: &Path,
    max_retries: u8,
    retry_wait: Duration,
) -> bool {
    let path_str = path.display();
    let bucket = &bucket_client.bucket;
    let mut retry = 0;

    while retry <= max_retries {
        tracing::debug!("storing {path_str} in {bucket} retry {retry}");
        match bucket_client.put_file(path).await {
            Ok(()) => {
                upload_counter(bucket, OK_LABEL).increment(1);
                return true;
            }
            Err(err) => {
                tracing::error!("failed to store {path_str} in {bucket} retry: {retry}: {err:?}");
                retry += 1;
                time::sleep(retry_wait).await;
            }
        }
    }

    upload_counter(bucket, ERROR_LABEL).increment(1);
    tracing::error!("failed to upload {path_str} to {bucket} after {max_retries} retries");
    false
}
