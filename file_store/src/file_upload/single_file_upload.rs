//! A [`FileUpload`] stores files in one bucket, and [`FileUploadServer`] is the
//! task that drains its queue.

use super::MessageSender;
use crate::{error::ChannelError, file_upload::FileUploader, BucketClient, Result};
use futures::{stream, StreamExt};
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

const DEFAULT_MAX_RETRIES: u8 = 5;
const DEFAULT_RETRY_WAIT: Duration = Duration::from_secs(10);

/// Terminal upload outcome per file per bucket, labeled `bucket` and `status`
/// (`ok` | `error`).
///
/// One increment per file per bucket, once that bucket has either stored the
/// file or exhausted its retries — not one per attempt. A bucket that quietly
/// stops accepting files is otherwise invisible from the outside: the uploader
/// keeps its copy and logs an error, while the service goes on serving traffic
/// and looking healthy. Alert on `status="error"` per bucket.
pub const UPLOAD_METRIC: &str = "file_store_upload";

const OK_LABEL: Label = Label::from_static_parts("status", "ok");
const ERROR_LABEL: Label = Label::from_static_parts("status", "error");

fn upload_counter(bucket: &str, status: Label) -> metrics::Counter {
    metrics::counter!(
        UPLOAD_METRIC,
        vec![Label::new("bucket", bucket.to_string()), status]
    )
}

/// Uploads files to a single bucket.
///
/// Built with [`FileUpload::from_bucket_client`] it uploads files from wherever
/// they are handed to it, and the sink that wrote them is responsible for
/// re-queueing anything left behind.
///
/// Built with [`FileUpload::staged_in`] it instead owns a directory: files live
/// there until the bucket has them, it is the only thing that removes them, and
/// it re-queues whatever it finds there at startup. That is what lets several
/// uploaders share one rolled file without racing each other to delete it, and
/// is required to put one under a [`MultiFileUpload`].
#[derive(Debug, Clone)]
pub struct FileUpload {
    pub sender: MessageSender,
    completion_rx: watch::Receiver<u64>,
    dir: Option<PathBuf>,
}

pub struct FileUploadServer {
    messages: UnboundedReceiverStream<PathBuf>,
    bucket: BucketClient,
    dir: Option<PathBuf>,
    completion_tx: std::sync::Arc<watch::Sender<u64>>,
    max_retries: u8,
    retry_wait: Duration,
}

impl FileUpload {
    pub async fn new(client: crate::Client, bucket: String) -> (Self, FileUploadServer) {
        Self::from_bucket_client(BucketClient { client, bucket }).await
    }

    /// An uploader that stores files wherever they are handed to it.
    pub async fn from_bucket_client(bucket: BucketClient) -> (Self, FileUploadServer) {
        Self::build(bucket, None)
    }

    /// An uploader that owns `dir`, created if missing.
    ///
    /// Files handed to it are expected to live there, it removes them once the
    /// bucket has them, and at startup it re-queues whatever is still there.
    /// Required to place an uploader under a [`MultiFileUpload`].
    pub async fn staged_in(
        bucket: BucketClient,
        dir: impl AsRef<Path>,
    ) -> Result<(Self, FileUploadServer)> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).await?;
        Ok(Self::build(bucket, Some(dir)))
    }

    fn build(bucket: BucketClient, dir: Option<PathBuf>) -> (Self, FileUploadServer) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (completion_tx, completion_rx) = watch::channel(0u64);
        let completion_tx = std::sync::Arc::new(completion_tx);
        (
            Self {
                sender,
                completion_rx,
                dir: dir.clone(),
            },
            FileUploadServer {
                messages: UnboundedReceiverStream::new(receiver),
                bucket,
                dir,
                completion_tx,
                max_retries: DEFAULT_MAX_RETRIES,
                retry_wait: DEFAULT_RETRY_WAIT,
            },
        )
    }

    /// Creates a `FileUpload` from a raw sender with a no-op completion tracker
    /// and no directory. Useful in tests that inspect the raw upload channel
    /// directly and never run the server.
    pub fn from_sender(sender: MessageSender) -> Self {
        let (_tx, rx) = watch::channel(0u64);
        Self {
            sender,
            completion_rx: rx,
            dir: None,
        }
    }

    /// The directory this uploader owns, if it was given one.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    pub async fn upload_file(&self, file: &Path) -> Result {
        self.sender
            .send(file.to_path_buf())
            .map_err(|_| ChannelError::upload_closed(file))
    }

    /// Returns the number of files this uploader has finished with (stored,
    /// skipped, or given up on).
    pub fn completed_uploads(&self) -> u64 {
        *self.completion_rx.borrow()
    }

    /// Waits until at least `n` files have been finished with.
    /// Intended for test use to synchronize on upload completion without
    /// blocking or changing production flows.
    pub async fn wait_for_uploads_at_least(&self, n: u64) {
        let mut rx = self.completion_rx.clone();
        let _ = rx.wait_for(|&count| count >= n).await;
    }
}

#[async_trait::async_trait]
impl FileUploader for FileUpload {
    async fn upload_file(&self, file: &Path) -> Result {
        FileUpload::upload_file(self, file).await
    }
}

impl ManagedTask for FileUploadServer {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl FileUploadServer {
    pub fn bucket(&self) -> &str {
        &self.bucket.bucket
    }

    /// Overrides how hard the bucket is retried before a file is left for the
    /// next startup. Intended for tests, which cannot afford to sit through the
    /// production backoff to observe what happens when a bucket never accepts
    /// the file.
    pub fn with_retry_policy(mut self, max_retries: u8, retry_wait: Duration) -> Self {
        self.max_retries = max_retries;
        self.retry_wait = retry_wait;
        self
    }

    pub async fn run(self, shutdown: triggered::Listener) -> Result {
        let Self {
            messages,
            bucket,
            dir,
            completion_tx,
            max_retries,
            retry_wait,
        } = self;

        let bucket_name = bucket.bucket.clone();
        tracing::info!("starting file uploader {bucket_name}");

        // Seed both series so a bucket that has never failed reports zero
        // rather than being absent. Without this an alert cannot tell "no
        // failures" from "this uploader is not reporting at all".
        upload_counter(&bucket_name, OK_LABEL).increment(0);
        upload_counter(&bucket_name, ERROR_LABEL).increment(0);

        // Whatever is already in our own directory is ours to finish: files
        // from a run that ended before this bucket had them. Queued ahead of new
        // work so a backlog drains first.
        let staged = match &dir {
            Some(dir) => {
                let staged = staged_files(dir).await?;
                if !staged.is_empty() {
                    tracing::info!(
                        "{bucket_name} resuming {} staged file(s) from {}",
                        staged.len(),
                        dir.display()
                    );
                }
                staged
            }
            // An uploader without a directory of its own does not own the files
            // it is handed, so it has nothing to resume — the sink that wrote
            // them re-queues those.
            None => Vec::new(),
        };

        let bucket = &bucket;
        let completion_tx = &completion_tx;
        let bucket_name = &bucket_name;

        let uploads =
            stream::iter(staged)
                .chain(messages)
                .for_each_concurrent(5, |path| async move {
                    let path_str = path.display();
                    if !path.exists() {
                        // Already handled — a file can reach the queue twice,
                        // once from the startup scan and once from a sink that
                        // re-staged it.
                        tracing::debug!("ignoring absent file {path_str}");
                        completion_tx.send_modify(|n| *n += 1);
                        return;
                    }
                    if !path.is_file() {
                        tracing::warn!("ignoring non file {path_str}");
                        completion_tx.send_modify(|n| *n += 1);
                        return;
                    }

                    if put_with_retries(bucket, &path, max_retries, retry_wait).await {
                        match fs::remove_file(&path).await {
                            Ok(()) => tracing::info!("stored {path_str} in {bucket_name}"),
                            Err(err) => tracing::error!(
                                "failed to remove uploaded file {path_str}: {err:?}"
                            ),
                        }
                    } else {
                        // Left in place deliberately: this uploader owns its
                        // own copy, so keeping it costs no other bucket
                        // anything, and the startup scan will pick it up again.
                        tracing::error!(
                            "keeping {path_str}: {bucket_name} did not accept it, \
                             will retry on restart"
                        );
                    }

                    completion_tx.send_modify(|n| *n += 1);
                });

        tokio::select! {
            _ = uploads => (),
            _ = shutdown.clone() => (),
        }

        tracing::info!("stopping file uploader {bucket_name}");
        Ok(())
    }
}

/// Files sitting in an uploader's directory, waiting to be stored.
/// Subdirectories are skipped: a sink's own output directory holds `tmp` and,
/// under a [`MultiFileUpload`], one directory per bucket.
async fn staged_files(dir: &Path) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(dir).await?;

    let mut staged = Vec::new();
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            staged.push(entry.path());
        }
    }
    Ok(staged)
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
