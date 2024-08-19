use crate::{file_upload::FileUpload, traits::MsgBytes, Error, Result};
use async_compression::tokio::write::GzipEncoder;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{future::LocalBoxFuture, SinkExt, TryFutureExt};
use metrics::Label;
use std::time::Duration;
use std::{
    io, mem,
    path::{Path, PathBuf},
};
use task_manager::ManagedTask;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::{
        mpsc::{self, error::SendTimeoutError},
        oneshot,
    },
    time,
};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedWrite};

pub const DEFAULT_SINK_ROLL_SECS: u64 = 3 * 60;

#[cfg(not(test))]
pub const SINK_CHECK_MILLIS: u64 = 60_000;
#[cfg(test)]
pub const SINK_CHECK_MILLIS: u64 = 50;

pub const MAX_FRAME_LENGTH: usize = 15_000_000;

type Sink = GzipEncoder<BufWriter<File>>;
type Transport = FramedWrite<Sink, LengthDelimitedCodec>;
pub type FileManifest = Vec<String>;

fn new_transport(sink: Sink) -> Transport {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_write(sink)
}

fn transport_sink(transport: &mut Transport) -> &mut Sink {
    transport.get_mut()
}

#[derive(Debug)]
pub enum Message<T> {
    Data(oneshot::Sender<Result>, T),
    Commit(oneshot::Sender<Result<FileManifest>>),
    Rollback(oneshot::Sender<Result<FileManifest>>),
}

pub type MessageSender<T> = mpsc::Sender<Message<T>>;
pub type MessageReceiver<T> = mpsc::Receiver<Message<T>>;

fn message_channel<T>(size: usize) -> (MessageSender<T>, MessageReceiver<T>) {
    mpsc::channel(size)
}

pub struct FileSinkBuilder {
    prefix: String,
    target_path: PathBuf,
    tmp_path: PathBuf,
    max_size: usize,
    roll_time: Duration,
    file_upload: FileUpload,
    auto_commit: bool,
    metric: &'static str,
}

impl FileSinkBuilder {
    pub fn new(
        prefix: impl ToString,
        target_path: &Path,
        file_upload: FileUpload,
        metric: &'static str,
    ) -> Self {
        Self {
            prefix: prefix.to_string(),
            target_path: target_path.to_path_buf(),
            tmp_path: target_path.join("tmp"),
            max_size: 50_000_000,
            roll_time: Duration::from_secs(DEFAULT_SINK_ROLL_SECS),
            file_upload,
            auto_commit: true,
            metric,
        }
    }

    pub fn max_size(self, max_size: usize) -> Self {
        Self { max_size, ..self }
    }

    pub fn target_path(self, target_path: &Path) -> Self {
        Self {
            target_path: target_path.to_path_buf(),
            ..self
        }
    }

    pub fn tmp_path(self, path: &Path) -> Self {
        Self {
            tmp_path: path.to_path_buf(),
            ..self
        }
    }

    pub fn auto_commit(self, auto_commit: bool) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    pub fn roll_time(self, duration: Duration) -> Self {
        Self {
            roll_time: duration,
            ..self
        }
    }

    pub async fn create<T>(self) -> Result<(FileSinkClient<T>, FileSink<T>)>
    where
        T: MsgBytes,
    {
        let (tx, rx) = message_channel(50);

        let client = FileSinkClient {
            sender: tx,
            metric: self.metric,
        };

        metrics::counter!(client.metric, vec![OK_LABEL]);

        let mut sink = FileSink {
            target_path: self.target_path,
            tmp_path: self.tmp_path,
            prefix: self.prefix,
            max_size: self.max_size,
            file_upload: self.file_upload,
            roll_time: self.roll_time,
            messages: rx,
            staged_files: Vec::new(),
            auto_commit: self.auto_commit,
            active_sink: None,
        };
        sink.init().await?;
        Ok((client, sink))
    }
}

#[derive(Debug, Clone)]
pub struct FileSinkClient<T> {
    pub sender: MessageSender<T>,
    pub metric: &'static str,
}

const OK_LABEL: Label = Label::from_static_parts("status", "ok");
const ERROR_LABEL: Label = Label::from_static_parts("status", "error");
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

impl<T> FileSinkClient<T> {
    pub fn new(sender: MessageSender<T>, metric: &'static str) -> Self {
        Self { sender, metric }
    }

    pub async fn write(
        &self,
        item: T,
        labels: impl IntoIterator<Item = &(&'static str, &'static str)>,
    ) -> Result<oneshot::Receiver<Result>> {
        let (on_write_tx, on_write_rx) = oneshot::channel();
        let labels = labels.into_iter().map(Label::from);
        tokio::select! {
            result = self.sender.send_timeout(Message::Data(on_write_tx, item), SEND_TIMEOUT) => match result {
                Ok(_) => {
                    metrics::counter!(
                        self.metric,
                        labels
                            .chain(std::iter::once(OK_LABEL))
                            .collect::<Vec<Label>>()
                    ).increment(1);
                    tracing::debug!("file_sink write succeeded for {:?}", self.metric);
                    Ok(on_write_rx)
                }
                Err(SendTimeoutError::Closed(_)) => {
                    metrics::counter!(
                        self.metric,
                        labels
                            .chain(std::iter::once(ERROR_LABEL))
                            .collect::<Vec<Label>>()
                    ).increment(1);
                    tracing::error!("file_sink write failed for {:?} channel closed", self.metric);
                    Err(Error::channel())
                }
                Err(SendTimeoutError::Timeout(_)) => {
                    tracing::error!("file_sink write failed for {:?} due to send timeout", self.metric);
                    Err(Error::SendTimeout)
                }
            },
        }
    }

    /// Writes all messages to the file sink, return the last oneshot
    pub async fn write_all(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<Option<oneshot::Receiver<Result>>> {
        let mut last_oneshot = None;
        for item in items {
            last_oneshot = Some(self.write(item, &[]).await?);
        }
        Ok(last_oneshot)
    }

    pub async fn commit(&self) -> Result<oneshot::Receiver<Result<FileManifest>>> {
        let (on_commit_tx, on_commit_rx) = oneshot::channel();
        self.sender
            .send(Message::Commit(on_commit_tx))
            .await
            .map_err(|e| {
                tracing::error!(
                    "file_sink failed to commit for {:?} with {e:?}",
                    self.metric
                );
                Error::channel()
            })
            .map(|_| on_commit_rx)
    }

    pub async fn rollback(&self) -> Result<oneshot::Receiver<Result<FileManifest>>> {
        let (on_rollback_tx, on_rollback_rx) = oneshot::channel();
        self.sender
            .send(Message::Rollback(on_rollback_tx))
            .await
            .map_err(|e| {
                tracing::error!(
                    "file_sink failed to rollback for {:?} with {e:?}",
                    self.metric
                );
                Error::channel()
            })
            .map(|_| on_rollback_rx)
    }
}

#[derive(Debug)]
pub struct FileSink<T> {
    target_path: PathBuf,
    tmp_path: PathBuf,
    prefix: String,
    max_size: usize,
    roll_time: Duration,

    messages: MessageReceiver<T>,
    file_upload: FileUpload,
    staged_files: Vec<PathBuf>,
    auto_commit: bool,

    active_sink: Option<ActiveSink>,
}

#[derive(Debug)]
struct ActiveSink {
    size: usize,
    time: DateTime<Utc>,
    transport: Transport,
}

impl ActiveSink {
    async fn shutdown(&mut self) -> Result {
        transport_sink(&mut self.transport).shutdown().await?;
        Ok(())
    }
}

impl<T: MsgBytes + Send + Sync + 'static> ManagedTask for FileSink<T> {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));

        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<T: MsgBytes> FileSink<T> {
    async fn init(&mut self) -> Result {
        fs::create_dir_all(&self.target_path).await?;
        fs::create_dir_all(&self.tmp_path).await?;

        // Notify all existing completed sinks via file uploads
        let mut dir = fs::read_dir(&self.target_path).await?;
        loop {
            match dir.next_entry().await {
                Ok(Some(entry))
                    if entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&self.prefix) =>
                {
                    self.file_upload.upload_file(&entry.path()).await?;
                }
                Ok(None) => break,
                _ => continue,
            }
        }

        // Move any partial previous sink files to the target
        let mut dir = fs::read_dir(&self.tmp_path).await?;
        loop {
            match dir.next_entry().await {
                Ok(Some(entry))
                    if entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&self.prefix) =>
                {
                    if self.auto_commit {
                        let _ = self.deposit_sink(&entry.path()).await;
                    } else {
                        let _ = fs::remove_file(&entry.path()).await;
                    }
                }
                Ok(None) => break,
                _ => continue,
            }
        }

        Ok(())
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!(
            "starting file sink {} in {}",
            self.prefix,
            self.target_path.display()
        );

        let mut rollover_timer = time::interval(Duration::from_millis(SINK_CHECK_MILLIS));
        rollover_timer.set_missed_tick_behavior(time::MissedTickBehavior::Burst);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = rollover_timer.tick() => self.maybe_roll().await?,
                msg = self.messages.recv() => match msg {
                    Some(Message::Data(on_write_tx, item)) => {
                        let res = match self.write(item.as_bytes()).await {
                            Ok(_) => Ok(()),
                            Err(err) => {
                                tracing::error!("failed to store {}: {err:?}", &self.prefix);
                                Err(err)
                            }
                        };
                        let _ = on_write_tx.send(res);
                    }
                    Some(Message::Commit(on_commit_tx)) => {
                        let res = self.commit().await;
                        let _ = on_commit_tx.send(res);
                    }
                    Some(Message::Rollback(on_rollback_tx)) => {
                        let res = self.rollback().await;
                        let _ = on_rollback_tx.send(res);
                    }
                    None => {
                        break
                    }
                }
            }
        }
        tracing::info!("stopping file sink {}", &self.prefix);
        if let Some(active_sink) = self.active_sink.as_mut() {
            let _ = active_sink.shutdown().await;
            self.active_sink = None;
        }
        Ok(())
    }

    async fn new_sink(&mut self) -> Result {
        let sink_time = Utc::now();
        let filename = format!("{}.{}.gz", self.prefix, sink_time.timestamp_millis());
        let new_path = self.tmp_path.join(filename);
        let writer = GzipEncoder::new(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&new_path)
                .await?,
        ));

        self.staged_files.push(new_path);

        self.active_sink = Some(ActiveSink {
            size: 0,
            time: sink_time,
            transport: new_transport(writer),
        });

        Ok(())
    }

    pub async fn commit(&mut self) -> Result<FileManifest> {
        self.maybe_close_active_sink().await?;

        let mut manifest: FileManifest = Vec::new();
        let staged_files = mem::take(&mut self.staged_files);

        for staged_file in staged_files.into_iter() {
            self.deposit_sink(staged_file.as_path()).await?;
            manifest.push(file_name(&staged_file)?);
        }

        Ok(manifest)
    }

    pub async fn rollback(&mut self) -> Result<FileManifest> {
        self.maybe_close_active_sink().await?;

        let mut manifest: FileManifest = Vec::new();
        let staged_files = mem::take(&mut self.staged_files);

        for staged_file in staged_files.into_iter() {
            fs::remove_file(&staged_file).await?;
            manifest.push(file_name(&staged_file)?);
        }

        Ok(manifest)
    }

    pub async fn maybe_roll(&mut self) -> Result {
        if let Some(active_sink) = self.active_sink.as_mut() {
            if (active_sink.time + self.roll_time) <= Utc::now() {
                if self.auto_commit {
                    self.commit().await?;
                } else {
                    self.maybe_close_active_sink().await?;
                }
            }
        }
        Ok(())
    }

    async fn maybe_close_active_sink(&mut self) -> Result {
        if let Some(active_sink) = self.active_sink.as_mut() {
            active_sink.shutdown().await?;
            self.active_sink = None;
        }

        Ok(())
    }

    async fn deposit_sink(&mut self, sink_path: &Path) -> Result {
        if !sink_path.exists() {
            return Ok(());
        }
        let target_filename = sink_path.file_name().ok_or_else(|| {
            Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "expected sink filename",
            ))
        })?;
        let target_path = self.target_path.join(target_filename);

        fs::rename(&sink_path, &target_path).await?;
        self.file_upload.upload_file(&target_path).await?;

        Ok(())
    }

    pub async fn write(&mut self, buf: Bytes) -> Result {
        let buf_len = buf.len();

        match self.active_sink.as_mut() {
            // If there is an active sink check if the write would make it too
            // large. if so deposit and make a new sink. Otherwise the current
            // active sink is usable.
            Some(active_sink) => {
                if active_sink.size + buf_len >= self.max_size {
                    active_sink.shutdown().await?;
                    if self.auto_commit {
                        self.commit().await?;
                    }
                    self.new_sink().await?;
                }
            }
            // No sink, make a new one
            None => {
                self.new_sink().await?;
            }
        }

        if let Some(active_sink) = self.active_sink.as_mut() {
            active_sink.transport.send(buf).await?;
            active_sink.size += buf_len;
            Ok(())
        } else {
            Err(Error::from(io::Error::new(
                io::ErrorKind::Other,
                "sink not available",
            )))
        }
    }
}

pub fn file_name(path_buf: &Path) -> Result<String> {
    path_buf
        .file_name()
        .map(|os_str| os_str.to_string_lossy().to_string())
        .ok_or_else(|| {
            Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "expected sink filename",
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file_source, file_upload, FileInfo, FileType};
    use futures::stream::StreamExt;
    use std::str::FromStr;
    use tempfile::TempDir;
    use tokio::fs::DirEntry;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_a_framed_gzip_encoded_file() {
        let tmp_dir = TempDir::new().expect("Unable to create temp dir");
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let (file_upload_tx, _file_upload_rx) = file_upload::message_channel();
        let file_upload = FileUpload {
            sender: file_upload_tx,
        };

        let (file_sink_client, file_sink_server) = FileSinkBuilder::new(
            FileType::EntropyReport,
            tmp_dir.path(),
            file_upload,
            "fake_metric",
        )
        .roll_time(Duration::from_millis(100))
        .create()
        .await
        .expect("failed to create file sink");

        let sink_thread = tokio::spawn(async move {
            file_sink_server
                .run(shutdown_listener.clone())
                .await
                .expect("failed to complete file sink");
        });

        let (on_write_tx, _on_write_rx) = oneshot::channel();

        file_sink_client
            .sender
            .try_send(Message::Data(
                on_write_tx,
                String::into_bytes("hello".to_string()),
            ))
            .expect("failed to send bytes to file sink");

        tokio::time::sleep(time::Duration::from_millis(200)).await;

        shutdown_trigger.trigger();
        sink_thread.await.expect("file sink did not complete");

        let entropy_file = get_entropy_file(&tmp_dir)
            .await
            .expect("no entropy available");
        assert_eq!("hello", read_file(&entropy_file).await);
    }

    #[tokio::test]
    async fn only_uploads_after_commit_when_auto_commit_is_false() {
        let tmp_dir = TempDir::new().expect("Unable to create temp dir");
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let (file_upload_tx, mut file_upload_rx) = file_upload::message_channel();
        let file_upload = FileUpload {
            sender: file_upload_tx,
        };

        let (file_sink_client, file_sink_server) = FileSinkBuilder::new(
            FileType::EntropyReport,
            tmp_dir.path(),
            file_upload,
            "fake_metric",
        )
        .roll_time(Duration::from_millis(100))
        .auto_commit(false)
        .create()
        .await
        .expect("failed to create file sink");

        let sink_thread = tokio::spawn(async move {
            file_sink_server
                .run(shutdown_listener.clone())
                .await
                .expect("failed to complete file sink");
        });

        let (on_write_tx, _on_write_rx) = oneshot::channel();
        file_sink_client
            .sender
            .try_send(Message::Data(
                on_write_tx,
                String::into_bytes("hello".to_string()),
            ))
            .expect("failed to send bytes to file sink");

        tokio::time::sleep(time::Duration::from_millis(200)).await;

        assert!(get_entropy_file(&tmp_dir).await.is_err());
        assert_eq!(
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
            file_upload_rx.try_recv()
        );

        let receiver = file_sink_client.commit().await.expect("commit failed");
        let _ = receiver.await.expect("commit didn't complete completed");

        assert!(file_upload_rx.try_recv().is_ok());

        let entropy_file = get_entropy_file(&tmp_dir)
            .await
            .expect("no entropy available");
        assert_eq!("hello", read_file(&entropy_file).await);

        shutdown_trigger.trigger();
        sink_thread.await.expect("file sink did not complete");
    }

    async fn read_file(entry: &DirEntry) -> bytes::BytesMut {
        file_source::source([entry.path()])
            .next()
            .await
            .unwrap()
            .expect("invalid data in file")
    }

    async fn get_entropy_file(tmp_dir: &TempDir) -> std::result::Result<DirEntry, String> {
        let mut entries = fs::read_dir(tmp_dir.path())
            .await
            .expect("failed to read tmp dir");

        while let Some(entry) = entries.next_entry().await.unwrap() {
            if is_entropy_file(&entry) {
                return Ok(entry);
            }
        }

        Err("no entropy available".to_string())
    }

    fn is_entropy_file(entry: &DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .and_then(|file_name| FileInfo::from_str(file_name).ok())
            .map_or(false, |file_info| {
                FileType::from_str(&file_info.prefix).expect("entropy report prefix")
                    == FileType::EntropyReport
            })
    }
}
