use crate::{file_upload, Error, FileType, Result};
use async_compression::tokio::write::GzipEncoder;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::SinkExt;
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::{mpsc, oneshot},
    time,
};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedWrite};

pub const DEFAULT_SINK_ROLL_MINS: i64 = 3;

#[cfg(not(test))]
pub const SINK_CHECK_MILLIS: i64 = 60_000;
#[cfg(test)]
pub const SINK_CHECK_MILLIS: i64 = 50;

pub const MAX_FRAME_LENGTH: usize = 15_000_000;

type Sink = GzipEncoder<BufWriter<File>>;
type Transport = FramedWrite<Sink, LengthDelimitedCodec>;

fn new_transport(sink: Sink) -> Transport {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_write(sink)
}

fn transport_sink(transport: &mut Transport) -> &mut Sink {
    transport.get_mut()
}

#[derive(Debug)]
pub struct Message {
    on_write_tx: oneshot::Sender<Result>,
    bytes: Vec<u8>,
}

pub type MessageSender = mpsc::Sender<Message>;
pub type MessageReceiver = mpsc::Receiver<Message>;

pub fn message_channel(size: usize) -> (MessageSender, MessageReceiver) {
    mpsc::channel(size)
}

#[macro_export]
macro_rules! file_sink_write {
    ($tag:literal, $tx:expr, $item:expr) => {
        file_store::_file_sink_write!($tag, $tx, $item, None)
    };

    ($tag:literal, $tx:expr, $item:expr, $context:expr) => {
        file_store::_file_sink_write!($tag, $tx, $item, Some($context))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _file_sink_write {
    ($tag:literal, $tx:expr, $item:expr, $context:expr) => {
        file_store::file_sink::write(
            concat!(env!("CARGO_PKG_NAME"), "-", $tag),
            $tx,
            $item,
            $context,
        )
    };
}

// Meant to be used indirectly, via file_sink_write macro.
#[doc(hidden)]
pub async fn write<T: prost::Message>(
    tag: &'static str,
    tx: &MessageSender,
    item: T,
    log_context: Option<String>,
) -> Result<oneshot::Receiver<Result>> {
    let (on_write_tx, on_write_rx) = oneshot::channel();
    let bytes = item.encode_to_vec();
    tx.send(Message { on_write_tx, bytes })
        .await
        .map_err(|e| {
            metrics::increment_counter!(tag, "status" => "error");
            tracing::error!(
                "file_sink write failed for {tag:?} with {e:?}. context: {log_context:?}"
            );
            Error::channel()
        })
        .map(move |_| {
            metrics::increment_counter!(tag, "status" => "ok");
            tracing::debug!("file_sink write succeeded for {tag:?}. context: {log_context:?}");
            on_write_rx
        })
}

pub struct FileSinkBuilder {
    prefix: String,
    target_path: PathBuf,
    tmp_path: PathBuf,
    max_size: usize,
    roll_time: Duration,
    messages: MessageReceiver,
    deposits: Option<file_upload::MessageSender>,
}

impl FileSinkBuilder {
    pub fn new(file_type: FileType, target_path: &Path, messages: MessageReceiver) -> Self {
        Self {
            prefix: file_type.to_string(),
            target_path: target_path.to_path_buf(),
            tmp_path: target_path.join("tmp"),
            max_size: 50_000_000,
            roll_time: Duration::minutes(DEFAULT_SINK_ROLL_MINS),
            deposits: None,
            messages,
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

    pub fn deposits(self, deposits: Option<file_upload::MessageSender>) -> Self {
        Self { deposits, ..self }
    }

    pub fn roll_time(self, duration: Duration) -> Self {
        Self {
            roll_time: duration,
            ..self
        }
    }

    pub async fn create(self) -> Result<FileSink> {
        let mut sink = FileSink {
            target_path: self.target_path,
            tmp_path: self.tmp_path,
            prefix: self.prefix,
            max_size: self.max_size,
            deposits: self.deposits,
            roll_time: self.roll_time,
            messages: self.messages,
            active_sink: None,
        };
        sink.init().await?;
        Ok(sink)
    }
}

#[derive(Debug)]
pub struct FileSink {
    target_path: PathBuf,
    tmp_path: PathBuf,
    prefix: String,
    max_size: usize,
    roll_time: Duration,

    messages: MessageReceiver,
    deposits: Option<file_upload::MessageSender>,

    active_sink: Option<ActiveSink>,
}

#[derive(Debug)]
struct ActiveSink {
    size: usize,
    path: PathBuf,
    time: DateTime<Utc>,
    transport: Transport,
}

impl ActiveSink {
    async fn shutdown(&mut self) -> Result {
        transport_sink(&mut self.transport).shutdown().await?;
        Ok(())
    }
}

impl FileSink {
    async fn init(&mut self) -> Result {
        fs::create_dir_all(&self.target_path).await?;
        fs::create_dir_all(&self.tmp_path).await?;
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
                    let _ = self.deposit_sink(&entry.path()).await;
                }
                Ok(None) => break,
                _ => continue,
            }
        }

        // Notify all existing completed sinks
        if let Some(deposits) = &self.deposits {
            let mut dir = fs::read_dir(&self.target_path).await?;
            loop {
                match dir.next_entry().await {
                    Ok(Some(entry))
                        if entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with(&self.prefix) =>
                    {
                        file_upload::upload_file(deposits, &entry.path()).await?;
                    }
                    Ok(None) => break,
                    _ => continue,
                }
            }
        }
        Ok(())
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> Result {
        tracing::info!(
            "starting file sink {} in {}",
            self.prefix,
            self.target_path.display()
        );

        let mut rollover_timer = time::interval(
            Duration::milliseconds(SINK_CHECK_MILLIS)
                .to_std()
                .expect("valid sink roll time"),
        );
        rollover_timer.set_missed_tick_behavior(time::MissedTickBehavior::Burst);

        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = rollover_timer.tick() => self.maybe_roll().await?,
                msg = self.messages.recv() => match msg {
                    Some(Message { bytes, on_write_tx }) => {
                        let res = match self.write(Bytes::from(bytes)).await {
                            Ok(_) => Ok(()),
                            Err(err) => {
                                tracing::error!("failed to store {}: {err:?}", &self.prefix);
                                Err(err)
                            }
                        };
                        let _ = on_write_tx.send(res);
                    },
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

    async fn new_sink(&self) -> Result<ActiveSink> {
        let sink_time = Utc::now();
        let filename = format!("{}.{}.gz", self.prefix, sink_time.timestamp_millis());
        let new_path = self.tmp_path.join(filename);
        let writer = GzipEncoder::new(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&new_path)
                .await?,
        ));
        Ok(ActiveSink {
            path: new_path,
            size: 0,
            time: sink_time,
            transport: new_transport(writer),
        })
    }

    pub async fn maybe_roll(&mut self) -> Result {
        if let Some(active_sink) = self.active_sink.as_mut() {
            if (active_sink.time + self.roll_time) <= Utc::now() {
                active_sink.shutdown().await?;
                let prev_path = active_sink.path.clone();
                self.deposit_sink(&prev_path).await?;
                self.active_sink = None;
            }
        }
        Ok(())
    }

    async fn deposit_sink(&self, sink_path: &Path) -> Result {
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
        if let Some(deposits) = &self.deposits {
            file_upload::upload_file(deposits, &target_path).await?;
        }
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
                    let prev_path = active_sink.path.clone();
                    self.deposit_sink(&prev_path).await?;
                    self.active_sink = Some(self.new_sink().await?);
                }
            }
            // No sink, make a new one
            None => {
                self.active_sink = Some(self.new_sink().await?);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file_source, FileInfo};
    use futures::stream::StreamExt;
    use std::str::FromStr;
    use tempfile::TempDir;
    use tokio::fs::DirEntry;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_a_framed_gzip_encoded_file() {
        let tmp_dir = TempDir::new().expect("Unable to create temp dir");
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        let (sender, receiver) = message_channel(10);

        let mut file_sink = FileSinkBuilder::new(FileType::EntropyReport, tmp_dir.path(), receiver)
            .roll_time(chrono::Duration::milliseconds(100))
            .create()
            .await
            .expect("failed to create file sink");

        let sink_thread = tokio::spawn(async move {
            file_sink
                .run(&shutdown_listener)
                .await
                .expect("failed to complete file sink");
        });

        let (on_write_tx, _on_write_rx) = oneshot::channel();

        sender
            .try_send(Message {
                on_write_tx,
                bytes: String::into_bytes("hello".to_string()),
            })
            .expect("failed to send bytes to file sink");

        tokio::time::sleep(time::Duration::from_millis(200)).await;

        shutdown_trigger.trigger();
        sink_thread.await.expect("file sink did not complete");

        let entropy_file = get_entropy_file(&tmp_dir).await;
        assert_eq!("hello", read_file(&entropy_file).await);
    }

    async fn read_file(entry: &DirEntry) -> bytes::BytesMut {
        file_source::source([entry.path()])
            .next()
            .await
            .unwrap()
            .expect("invalid data in file")
    }

    async fn get_entropy_file(tmp_dir: &TempDir) -> DirEntry {
        let mut entries = fs::read_dir(tmp_dir.path())
            .await
            .expect("failed to read tmp dir");

        while let Some(entry) = entries.next_entry().await.unwrap() {
            if is_entropy_file(&entry) {
                return entry;
            }
        }

        panic!("no entropy file available")
    }

    fn is_entropy_file(entry: &DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .and_then(|file_name| FileInfo::from_str(file_name).ok())
            .map_or(false, |file_info| {
                file_info.file_type == FileType::EntropyReport
            })
    }
}
