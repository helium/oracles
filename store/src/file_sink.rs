use crate::{Error, FileType, Result};
use async_compression::tokio::write::GzipEncoder;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::SinkExt;
use std::{
    io,
    path::{Path, PathBuf},
    time,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedWrite};

type Sink = GzipEncoder<BufWriter<File>>;
type Transport = FramedWrite<Sink, LengthDelimitedCodec>;

fn new_transport(sink: Sink) -> Transport {
    FramedWrite::new(sink, LengthDelimitedCodec::new())
}

fn transport_sink(transport: &mut Transport) -> &mut Sink {
    transport.get_mut()
}

pub struct FileSinkBuilder {
    prefix: String,
    target_path: PathBuf,
    tmp_path: PathBuf,
    max_size: usize,
}

impl FileSinkBuilder {
    pub fn new(file_type: FileType, target_path: &Path) -> Self {
        Self {
            prefix: file_type.to_string(),
            target_path: target_path.to_path_buf(),
            tmp_path: target_path.join("tmp"),
            max_size: 50_000_000,
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

    pub async fn create(self) -> Result<FileSink> {
        let mut sink = FileSink {
            target_path: self.target_path,
            tmp_path: self.tmp_path,
            prefix: self.prefix,
            max_size: self.max_size,

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
        let mut dir = fs::read_dir(&self.tmp_path).await?;
        // Move any partial previous sink files to the target
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    let _ = self.deposit_sink(&entry.path()).await;
                }
                Ok(None) => break,
                _ => continue,
            }
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

    pub async fn maybe_roll(&mut self, max_age: &time::Duration) -> Result {
        let roll_time = Duration::from_std(*max_age).expect("valid duration");
        if let Some(active_sink) = self.active_sink.as_mut() {
            if active_sink.time + roll_time > Utc::now() {
                active_sink.shutdown().await?;
                let prev_path = active_sink.path.clone();
                self.deposit_sink(&prev_path).await?;
                self.active_sink = None;
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        if let Some(active_sink) = self.active_sink.as_mut() {
            let _ = active_sink.shutdown().await;
            self.active_sink = None;
        }
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
        let target_path = self.target_path.join(&target_filename);

        fs::rename(&sink_path, &target_path).await?;
        Ok(())
    }

    pub async fn write<T: prost::Message>(&mut self, item: T) -> Result {
        let buf = item.encode_to_vec();
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
            active_sink.transport.send(Bytes::from(buf)).await?;
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
