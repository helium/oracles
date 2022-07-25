use crate::{Error, FileType, Result};
use async_compression::tokio::write::GzipEncoder;
use chrono::{DateTime, Duration, Utc};
use prost::bytes::BufMut;
use std::{
    io,
    path::{Path, PathBuf},
    time,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};

type Sink = GzipEncoder<BufWriter<File>>;

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

            buf: vec![],
            current_sink_size: 0,
            current_sink: None,
            current_sink_time: None,
            current_sink_path: PathBuf::new(),
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
    buf: Vec<u8>,

    current_sink_size: usize,
    current_sink_path: PathBuf,
    current_sink_time: Option<DateTime<Utc>>,
    current_sink: Option<Sink>,
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

    async fn new_sink(&self) -> Result<(PathBuf, PathBuf, DateTime<Utc>, Sink)> {
        let sink_time = Utc::now();
        let filename = format!("{}.{}.gz", self.prefix, sink_time.timestamp_millis());
        let prev_path = self.current_sink_path.to_path_buf();
        let new_path = self.tmp_path.join(filename);
        let writer = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&new_path)
                .await?,
        );
        Ok((prev_path, new_path, sink_time, GzipEncoder::new(writer)))
    }

    pub async fn maybe_roll(&mut self, max_age: &time::Duration) -> Result {
        let roll_time = Duration::from_std(*max_age).expect("valid duration");
        if let Some(current_sink_time) = self.current_sink_time {
            if current_sink_time + roll_time > Utc::now() {
                let prev_sink_path = self.roll_sink().await?;
                self.deposit_sink(&prev_sink_path).await?;
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        if let Some(current_sink) = self.current_sink.as_mut() {
            let _ = current_sink.shutdown().await;
            self.current_sink = None;
        }
    }

    async fn roll_sink(&mut self) -> Result<PathBuf> {
        let (prev_path, new_path, new_sink_time, new_sink) = self.new_sink().await?;
        if let Some(current_sink) = self.current_sink.as_mut() {
            current_sink.shutdown().await?;
        }
        self.current_sink = Some(new_sink);
        self.current_sink_time = Some(new_sink_time);
        self.current_sink_path = new_path;
        self.current_sink_size = 0;
        Ok(prev_path)
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

    pub async fn write<T: prost::Message>(&mut self, item: T) -> Result<usize> {
        let len = item.encoded_len();
        if len > self.buf.len() {
            self.buf.reserve(len - self.buf.len())
        }

        if self.current_sink.is_none() {
            let _ = self.roll_sink().await?;
        };
        let prev_sink_path = if (self.current_sink_size + len) >= self.max_size {
            Some(self.roll_sink().await?)
        } else {
            None
        };

        if let Some(prev_sink_path) = prev_sink_path {
            self.deposit_sink(&prev_sink_path).await?;
        }

        if let Some(sink) = self.current_sink.as_mut() {
            sink.write_u32(len as u32).await?;
            let written = sink.write(&self.buf[0..len]).await?;
            Ok(written)
        } else {
            Err(Error::from(io::Error::new(
                io::ErrorKind::Other,
                "sink not available",
            )))
        }
    }
}
