use crate::{Error, Result};
use async_compression::tokio::write::GzipEncoder;
use chrono::Utc;
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

type Sink = GzipEncoder<File>;

pub struct FileWriter {
    target_path: PathBuf,
    tmp_path: PathBuf,
    max_size: usize,
    prefix: String,

    current_sink_size: usize,
    current_sink_path: PathBuf,
    current_sink: Option<Sink>,
}

impl FileWriter {
    pub async fn new(
        target_path: &Path,
        tmp_path: &Path,
        prefix: &str,
        max_size: usize,
    ) -> Result<Self> {
        fs::create_dir_all(target_path).await?;
        Ok(Self {
            target_path: target_path.to_path_buf(),
            tmp_path: tmp_path.to_path_buf(),
            max_size,
            prefix: prefix.to_string(),
            current_sink_size: 0,
            current_sink_path: PathBuf::new(),
            current_sink: None,
        })
    }

    async fn new_sink(&self) -> Result<(PathBuf, PathBuf, Sink)> {
        let filename = format!("{}-{}.gz", self.prefix, Utc::now().timestamp_millis());
        let prev_path = self.current_sink_path.to_path_buf();
        let new_path = self.tmp_path.join(filename);
        let new_sink = GzipEncoder::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&new_path)
                .await?,
        );
        Ok((prev_path, new_path, new_sink))
    }

    async fn roll_sink(&mut self) -> Result<PathBuf> {
        let (prev_path, new_path, new_sink) = self.new_sink().await?;
        if let Some(current_sink) = self.current_sink.as_mut() {
            current_sink.shutdown().await?;
        }
        self.current_sink = Some(new_sink);
        self.current_sink_path = new_path;
        self.current_sink_size = 0;
        Ok(prev_path)
    }

    async fn deposit_sink(&self, sink_path: &Path) -> Result {
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
        let buf = item.encode_to_vec();
        self.write_all(&buf).await
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<usize> {
        if self.current_sink.is_none() {
            let _ = self.roll_sink().await?;
        };
        let prev_sink_path = if (self.current_sink_size + buf.len()) >= self.max_size {
            Some(self.roll_sink().await?)
        } else {
            None
        };

        if let Some(prev_sink_path) = prev_sink_path {
            self.deposit_sink(&prev_sink_path).await?;
        }

        if let Some(sink) = self.current_sink.as_mut() {
            Ok(sink.write(buf).await?)
        } else {
            Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "sink not available",
            )))
        }
    }
}
