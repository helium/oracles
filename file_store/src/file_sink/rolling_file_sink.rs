use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{GzippedFramedFile, GzippedFramedFileError};

#[derive(Debug)]
pub enum RollingFileWriteResult {
    Wrote,
    Rolled { closed_file: PathBuf },
}

#[derive(Debug)]
pub struct RollingFileSink {
    dir: PathBuf,
    prefix: String,
    max_size: usize,
    roll_time: Duration,
    current_file: Option<GzippedFramedFile>,
}

#[derive(thiserror::Error, Debug)]
pub enum RollingFileSinkError {
    #[error("no active file")]
    NoActiveFile,

    #[error("gzipped framed file error: {0}")]
    Sink(#[from] GzippedFramedFileError),
}

pub type Result<T> = std::result::Result<T, RollingFileSinkError>;

impl RollingFileSink {
    pub fn new(dir: PathBuf, prefix: String, max_size: usize, roll_time: Duration) -> Self {
        Self {
            dir,
            prefix,
            max_size,
            roll_time,
            current_file: None,
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub async fn write(&mut self, buf: Bytes) -> Result<RollingFileWriteResult> {
        self._write(buf).await.inspect(|err| {
            tracing::error!(?err, prefix = self.prefix, "write error");
        })
    }

    async fn _write(&mut self, buf: Bytes) -> Result<RollingFileWriteResult> {
        match self.should_roll_file(&buf) {
            true => {
                let closed_file = self.close_current_file().await?;
                self.get_writer().await?.write(buf).await?;

                Ok(RollingFileWriteResult::Rolled { closed_file })
            }
            false => {
                self.get_writer().await?.write(buf).await?;

                Ok(RollingFileWriteResult::Wrote)
            }
        }
    }

    pub fn should_close(&self, now: DateTime<Utc>) -> bool {
        if let Some(ref file) = self.current_file {
            let time_to_close = file.open_timestamp() + self.roll_time;
            return time_to_close <= now;
        }

        false
    }

    pub async fn close_current_file(&mut self) -> Result<PathBuf> {
        let path = self
            .current_file
            .take()
            .ok_or(RollingFileSinkError::NoActiveFile)?
            .close()
            .await?;
        Ok(path)
    }

    pub async fn close_current_file_if_exists(&mut self) -> Result<Option<PathBuf>> {
        if self.current_file.is_some() {
            Ok(Some(self.close_current_file().await?))
        } else {
            Ok(None)
        }
    }

    async fn get_writer(&mut self) -> Result<&mut GzippedFramedFile> {
        if self.current_file.is_none() {
            // We wait until we're about to write to generate the file so the
            // timestamp aligns with the first write to the file.
            let file =
                GzippedFramedFile::new(&self.dir, &self.prefix, Utc::now(), self.max_size).await?;
            self.current_file = Some(file);
        }

        // Unwrap is safe because we are ensuring the file exists above.
        Ok(self.current_file.as_mut().unwrap())
    }

    fn should_roll_file(&self, buf: &Bytes) -> bool {
        self.current_file
            .as_ref()
            .map(|f| !f.will_fit(buf))
            .unwrap_or(false)
    }
}
