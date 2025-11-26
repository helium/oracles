use std::path::{Path, PathBuf};

use async_compression::tokio::write::GzipEncoder;
use chrono::{DateTime, Utc};
use futures::SinkExt;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

pub const MAX_FRAME_LENGTH: usize = 15_000_000;
pub const MAX_FILE_SIZE: usize = 50_000_000;

#[derive(Debug, thiserror::Error)]
pub enum GzippedFramedFileError {
    #[error("invalid configuration when creating file")]
    InvalidConfiguration,
    #[error("file has reached maximum size")]
    MaxSizeError,
    #[error("io error: {0}")]
    IOError(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct GzippedFramedFile {
    transport: FramedWrite<GzipEncoder<BufWriter<File>>, LengthDelimitedCodec>,
    path: PathBuf,
    size: usize,
    max_size: usize,
    time: DateTime<Utc>,
}

impl GzippedFramedFile {
    pub fn builder() -> GzippedFramedFileBuilder {
        GzippedFramedFileBuilder::default()
    }

    pub async fn new(
        dir: impl AsRef<Path>,
        prefix: impl Into<String>,
        time: DateTime<Utc>,
        max_size: usize,
    ) -> Result<Self, GzippedFramedFileError> {
        let filename = format!("{}.{}.gz", prefix.into(), time.timestamp_millis());
        let path = dir.as_ref().join(filename);

        let writer = GzipEncoder::new(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .await?,
        ));

        let transport = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_LENGTH)
            .new_write(writer);

        Ok(Self {
            transport,
            path,
            size: 0,
            max_size,
            time,
        })
    }

    pub fn open_timestamp(&self) -> DateTime<Utc> {
        self.time
    }

    pub fn will_fit(&self, bytes: &bytes::Bytes) -> bool {
        self.size + bytes.len() <= self.max_size
    }

    pub fn will_all_fit<'a>(&self, items: impl IntoIterator<Item = &'a bytes::Bytes>) -> bool {
        let sum: usize = items.into_iter().map(|b| b.len()).sum();
        self.size + sum <= self.max_size
    }

    pub async fn write(&mut self, bytes: bytes::Bytes) -> Result<(), GzippedFramedFileError> {
        if !self.will_fit(&bytes) {
            return Err(GzippedFramedFileError::MaxSizeError);
        }

        self.size += bytes.len();
        self.transport.send(bytes).await?;

        Ok(())
    }

    pub async fn write_all(
        &mut self,
        items: impl IntoIterator<Item = bytes::Bytes>,
    ) -> Result<(), GzippedFramedFileError> {
        for item in items {
            self.write(item).await?;
        }

        Ok(())
    }

    pub async fn close(self) -> Result<PathBuf, GzippedFramedFileError> {
        self.transport.into_inner().shutdown().await?;
        Ok(self.path)
    }
}

#[derive(Debug, Default)]
pub struct GzippedFramedFileBuilder {
    path: Option<PathBuf>,
    prefix: Option<String>,
    time: Option<DateTime<Utc>>,
    max_size: Option<usize>,
}

impl GzippedFramedFileBuilder {
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().into());
        self
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn time(mut self, time: DateTime<Utc>) -> Self {
        self.time = Some(time);
        self
    }

    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    pub async fn build(self) -> Result<GzippedFramedFile, GzippedFramedFileError> {
        GzippedFramedFile::new(
            self.path
                .ok_or(GzippedFramedFileError::InvalidConfiguration)?,
            self.prefix
                .ok_or(GzippedFramedFileError::InvalidConfiguration)?,
            self.time
                .ok_or(GzippedFramedFileError::InvalidConfiguration)?,
            self.max_size.unwrap_or(MAX_FILE_SIZE),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn file_name_formatted_correctly() -> Result<(), GzippedFramedFileError> {
        let temp_dir = tempfile::tempdir()?;

        let now = Utc::now();

        let file = GzippedFramedFile::builder()
            .path(&temp_dir)
            .prefix("testing")
            .time(now)
            .build()
            .await?;

        let path = file.close().await?;

        let expected_filename = format!("testing.{}.gz", now.timestamp_millis());

        assert_eq!(expected_filename.as_str(), path.file_name().unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn can_write_and_read_bytes() -> Result<(), GzippedFramedFileError> {
        let temp_dir = tempfile::tempdir()?;

        let now = Utc::now();

        let mut file = GzippedFramedFile::builder()
            .path(&temp_dir)
            .prefix("testing")
            .time(now)
            .build()
            .await?;

        file.write("Hello World".into()).await?;
        file.write("Bye Bye".into()).await?;

        let file = file.close().await?;

        let bytes = std::fs::read(&file).expect("unable to read file");

        let strings: Vec<String> = crate::stream_source(bytes.into())
            .map_ok(|b| String::from_utf8(b.into()).expect("unable to parse bytes"))
            .try_collect()
            .await
            .expect("unable to parse file");

        assert_eq!(2, strings.len());
        assert_eq!("Hello World", strings[0]);
        assert_eq!("Bye Bye", strings[1]);

        Ok(())
    }

    #[tokio::test]
    async fn can_write_and_read_bytes_from_iterator() -> Result<(), GzippedFramedFileError> {
        let temp_dir = tempfile::tempdir()?;

        let now = Utc::now();

        let mut file = GzippedFramedFile::builder()
            .path(&temp_dir)
            .prefix("testing")
            .time(now)
            .build()
            .await?;

        file.write_all(vec!["Hello World".into(), "Bye Bye".into()])
            .await?;

        let file = file.close().await?;

        let bytes = std::fs::read(&file).expect("unable to read file");

        let strings: Vec<String> = crate::stream_source(bytes.into())
            .map_ok(|b| String::from_utf8(b.into()).expect("unable to parse bytes"))
            .try_collect()
            .await
            .expect("unable to parse file");

        assert_eq!(2, strings.len());
        assert_eq!("Hello World", strings[0]);
        assert_eq!("Bye Bye", strings[1]);

        Ok(())
    }

    #[tokio::test]
    async fn tracks_bytes_written_against_max_size() -> Result<(), GzippedFramedFileError> {
        let temp_dir = tempfile::tempdir()?;

        let now = Utc::now();

        let file = GzippedFramedFile::builder()
            .path(&temp_dir)
            .prefix("testing")
            .time(now)
            .max_size(10)
            .build()
            .await?;

        assert!(file.will_fit(&"HelloWorld".into()));
        assert!(!file.will_fit(&"Hello World".into()));

        Ok(())
    }

    #[tokio::test]
    async fn tracks_bytes_written_against_max_size_from_iterator(
    ) -> Result<(), GzippedFramedFileError> {
        let temp_dir = tempfile::tempdir()?;

        let now = Utc::now();

        let file = GzippedFramedFile::builder()
            .path(&temp_dir)
            .prefix("testing")
            .time(now)
            .max_size(10)
            .build()
            .await?;

        let one = vec!["HelloWorld".into()];
        let two = vec!["HelloWorld".into(), "a".into()];

        assert!(file.will_all_fit(&one));
        assert!(!file.will_all_fit(&two));

        Ok(())
    }
}
