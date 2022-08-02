use crate::{FileInfo, Result};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::BytesMut;
use futures_core::stream::BoxStream;
use std::{
    boxed::Box,
    path::{Path, PathBuf},
};
use tokio::{fs::File, io::BufReader};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead};

type Source = BufReader<File>;

pub type Stream = BoxStream<'static, std::result::Result<BytesMut, std::io::Error>>;

fn new_stream<S>(source: S) -> Stream
where
    S: tokio::io::AsyncRead + Send + 'static,
{
    Box::pin(FramedRead::new(source, LengthDelimitedCodec::new()))
}

#[derive(Clone)]
pub struct FileSource {
    pub file_path: PathBuf,
    pub file_info: FileInfo,
}

impl FileSource {
    pub fn new(path: &Path) -> Result<Self> {
        let file_info = FileInfo::try_from(path)?;
<<<<<<< HEAD
=======

>>>>>>> 3411799 (Convert FileSource and FileMultiSource to use Streams)
        Ok(Self {
            file_path: path.to_path_buf(),
            file_info,
        })
    }

    pub async fn into_stream(self) -> Result<Stream> {
        let file = File::open(&self.file_path).await?;

        let buf_reader = BufReader::new(file);
        Ok(
            if let Some("gz") = self.file_path.extension().and_then(|e| e.to_str()) {
                new_stream(GzipDecoder::new(buf_reader))
            } else {
                new_stream::<Source>(buf_reader)
            },
        )
    }
}
