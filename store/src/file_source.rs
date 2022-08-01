use crate::{FileInfo, Result};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::BytesMut;
use futures::StreamExt;
use futures_core::stream::BoxStream;
use std::{boxed::Box, path::Path};
use tokio::{fs::File, io::BufReader};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead};

type Source = BufReader<File>;
type CompressedSource = GzipDecoder<Source>;

type Transport<'a> = BoxStream<'a, std::result::Result<BytesMut, std::io::Error>>;

fn new_transport<'a, S>(source: S) -> Transport<'a>
where
    S: tokio::io::AsyncRead + std::marker::Send + 'a,
{
    Box::pin(FramedRead::new(source, LengthDelimitedCodec::new()))
}

pub struct FileSource<'a> {
    pub file_info: FileInfo,
    transport: Transport<'a>,
}

impl<'a> FileSource<'a> {
    pub async fn new(path: &Path) -> Result<FileSource<'a>> {
        let file_info = FileInfo::try_from(path)?;
        let file = File::open(path).await?;

        let buf_reader = BufReader::new(file);
        let transport = if let Some("gz") = path.extension().and_then(|e| e.to_str()) {
            new_transport::<CompressedSource>(GzipDecoder::new(buf_reader))
        } else {
            new_transport::<Source>(buf_reader)
        };
        Ok(Self {
            file_info,
            transport,
        })
    }

    pub async fn read(&mut self) -> Result<Option<BytesMut>> {
        match self.transport.next().await {
            Some(result) => Ok(Some(result?)),
            None => Ok(None),
        }
    }
}
