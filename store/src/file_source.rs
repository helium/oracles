use crate::{Error, FileType, Result};
use async_compression::tokio::bufread::GzipDecoder;
use std::{io, path::Path};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

type Source = GzipDecoder<BufReader<File>>;

#[derive(Debug)]
pub struct FileSource {
    pub file_type: FileType,
    source: Source,
    buf: Vec<u8>,
}

impl FileSource {
    pub async fn new(path: &Path) -> Result<Self> {
        let file_type = FileType::try_from(path)?;
        let file = File::open(path).await?;
        let source = GzipDecoder::new(BufReader::new(file));
        let buf = vec![];
        Ok(Self {
            file_type,
            source,
            buf,
        })
    }

    pub async fn read(&mut self) -> Result<&[u8]> {
        match self.source.read_u32().await {
            Ok(len) => {
                let len = len as usize;
                if len > self.buf.len() {
                    self.buf.reserve(len - self.buf.len());
                }
                self.source.read_exact(&mut self.buf[0..len]).await?;
                Ok(&self.buf[0..len])
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(&self.buf[0..0]),
            Err(err) => Err(Error::from(err)),
        }
    }

    // pub async fn read<T>(&mut self) -> Result<Option<T>>
    // where
    //     T: helium_proto::Message + Default,
    // {
    //     let buf = self.read_bytes().await?;
    //     let result = match buf {
    //         Some(buf) => Some(T::decode(buf)?),
    //         None => None,
    //     };
    //     Ok(result)
    // }
}
