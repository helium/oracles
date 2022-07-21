use crate::{Error, Result};
use async_compression::tokio::bufread::GzipDecoder;
use std::path::Path;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

type Source = GzipDecoder<BufReader<File>>;

#[derive(Debug)]
pub struct FileSource {
    source: Source,
    buf: Vec<u8>,
}

impl FileSource {
    pub async fn new(path: &Path) -> Result<Self> {
        let file = File::open(path).await?;
        Ok(Self {
            source: GzipDecoder::new(BufReader::new(file)),
            buf: vec![],
        })
    }

    pub async fn read<T: prost::Message>(&mut self) -> Result<T>
    where
        T: Default,
    {
        let len = self.source.read_u32().await.map_err(Error::from)? as usize;
        if len > self.buf.len() {
            self.buf.reserve(len - self.buf.len());
        }
        self.source.read_exact(&mut self.buf[0..len]).await?;
        let msg = T::decode(&self.buf[0..len])?;
        Ok(msg)
    }
}
