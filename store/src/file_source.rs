use crate::{FileType, Result};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::BytesMut;
use futures::StreamExt;
use std::path::Path;
use tokio::{fs::File, io::BufReader};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead};

type Source = GzipDecoder<BufReader<File>>;
type Transport = FramedRead<Source, LengthDelimitedCodec>;

fn new_transport(source: Source) -> Transport {
    FramedRead::new(source, LengthDelimitedCodec::new())
}

#[derive(Debug)]
pub struct FileSource {
    pub file_type: FileType,
    source: Transport,
}

impl FileSource {
    pub async fn new(path: &Path) -> Result<Self> {
        let file_type = FileType::try_from(path)?;
        let file = File::open(path).await?;
        let source = new_transport(GzipDecoder::new(BufReader::new(file)));
        Ok(Self { file_type, source })
    }

    pub async fn read(&mut self) -> Result<Option<BytesMut>> {
        match self.source.next().await {
            Some(result) => Ok(Some(result?)),
            None => Ok(None),
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
