use crate::{
    file_source::{FileSource, FileSourceRead},
    Result,
};
use async_trait::async_trait;
use bytes::BytesMut;
use std::path::Path;

pub struct FileMultiSource<'a>(Vec<FileSource<'a>>);

impl<'a> FileMultiSource<'a> {
    pub async fn new(paths: &[&Path]) -> Result<FileMultiSource<'a>> {
        let mut files = vec![];
        // NOTE: Add in reverse, so we pop in order when reading
        for path in paths.iter().rev() {
            let fsource = FileSource::new(path).await?;
            files.push(fsource);
        }
        Ok(FileMultiSource(files))
    }
}

#[async_trait]
impl<'a> FileSourceRead<'a> for FileMultiSource<'a> {
    async fn read(&mut self) -> Result<Option<BytesMut>> {
        if self.0.is_empty() {
            return Ok(None);
        }

        let mut optional = self.0.pop();

        while let Some(mut cur_src) = optional {
            match cur_src.read().await? {
                Some(result) => return Ok(Some(result)),
                None => {
                    optional = self.0.pop();
                    continue;
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;
    use crate::{heartbeat::CellHeartbeat, FileSourceRead};
    use csv::Writer;
    use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
    use std::io;

    #[tokio::test]
    async fn test_multi_read() {
        let p1 = PathBuf::from(r"../test/cell_heartbeat.1658832527866.gz");
        let p2 = PathBuf::from(r"../test/cell_heartbeat.1658834120042.gz");
        let mut file_multi_src = FileMultiSource::new(&[p1.as_path(), p2.as_path()])
            .await
            .unwrap();
        let mut wtr = Writer::from_writer(io::stdout());
        while let Some(msg) = file_multi_src.read().await.unwrap() {
            let dec_msg = CellHeartbeatReqV1::decode(msg).unwrap();
            wtr.serialize(CellHeartbeat::try_from(dec_msg).unwrap())
                .unwrap();
        }

        wtr.flush().unwrap();
    }
}
