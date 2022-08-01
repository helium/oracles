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
        while let Some(cur_src) = self.0.last_mut() {
            match cur_src.read().await? {
                Some(result) => return Ok(Some(result)),
                None => {
                    // Current source exhausted, pop and move to next one
                    self.0.pop();
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_multi_read() {
        async fn get_count(mut file_src: FileSource<'_>) -> Result<i32> {
            let mut count = 0;
            while let Some(_buf) = file_src.read().await.unwrap() {
                count += 1;
            }
            Ok(count)
        }

        let p1 = PathBuf::from(r"../test/cell_heartbeat.1658832527866.gz");
        let p2 = PathBuf::from(r"../test/cell_heartbeat.1658834120042.gz");
        let mut file_multi_src = FileMultiSource::new(&[p1.as_path(), p2.as_path()])
            .await
            .unwrap();

        let mut tot_count = 0;
        while let Some(_buf) = file_multi_src.read().await.unwrap() {
            tot_count += 1;
        }

        let f1 = FileSource::new(p1.as_path()).await.unwrap();
        let c1 = get_count(f1).await.unwrap();
        let f2 = FileSource::new(p2.as_path()).await.unwrap();
        let c2 = get_count(f2).await.unwrap();

        assert_eq!(tot_count, c1 + c2);
    }
}
