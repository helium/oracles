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
        for path in paths {
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
