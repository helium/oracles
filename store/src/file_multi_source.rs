use crate::{
    file_source::{FileSource, Stream},
    Result,
};
use futures::stream::{select_all, StreamExt};
use std::path::Path;

pub struct FileMultiSource(Vec<FileSource>);

impl FileMultiSource {
    pub fn new<I, P>(paths: I) -> Result<FileMultiSource>
    where
        I: IntoIterator<Item = P>,
        <I as IntoIterator>::IntoIter: DoubleEndedIterator,
        P: AsRef<Path>,
    {
        let mut files = vec![];
        // NOTE: Add in reverse, so we pop in order when reading
        for path in paths.into_iter().rev() {
            let fsource = FileSource::new(path.as_ref())?;
            files.push(fsource);
        }
        Ok(FileMultiSource(files))
    }

    pub async fn into_stream(self) -> Result<Stream> {
        let mut streams = Vec::new();
        for source in self.0.into_iter() {
            streams.push(source.into_stream().await?);
        }
        Ok(select_all(streams.into_iter()).boxed())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_multi_read() {
        async fn get_count(file_src: FileSource) -> Result<i32> {
            let mut count = 0;
            let mut stream = file_src.into_stream().await?;
            while let Some(_buf) = stream.next().await {
                count += 1;
            }
            Ok(count)
        }

        let p1 = PathBuf::from(r"../test/cell_heartbeat.1658832527866.gz");
        let p2 = PathBuf::from(r"../test/cell_heartbeat.1658834120042.gz");
        let mut file_multi_stream = FileMultiSource::new(&[p1.as_path(), p2.as_path()])
            .unwrap()
            .into_stream()
            .await
            .unwrap();

        let mut tot_count = 0;
        while let Some(_buf) = file_multi_stream.next().await {
            tot_count += 1;
        }

        let f1 = FileSource::new(p1.as_path()).unwrap();
        let c1 = get_count(f1).await.unwrap();
        let f2 = FileSource::new(p2.as_path()).unwrap();
        let c2 = get_count(f2).await.unwrap();

        assert_eq!(tot_count, c1 + c2);
    }
}
