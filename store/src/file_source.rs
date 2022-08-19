use crate::{BytesMutStream, Error};
use async_compression::tokio::bufread::GzipDecoder;
use futures::{
    stream::{self},
    StreamExt, TryFutureExt, TryStreamExt,
};
use std::path::{Path, PathBuf};
use tokio::{fs::File, io::BufReader};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead};

pub fn source<I, P>(paths: I) -> BytesMutStream
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let paths: Vec<PathBuf> = paths
        .into_iter()
        .map(|path| path.as_ref().to_path_buf())
        .collect();
    stream::iter(paths)
        .map(|path| File::open(path).map_err(Error::from))
        .buffered(2)
        .flat_map(|file| match file {
            Ok(file) => {
                let buf_reader = BufReader::new(file);
                FramedRead::new(GzipDecoder::new(buf_reader), LengthDelimitedCodec::new())
                    .map_err(Error::from)
                    .boxed()
            }
            Err(err) => stream::once(async { Err(err) }).boxed(),
        })
        .boxed()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{FileInfo, FileInfoStream, FileStore};
    use std::str::FromStr;

    fn infos(names: &'static [&str]) -> FileInfoStream {
        futures::stream::iter(names.iter().map(|v| FileInfo::from_str(v))).boxed()
    }

    #[tokio::test]
    #[ignore = "credentials required"]
    async fn test_multi_read() {
        //
        // Run with `cargo test -- --include-ignored`
        //
        // Use FileStore::get. These two files exist in the devnet bucket:
        //
        // aws s3 ls s3://devnet-poc5g-rewards
        // 2022-08-05 15:35:55     240363 cell_heartbeat.1658832527866.gz
        // 2022-08-05 15:36:08    6525274 cell_heartbeat.1658834120042.gz
        //
        let file_store = FileStore::new(None, "us-east-1", "devnet-poc5g-rewards")
            .await
            .expect("file store");
        let stream = file_store.source(infos(&[
            "cell_heartbeat.1658832527866.gz",
            "cell_heartbeat.1658834120042.gz",
        ]));
        let p1_stream = file_store.source(infos(&["cell_heartbeat.1658832527866.gz"]));
        let p2_stream = file_store.source(infos(&["cell_heartbeat.1658834120042.gz"]));

        let p1_count = p1_stream.count().await;
        let p2_count = p2_stream.count().await;
        let multi_count = stream.count().await;

        assert_eq!(multi_count, p1_count + p2_count);
    }
}
