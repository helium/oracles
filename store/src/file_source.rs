use crate::{Error, FileInfo, FileStore, Result};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::BytesMut;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryFutureExt, TryStreamExt,
};
use std::{
    boxed::Box,
    marker::Send,
    path::{Path, PathBuf},
};
use tokio::{fs::File, io::AsyncRead, io::BufReader};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead};

pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type ByteStream = Stream<BytesMut>;

const STREAM_BUFFER_SIZE: usize = 100_000;

fn stream_source<S>(stream: S) -> ByteStream
where
    S: AsyncRead + Send + 'static,
{
    let buf_reader = BufReader::with_capacity(STREAM_BUFFER_SIZE, stream);
    Box::pin(
        FramedRead::new(GzipDecoder::new(buf_reader), LengthDelimitedCodec::new())
            .map_err(Error::from),
    )
}

/// Stream a series of items from a given remote store and bucket. This will
/// automatically open the next file when the current one is exhausted.
pub fn store_source<I, B>(store: FileStore, bucket: B, infos: I) -> ByteStream
where
    I: IntoIterator<Item = FileInfo>,
    B: Into<String>,
    <I as IntoIterator>::IntoIter: DoubleEndedIterator + Send + 'static,
{
    let infos = infos.into_iter().rev();
    stream::try_unfold(
        (bucket.into(), infos, store, None),
        move |(bucket, mut infos, store, current): (
            String,
            _,
            FileStore,
            Option<ByteStream>,
        )| async move {
            if let Some(mut stream) = current {
                match stream.next().await {
                    Some(Ok(item)) => {
                        return Ok(Some((
                            item,
                            (bucket, infos, store, Some(stream)),
                        )))
                    }
                    Some(Err(err)) => return Err(err),
                    None => (),
                }
            };
            // No current exhausted or none. get next key and make a new stream
            while let Some(info) = infos.next() {
                tracing::info!("streaming {bucket}: {}", &info.key);
                let mut stream = stream_source(store.get(bucket.clone(), info.key.clone()).await?);
                match stream.next().await {
                    Some(Ok(item)) => {
                        return Ok(Some((
                            item,
                            (bucket, infos, store, Some(stream)),
                        )))
                    }
                    Some(Err(err)) => return Err(err),
                    None => (),
                }
            }
            Ok(None)
        },
    )
    .boxed()
}

pub fn file_source<I, P>(paths: I) -> ByteStream
where
    I: IntoIterator<Item = P>,
    <I as IntoIterator>::IntoIter: DoubleEndedIterator + Send,
    P: AsRef<Path>,
{
    let paths = paths
        .into_iter()
        .rev()
        .map(|p| p.as_ref().to_path_buf())
        .collect();
    stream::try_unfold(
        (paths, None),
        |(mut paths, current): (Vec<PathBuf>, Option<ByteStream>)| async move {
            if let Some(mut stream) = current {
                match stream.next().await {
                    Some(Ok(item)) => return Ok(Some((item, (paths, Some(stream))))),
                    Some(Err(err)) => return Err(err),
                    None => (),
                }
            };
            // No current exhausted or none. Pop paths and make a new file_source
            while let Some(path) = paths.pop() {
                let mut stream = File::open(&path)
                    .map_ok(stream_source)
                    .map_err(Error::from)
                    .await?;
                match stream.next().await {
                    Some(Ok(item)) => return Ok(Some((item, (paths, Some(stream))))),
                    Some(Err(err)) => return Err(err),
                    None => (),
                }
            }
            Ok(None)
        },
    )
    .boxed()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::file_store::FileStore;
    use std::str::FromStr;

    fn infos(names: &[&str]) -> Vec<FileInfo> {
        names
            .iter()
            .map(|v| FileInfo::from_str(v).expect("valid file_info"))
            .collect()
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
        let file_store = FileStore::new(None, "us-east-1").await.expect("file store");
        let stream = store_source(
            file_store.clone(),
            "devnet-poc5g-rewards",
            infos(&[
                "cell_heartbeat.1658832527866.gz",
                "cell_heartbeat.1658834120042.gz",
            ]),
        );
        let p1_stream = store_source(
            file_store.clone(),
            "devnet-poc5g-rewards",
            infos(&["cell_heartbeat.1658832527866.gz"]),
        );
        let p2_stream = store_source(
            file_store.clone(),
            "devnet-poc5g-rewards",
            infos(&["cell_heartbeat.1658834120042.gz"]),
        );

        let p1_count = p1_stream.count().await;
        let p2_count = p2_stream.count().await;
        let multi_count = stream.count().await;

        assert_eq!(multi_count, p1_count + p2_count);
    }
}
