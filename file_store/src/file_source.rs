use crate::{
    file_info_poller::{
        FileInfoPollerConfigBuilder, MsgDecodeFileInfoPollerParser, ProstFileInfoPollerParser,
    },
    file_sink, BytesMutStream, Error,
};
use async_compression::tokio::bufread::GzipDecoder;
use futures::{
    stream::{self},
    StreamExt, TryFutureExt, TryStreamExt,
};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tokio::{fs::File, io::BufReader};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead};

pub struct Continuous<Parser>(PhantomData<Parser>);

impl Continuous<MsgDecodeFileInfoPollerParser> {
    pub fn msg_source<Msg, State, Store>(
    ) -> FileInfoPollerConfigBuilder<Msg, State, Store, MsgDecodeFileInfoPollerParser>
    where
        Msg: Clone,
    {
        FileInfoPollerConfigBuilder::<Msg, State, Store, MsgDecodeFileInfoPollerParser>::default()
            .parser(MsgDecodeFileInfoPollerParser)
    }
}

impl Continuous<ProstFileInfoPollerParser> {
    pub fn prost_source<Msg, State, Store>(
    ) -> FileInfoPollerConfigBuilder<Msg, State, Store, ProstFileInfoPollerParser>
    where
        Msg: Clone,
    {
        FileInfoPollerConfigBuilder::<Msg, State, Store, ProstFileInfoPollerParser>::default()
            .parser(ProstFileInfoPollerParser)
    }
}

pub fn continuous_source<T, S, S2>(
) -> FileInfoPollerConfigBuilder<T, S, S2, MsgDecodeFileInfoPollerParser>
where
    T: Clone,
{
    Continuous::msg_source::<T, S, S2>()
}

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
                let codec = LengthDelimitedCodec::builder()
                    .max_frame_length(file_sink::MAX_FRAME_LENGTH)
                    .new_codec();

                FramedRead::new(GzipDecoder::new(buf_reader), codec)
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
    use crate::{FileInfo, FileInfoStream, Settings};
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

        let settings = Settings {
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        };

        let client = settings.connect().await;
        let bucket = "devnet-poc5g-rewards".to_string();

        let stream = crate::source_files(
            &client,
            &bucket,
            infos(&[
                "cell_heartbeat.1658832527866.gz",
                "cell_heartbeat.1658834120042.gz",
            ]),
        );
        let p1_stream = crate::source_files(
            &client,
            &bucket,
            infos(&["cell_heartbeat.1658832527866.gz"]),
        );
        let p2_stream = crate::source_files(
            &client,
            &bucket,
            infos(&["cell_heartbeat.1658834120042.gz"]),
        );

        let p1_count = p1_stream.count().await;
        let p2_count = p2_stream.count().await;
        let multi_count = stream.count().await;

        assert_eq!(multi_count, p1_count + p2_count);
    }
}
