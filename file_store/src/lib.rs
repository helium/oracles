extern crate tls_init;

mod error;
mod gzipped_framed_file;
mod settings;

pub mod aws_local;
pub mod bucket_client;
pub mod file_info;
pub mod file_info_poller;
pub mod file_sink;
pub mod file_source;
pub mod file_upload;
pub mod traits;

// Re-exports
pub use bucket_client::BucketClient;
pub use error::{AwsError, ChannelError, Error, Result};
pub use file_info::FileInfo;
pub use file_sink::{FileSink, FileSinkBuilder};
pub use gzipped_framed_file::GzippedFramedFile;
pub use settings::{BucketSettings, Settings};

// Client functions
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types_convert::stream::PaginationStreamExt;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::{
    future,
    stream::{self, BoxStream},
    FutureExt, StreamExt, TryFutureExt, TryStreamExt,
};
use std::{collections::HashMap, path::Path, sync::OnceLock};
use tokio::sync::Mutex;

pub type Client = aws_sdk_s3::Client;
pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileInfoStream = Stream<FileInfo>;
pub type BytesMutStream = Stream<BytesMut>;

static CLIENT_MAP: OnceLock<Mutex<HashMap<ClientKey, Client>>> = OnceLock::new();

#[derive(PartialEq, Eq, Hash, Debug)]
struct ClientKey {
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
}

pub async fn new_client(
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
) -> aws_sdk_s3::Client {
    let mut client_map = CLIENT_MAP
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .await;

    let key = ClientKey {
        region: region.clone(),
        endpoint: endpoint.clone(),
        access_key_id: access_key_id.clone(),
        secret_access_key: secret_access_key.clone(),
    };

    if let Some(client) = client_map.get(&key) {
        tracing::debug!(params = ?key, "Using existing file-store s3 client");
        return client.clone();
    }

    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let mut s3_config = aws_sdk_s3::config::Builder::from(&config);

    if let Some(region_str) = region {
        s3_config = s3_config.region(aws_config::Region::new(region_str));
    }

    if let Some(endpoint) = endpoint {
        s3_config = s3_config.endpoint_url(endpoint);
        // NOTE(mj): If you see something like a DNS error, this is probably
        // the culprit. Need to find a way to make this configurable. It
        // would be nice to allow the "local" feature to be active, but not
        // enforce path style.
        s3_config = s3_config.force_path_style(true);
    }

    if let Some((access_key_id, secret_access_key)) = access_key_id.zip(secret_access_key) {
        let creds = aws_sdk_s3::config::Credentials::builder()
            .provider_name("Static")
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key);

        s3_config = s3_config.credentials_provider(creds.build());
    }

    tracing::debug!(params = ?key, "Creating new file-store s3 client");
    let client = Client::from_conf(s3_config.build());
    client_map.insert(key, client.clone());
    client
}

pub fn list_files<A, B>(
    client: &Client,
    bucket: impl Into<String>,
    prefix: impl Into<String>,
    after: A,
    before: B,
) -> FileInfoStream
where
    A: Into<Option<DateTime<Utc>>> + Copy,
    B: Into<Option<DateTime<Utc>>> + Copy,
{
    let before = before.into();
    let after = after.into();

    _list_files(client, bucket, prefix, after)
        .try_filter(move |info| future::ready(after.is_none_or(|v| info.timestamp > v)))
        .try_filter(move |info| future::ready(before.is_none_or(|v| info.timestamp <= v)))
        .boxed()
}

fn _list_files(
    client: &Client,
    bucket: impl Into<String>,
    prefix: impl Into<String>,
    after: Option<DateTime<Utc>>,
) -> FileInfoStream {
    let prefix = prefix.into();

    client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&prefix)
        .set_start_after(after.map(|dt| FileInfo::from_maybe_dotted_prefix(&prefix, dt).into()))
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_ok(|page| stream::iter(page.contents.unwrap_or_default()).map(Ok))
        .map_err(AwsError::list_object_error)
        .try_flatten()
        .try_filter_map(|file| {
            future::ready(FileInfo::try_from(&file).map(Some).map_err(Error::from))
        })
        .boxed()
}

pub async fn list_all_files<A, B>(
    client: &Client,
    bucket: impl Into<String>,
    prefix: impl Into<String>,
    after: A,
    before: B,
) -> Result<Vec<FileInfo>>
where
    A: Into<Option<DateTime<Utc>>> + Copy,
    B: Into<Option<DateTime<Utc>>> + Copy,
{
    list_files(client, bucket, prefix, after, before)
        .try_collect()
        .await
}

pub async fn put_file(client: &Client, bucket: impl Into<String>, file: &Path) -> Result {
    let byte_stream = ByteStream::from_path(&file)
        .map_err(AwsError::pub_object_byte_stream_error)
        .await?;

    poc_metrics::record_duration!(
        "file_store_put_duration",
        client
            .put_object()
            .bucket(bucket)
            .key(file.file_name().map(|name| name.to_string_lossy()).unwrap())
            .body(byte_stream)
            .content_type("application/octet-stream")
            .send()
            .map_ok(|_| ())
            .map_err(AwsError::put_object_error)
            .await
    )
}

pub async fn remove_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
) -> Result {
    poc_metrics::record_duration!(
        "file_store_remove_duration",
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .map_ok(|_| ())
            .map_err(AwsError::delete_object_error)
            .await
    )
}

pub async fn get_raw_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
) -> Result<ByteStream> {
    get_byte_stream(client.clone(), bucket, key).await
}

pub async fn get_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
) -> Result<BytesMutStream> {
    Ok(stream_source(get_raw_file(client, bucket, key).await?))
}

pub fn source_files(
    client: &Client,
    bucket: impl Into<String>,
    infos: FileInfoStream,
) -> BytesMutStream {
    let client = client.clone();
    let bucket: String = bucket.into();
    infos
        .map_ok(move |info| get_byte_stream(client.clone(), bucket.clone(), info.key))
        .try_buffered(2)
        .flat_map(|stream| match stream {
            Ok(stream) => stream_source(stream),
            Err(err) => stream::once(async move { Err(err) }).boxed(),
        })
        .fuse()
        .boxed()
}

pub fn source_files_unordered(
    client: &Client,
    bucket: impl Into<String>,
    workers: usize,
    infos: FileInfoStream,
) -> BytesMutStream {
    let bucket: String = bucket.into();
    let client = client.clone();
    infos
        .map_ok(move |info| get_byte_stream(client.clone(), bucket.clone(), info.key))
        .try_buffer_unordered(workers)
        .flat_map(|stream| match stream {
            Ok(stream) => stream_source(stream),
            Err(err) => stream::once(async move { Err(err) }).boxed(),
        })
        .fuse()
        .boxed()
}

pub async fn stream_single_file(
    client: &Client,
    bucket: impl Into<String>,
    file_info: FileInfo,
) -> Result<BytesMutStream> {
    get_byte_stream(client.clone(), bucket, file_info)
        .await
        .map(stream_source)
}

pub fn stream_source(stream: ByteStream) -> BytesMutStream {
    use async_compression::tokio::bufread::GzipDecoder;
    use tokio::io::BufReader;
    use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead};

    Box::pin(
        FramedRead::new(
            GzipDecoder::new(BufReader::new(stream.into_async_read())),
            LengthDelimitedCodec::new(),
        )
        .map_err(Error::from),
    )
}

async fn get_byte_stream(
    client: Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
) -> Result<ByteStream> {
    client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .map_ok(|output| output.body)
        .map_err(AwsError::get_object_error)
        .fuse()
        .await
}

#[cfg(test)]
tls_init::include_tls_tests!();

#[cfg(test)]
mod tests {
    use aws_local::AwsLocal;
    use std::time::Duration;

    use super::*;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[derive(Clone, prost::Message)]
    struct TestMsg {}

    #[tokio::test]
    async fn list_files_respects_start_after_with_dotted_prefix() -> TestResult {
        let file_prefix = "file_prefix.";

        let awsl = create_aws_local_with_timestamped_files(
            file_prefix.trim_end_matches('.'),
            &[hours_ago(5), hours_ago(3), hours_ago(1)],
        )
        .await?;

        let after = hours_ago(4);
        let files = test_list_files_after(&awsl, file_prefix, Some(after)).await?;
        assert_eq!(files.len(), 2);
        assert!(files.into_iter().all(|f| f.timestamp >= after));

        awsl.cleanup().await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_files_respects_start_after_without_dotted_prefix() -> TestResult {
        let file_prefix = "file_prefix";

        let awsl = create_aws_local_with_timestamped_files(
            file_prefix,
            &[hours_ago(5), hours_ago(3), hours_ago(1)],
        )
        .await?;

        let after = hours_ago(4);
        let files = test_list_files_after(&awsl, file_prefix, Some(after)).await?;
        assert_eq!(files.len(), 2);
        assert!(files.into_iter().all(|f| f.timestamp >= after));

        awsl.cleanup().await?;

        Ok(())
    }

    #[tokio::test]
    async fn list_files_returns_all_files_when_no_after_provided() -> TestResult {
        let file_prefix = "file_prefix.";

        let awsl = create_aws_local_with_timestamped_files(
            file_prefix.trim_end_matches('.'),
            &[hours_ago(5), hours_ago(3), hours_ago(1)],
        )
        .await?;

        let files = test_list_files_after(&awsl, file_prefix, None).await?;
        assert_eq!(files.len(), 3);

        awsl.cleanup().await?;

        Ok(())
    }

    async fn create_aws_local_with_timestamped_files(
        file_prefix: &str,
        timestamps: &[DateTime<Utc>],
    ) -> TestResult<AwsLocal> {
        let awsl = AwsLocal::new().await;
        awsl.create_bucket().await?;

        for timestamp in timestamps {
            awsl.put_protos_at_time(file_prefix, vec![TestMsg {}], *timestamp)
                .await?;
        }

        Ok(awsl)
    }

    async fn test_list_files_after(
        awsl: &AwsLocal,
        file_prefix: &str,
        after: Option<DateTime<Utc>>,
    ) -> TestResult<Vec<FileInfo>> {
        let files = _list_files(&awsl.aws_client(), awsl.bucket(), file_prefix, after)
            .try_collect()
            .await?;
        Ok(files)
    }

    fn hours_ago(hours: u64) -> DateTime<Utc> {
        Utc::now() - Duration::from_hours(hours)
    }
}
