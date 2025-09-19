extern crate tls_init;

pub mod cli;
pub mod coverage;
pub mod entropy_report;
mod error;
pub mod file_info;
pub mod file_info_poller;
pub mod file_sink;
pub mod file_source;
pub mod file_upload;
pub mod hex_boost;
pub mod iot_beacon_report;
pub mod iot_invalid_poc;
pub mod iot_packet;
pub mod iot_valid_poc;
pub mod iot_witness_report;
pub mod mobile_ban;
pub mod mobile_radio_invalidated_threshold;
pub mod mobile_radio_threshold;
pub mod mobile_session;
pub mod mobile_subscriber;
pub mod mobile_transfer;
pub mod reward_manifest;
mod settings;
pub mod speedtest;
pub mod subscriber_verified_mapping_event;
pub mod subscriber_verified_mapping_event_ingest_report;
pub mod traits;
pub mod unique_connections;
pub mod usage_counts;
pub mod verified_subscriber_verified_mapping_event_ingest_report;
pub mod wifi_heartbeat;

use std::path::Path;

use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types_convert::stream::PaginationStreamExt;
use chrono::{DateTime, Utc};
pub use cli::bucket::FileFilter;
pub use error::{Error, Result};
pub use file_info::{FileInfo, FileType};
pub use file_sink::{FileSink, FileSinkBuilder};
pub use iot_valid_poc::SCALING_PRECISION;
pub use settings::Settings;

use bytes::BytesMut;
use futures::{
    future,
    stream::{self, BoxStream},
    FutureExt, StreamExt, TryFutureExt, TryStreamExt,
};

pub type Client = aws_sdk_s3::Client;
pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileInfoStream = Stream<FileInfo>;
pub type BytesMutStream = Stream<BytesMut>;

pub async fn new_client(
    endpoint: Option<String>,
    _access_key_id: Option<String>,
    _secret_access_key: Option<String>,
) -> Client {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let mut s3_config = aws_sdk_s3::config::Builder::from(&config);
    if let Some(endpoint) = endpoint {
        s3_config = s3_config.endpoint_url(endpoint);
    }

    #[cfg(feature = "local")]
    {
        // NOTE(mj): If you see something like a DNS error, this is probably
        // the culprit. Need to find a way to make this configurable. It
        // would be nice to allow the "local" feature to be active, but not
        // enforce path style.
        s3_config = s3_config.force_path_style(true);

        if let Some((access_key_id, secret_access_key)) = _access_key_id.zip(_secret_access_key) {
            let creds = aws_sdk_s3::config::Credentials::builder()
                .provider_name("Static")
                .access_key_id(access_key_id)
                .secret_access_key(secret_access_key);

            s3_config = s3_config.credentials_provider(creds.build());
        }
    }

    Client::from_conf(s3_config.build())
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
    let file_type: String = prefix.into();
    let before = before.into();
    let after = after.into();

    client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&file_type)
        .set_start_after(after.map(|dt| FileInfo::from((file_type, dt)).into()))
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_ok(|page| stream::iter(page.contents.unwrap_or_default()).map(Ok))
        .map_err(|err| Error::from(aws_sdk_s3::Error::from(err)))
        .try_flatten()
        .try_filter_map(|file| future::ready(FileInfo::try_from(&file).map(Some)))
        .try_filter(move |info| future::ready(after.is_none_or(|v| info.timestamp > v)))
        .try_filter(move |info| future::ready(before.is_none_or(|v| info.timestamp <= v)))
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
        .await
        .map_err(|_| Error::not_found(format!("could not open {}", file.display())))?;
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
            .map_err(Error::s3_error)
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
            .map_err(Error::s3_error)
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
        .map_err(Error::s3_error)
        .fuse()
        .await
}

#[cfg(test)]
tls_init::include_tls_tests!();
