use crate::{
    settings::{self, Settings},
    BytesMutStream, Error, FileInfo, FileInfoStream, Result,
};
use aws_config::{
    meta::region::RegionProviderChain, retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion,
    Region,
};
use aws_sdk_s3::{primitives::ByteStream, Client};
use aws_smithy_types_convert::stream::PaginationStreamExt;
use chrono::{DateTime, Utc};
use futures::{future, stream, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct FileStore {
    pub(crate) bucket: String,
    client: Client,
}

pub struct FileData {
    pub info: FileInfo,
    pub stream: BytesMutStream,
}

impl FileStore {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let Settings {
            bucket,
            endpoint,
            access_key_id,
            secret_access_key,
            region,
        } = settings.clone();
        Self::new(
            bucket,
            endpoint,
            Some(region),
            None,
            None,
            access_key_id,
            secret_access_key,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        bucket: String,
        endpoint: Option<String>,
        region: Option<String>,
        timeout_config: Option<TimeoutConfig>,
        retry_config: Option<RetryConfig>,
        _access_key_id: Option<String>,
        _secret_access_key: Option<String>,
    ) -> Result<Self> {
        let region = Region::new(region.unwrap_or_else(settings::default_region));
        let region_provider = RegionProviderChain::first_try(region).or_default_provider();

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;

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

            if let Some((access_key_id, secret_access_key)) = _access_key_id.zip(_secret_access_key)
            {
                let creds = aws_sdk_s3::config::Credentials::builder()
                    .provider_name("Static")
                    .access_key_id(access_key_id)
                    .secret_access_key(secret_access_key);

                s3_config = s3_config.credentials_provider(creds.build());
            }
        }

        if let Some(timeout) = timeout_config {
            s3_config = s3_config.timeout_config(timeout);
        }

        if let Some(retry) = retry_config {
            s3_config = s3_config.retry_config(retry);
        }

        let client = Client::from_conf(s3_config.build());
        Ok(Self { client, bucket })
    }

    pub async fn list_all<A, B>(
        &self,
        file_type: &str,
        after: A,
        before: B,
    ) -> Result<Vec<FileInfo>>
    where
        A: Into<Option<DateTime<Utc>>> + Copy,
        B: Into<Option<DateTime<Utc>>> + Copy,
    {
        self.list(file_type, after, before).try_collect().await
    }

    pub fn list<A, B>(&self, prefix: &str, after: A, before: B) -> FileInfoStream
    where
        A: Into<Option<DateTime<Utc>>> + Copy,
        B: Into<Option<DateTime<Utc>>> + Copy,
    {
        let file_type = prefix.to_string();
        let before = before.into();
        let after = after.into();

        self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(file_type.to_string())
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

    pub async fn put(&self, file: &Path) -> Result {
        let byte_stream = ByteStream::from_path(&file)
            .await
            .map_err(|_| Error::not_found(format!("could not open {}", file.display())))?;
        poc_metrics::record_duration!(
            "file_store_put_duration",
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(file.file_name().map(|name| name.to_string_lossy()).unwrap())
                .body(byte_stream)
                .content_type("application/octet-stream")
                .send()
                .map_ok(|_| ())
                .map_err(Error::s3_error)
                .await
        )
    }

    pub async fn remove(&self, key: &str) -> Result {
        poc_metrics::record_duration!(
            "file_store_remove_duration",
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .map_ok(|_| ())
                .map_err(Error::s3_error)
                .await
        )
    }

    pub async fn get_raw<K>(&self, key: K) -> Result<ByteStream>
    where
        K: Into<String>,
    {
        get_byte_stream(self.client.clone(), self.bucket.clone(), key).await
    }

    pub async fn get<K>(&self, key: K) -> Result<BytesMutStream>
    where
        K: Into<String>,
    {
        Ok(stream_source(self.get_raw(key).await?))
    }

    /// Stream a series of ordered items from the store from remote files with
    /// the given keys.
    pub fn source(&self, infos: FileInfoStream) -> BytesMutStream {
        let bucket = self.bucket.clone();
        let client = self.client.clone();
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

    /// Stream a series of unordered items from the store from remote files with
    /// the given keys using a number of workers.  This allows for an unordered
    /// stream of buffers to be produced as soon as available from up to
    /// "worker" number of remote files
    pub fn source_unordered(&self, workers: usize, infos: FileInfoStream) -> BytesMutStream {
        let bucket = self.bucket.clone();
        let client = self.client.clone();
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

    pub async fn stream_file(&self, file_info: FileInfo) -> Result<BytesMutStream> {
        get_byte_stream(self.client.clone(), self.bucket.clone(), file_info)
            .await
            .map(stream_source)
    }
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

async fn get_byte_stream<K>(client: Client, bucket: String, key: K) -> Result<ByteStream>
where
    K: Into<String>,
{
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
