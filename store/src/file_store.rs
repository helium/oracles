use crate::{
    env_var, error::DecodeError, BytesMutStream, Error, FileInfo, FileInfoStream, FileType, Result,
};
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_sdk_s3::{types::ByteStream, Client, Endpoint, Region};
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use http::Uri;
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct FileStore {
    pub(crate) bucket: String,
    client: Client,
}

impl FileStore {
    pub async fn from_env() -> Result<Self> {
        let endpoint: Option<Endpoint> = env_var("BUCKET_ENDPOINT")?
            .map_or_else(
                || Ok(None),
                |str| Uri::from_str(&str).map(Endpoint::immutable).map(Some),
            )
            .map_err(DecodeError::from)?;
        let region =
            env_var("BUCKET_REGION")?.map_or_else(|| Region::new("us-west-2"), Region::new);
        let bucket =
            env_var("BUCKET")?.ok_or_else(|| Error::not_found("BUCKET env var not found"))?;
        Self::new(endpoint, region, bucket).await
    }

    pub async fn new(
        endpoint: Option<Endpoint>,
        default_region: impl ProvideRegion + 'static,
        bucket: impl Into<String>,
    ) -> Result<Self> {
        let region_provider = RegionProviderChain::default_provider().or_else(default_region);

        let mut config = aws_config::from_env().region(region_provider);
        if let Some(endpoint) = endpoint {
            config = config.endpoint_resolver(endpoint);
        }
        let config = config.load().await;

        let client = Client::new(&config);
        Ok(Self {
            client,
            bucket: bucket.into(),
        })
    }

    pub async fn list_all<A, B, F>(
        &self,
        file_type: F,
        after: A,
        before: B,
    ) -> Result<Vec<FileInfo>>
    where
        F: Into<Option<FileType>> + Copy,
        A: Into<Option<DateTime<Utc>>> + Copy,
        B: Into<Option<DateTime<Utc>>> + Copy,
    {
        self.list(file_type, after, before).try_collect().await
    }

    pub fn list<A, B, F>(&self, file_type: F, after: A, before: B) -> FileInfoStream
    where
        F: Into<Option<FileType>> + Copy,
        A: Into<Option<DateTime<Utc>>> + Copy,
        B: Into<Option<DateTime<Utc>>> + Copy,
    {
        let prefix = file_type.into().map(|file_type| file_type.to_string());
        let before = before.into();
        let after = after.into();

        let stream = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_prefix(prefix)
            .into_paginator()
            .send();
        stream
            .flat_map(move |entry| match entry {
                Ok(output) => {
                    let filtered = output
                        .contents
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|obj| {
                            if FileInfo::matches(obj.key().unwrap_or_default()) {
                                Some(FileInfo::try_from(&obj).unwrap())
                            } else {
                                None
                            }
                        })
                        .filter(move |info| after.map_or(true, |v| info.timestamp >= v))
                        .filter(move |info| before.map_or(true, |v| info.timestamp <= v))
                        .map(Ok);
                    stream::iter(filtered).boxed()
                }
                Err(err) => stream::once(async move { Err(Error::s3_error(err)) }).boxed(),
            })
            .boxed()
    }

    pub async fn put(&self, file: &Path) -> Result {
        let byte_stream = ByteStream::from_path(&file)
            .await
            .map_err(|_| Error::not_found(format!("could not open {}", file.display())))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(file.file_name().map(|name| name.to_string_lossy()).unwrap())
            .body(byte_stream)
            .send()
            .map_ok(|_| ())
            .map_err(Error::s3_error)
            .await
    }

    pub async fn remove(&self, key: &str) -> Result {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .map_ok(|_| ())
            .map_err(Error::s3_error)
            .await
    }

    pub async fn get<K>(&self, key: K) -> Result<ByteStream>
    where
        K: Into<String>,
    {
        Self::_get(self.client.clone(), self.bucket.clone(), key).await
    }

    async fn _get<K>(client: Client, bucket: String, key: K) -> Result<ByteStream>
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
            .await
    }

    /// Stream a series of ordered items from the store from remote files with
    /// the given keys.
    pub fn source(&self, infos: FileInfoStream) -> BytesMutStream {
        let bucket = self.bucket.clone();
        let client = self.client.clone();
        infos
            .map_ok(move |info| Self::_get(client.clone(), bucket.clone(), info.key))
            .try_buffered(2)
            .flat_map(|stream| match stream {
                Ok(stream) => stream_source(stream),
                Err(err) => stream::once(async move { Err(err) }).boxed(),
            })
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
            .map_ok(move |info| Self::_get(client.clone(), bucket.clone(), info.key))
            .try_buffer_unordered(workers)
            .flat_map(|stream| match stream {
                Ok(stream) => stream_source(stream),
                Err(err) => stream::once(async move { Err(err) }).boxed(),
            })
            .boxed()
    }
}

fn stream_source(stream: ByteStream) -> BytesMutStream {
    use async_compression::tokio::bufread::GzipDecoder;
    use tokio_util::{
        codec::{length_delimited::LengthDelimitedCodec, FramedRead},
        io::StreamReader,
    };

    Box::pin(
        FramedRead::new(
            GzipDecoder::new(StreamReader::new(stream)),
            LengthDelimitedCodec::new(),
        )
        .map_err(Error::from),
    )
}
