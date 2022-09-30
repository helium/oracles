use crate::file_info::FileName;
use crate::{
    error::DecodeError, BytesMutStream, Error, FileContentStream, FileInfo, FileInfoStream,
    FileType, Result,
};
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_sdk_s3::{types::ByteStream, Client, Endpoint, Region};
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use http::Uri;
use std::str::FromStr;
use std::{env, path::Path};

#[derive(Debug, Clone)]
pub struct FileStore {
    pub(crate) bucket: String,
    client: Client,
}

impl FileStore {
    pub async fn from_env() -> Result<Self> {
        let endpoint: Option<Endpoint> = match env::var("BUCKET_ENDPOINT") {
            Ok(endpoint_env) => Uri::from_str(&endpoint_env)
                .map(Endpoint::immutable)
                .map(Some)
                .map_err(DecodeError::from)?,
            _ => None,
        };
        let region =
            env::var("BUCKET_REGION").map_or_else(|_| Region::new("us-west-2"), Region::new);
        let bucket = env::var("BUCKET")?;
        Self::new(endpoint, region, bucket).await
    }

    // TODO: Figure out how to better deal with multiple file stores
    pub async fn from_env_with_prefix(prefix: &str) -> Result<FileStore> {
        let endpoint: Option<Endpoint> = match env::var(&format!("{prefix}_BUCKET_ENDPOINT")) {
            Ok(endpoint_env) => Uri::from_str(&endpoint_env)
                .map(Endpoint::immutable)
                .map(Some)
                .map_err(DecodeError::from)?,
            _ => None,
        };
        let region = env::var(&format!("{prefix}_BUCKET_REGION"))
            .map_or_else(|_| Region::new("us-west-2"), Region::new);
        let bucket = env::var(&format!("{prefix}_BUCKET"))?;
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

    pub fn list<T>(
        &self,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> FileInfoStream
    where
        T: FileName,
    {
        let prefix = T::FILE_NAME;

        let stream = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
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

    pub fn list_contents<T>(
        &self,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> FileContentStream<T>
    where
        T: FileName + prost::Message + Default,
    {
        let bucket = self.bucket.clone();
        let client = self.client.clone();
        self.list::<T>(after, before)
            .map_ok(move |info| get_with_info::<T>(client.clone(), bucket.clone(), info))
            .try_buffered(2)
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

    pub async fn put_bytes(&self, file_name: &str, bytes: Vec<u8>) -> Result {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(file_name)
            .body(ByteStream::from(bytes))
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
        get(self.client.clone(), self.bucket.clone(), key).await
    }

    /// Stream a series of ordered items from the store from remote files with
    /// the given keys.
    pub fn source(&self, infos: FileInfoStream) -> BytesMutStream {
        let bucket = self.bucket.clone();
        let client = self.client.clone();
        infos
            .map_ok(move |info| get(client.clone(), bucket.clone(), info.key))
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
            .map_ok(move |info| get(client.clone(), bucket.clone(), info.key))
            .try_buffer_unordered(workers)
            .flat_map(|stream| match stream {
                Ok(stream) => stream_source(stream),
                Err(err) => stream::once(async move { Err(err) }).boxed(),
            })
            .boxed()
    }
}

async fn get<K>(client: Client, bucket: String, key: K) -> Result<ByteStream>
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

async fn get_with_info<T>(client: Client, bucket: String, info: FileInfo) -> Result<(FileInfo, T)>
where
    T: Default + prost::Message,
{
    let res = get(client, bucket, info.key.clone()).await?;
    let mut t = stream_source(res);
    let mut res = Vec::new();
    while let Some(Ok(msg)) = t.next().await {
        res.extend(msg);
    }
    Ok((info, T::decode(&*res)?))
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
