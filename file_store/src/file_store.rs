use crate::{
    error::DecodeError,
    settings::{self, Settings},
    BytesMutStream, Error, FileInfo, FileInfoStream, Result,
};
use aws_config::{meta::region::RegionProviderChain, retry::RetryConfig, timeout::TimeoutConfig};
use aws_sdk_s3::{types::ByteStream, Client, Endpoint, Region};
use chrono::{DateTime, Utc};
use futures::FutureExt;
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use http::Uri;
use std::path::Path;
use std::str::FromStr;

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

    pub async fn new(
        bucket: String,
        endpoint: Option<String>,
        region: Option<String>,
        timeout_config: Option<TimeoutConfig>,
        retry_config: Option<RetryConfig>,
        _access_key_id: Option<String>,
        _secret_access_key: Option<String>,
    ) -> Result<Self> {
        let endpoint: Option<Endpoint> = match &endpoint {
            Some(endpoint) => Uri::from_str(endpoint)
                .map(Endpoint::immutable)
                .map(Some)
                .map_err(DecodeError::from)?,
            _ => None,
        };
        let region = Region::new(region.unwrap_or_else(settings::default_region));
        let region_provider = RegionProviderChain::first_try(region).or_default_provider();

        let mut config = aws_config::from_env().region(region_provider);
        if let Some(endpoint) = endpoint {
            config = config.endpoint_resolver(endpoint);
        }

        #[cfg(feature = "local")]
        if _access_key_id.is_some() && _secret_access_key.is_some() {
            let creds = aws_types::credentials::Credentials::from_keys(
                _access_key_id.as_ref().unwrap(),
                _secret_access_key.as_ref().unwrap(),
                None,
            );
            config = config.credentials_provider(creds);
        }

        if let Some(timeout) = timeout_config {
            config = config.timeout_config(timeout);
        }

        if let Some(retry) = retry_config {
            config = config.retry_config(retry);
        }

        let config = config.load().await;

        let client = Client::new(&config);
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

        let request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(file_type.to_string())
            .set_start_after(after.map(|dt| FileInfo::from((file_type, dt)).into()));

        futures::stream::unfold(
            (request, true, None),
            |(req, first_time, next)| async move {
                if first_time || next.is_some() {
                    let list_objects_response =
                        req.clone().set_continuation_token(next).send().await;

                    let next_token = list_objects_response
                        .as_ref()
                        .ok()
                        .and_then(|r| r.next_continuation_token())
                        .map(|x| x.to_owned());

                    Some((list_objects_response, (req, false, next_token)))
                } else {
                    None
                }
            },
        )
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
                    .filter(move |info| after.is_none_or(|v| info.timestamp > v))
                    .filter(move |info| before.is_none_or(|v| info.timestamp <= v))
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
