use crate::{env_var, error::DecodeError, Error, FileInfo, FileType, Result};
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_sdk_s3::{types::ByteStream, Client, Endpoint, Error as SdkError, Region};
use chrono::{DateTime, Utc};
use http::Uri;
use std::path::Path;
use std::str::FromStr;
use tokio::io::AsyncRead;

#[derive(Debug, Clone)]
pub struct FileStore {
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
        Self::new(endpoint, region).await
    }

    pub async fn new(
        endpoint: Option<Endpoint>,
        default_region: impl ProvideRegion + 'static,
    ) -> Result<Self> {
        let region_provider = RegionProviderChain::default_provider().or_else(default_region);

        let mut config = aws_config::from_env().region(region_provider);
        if let Some(endpoint) = endpoint {
            config = config.endpoint_resolver(endpoint);
        }
        let config = config.load().await;

        let client = Client::new(&config);
        Ok(Self { client })
    }

    pub async fn list(
        &self,
        bucket: &str,
        file_type: Option<FileType>,
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> Result<Option<Vec<FileInfo>>> {
        let prefix = file_type.as_ref().map(|file_type| file_type.to_string());
        let resp = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .set_prefix(prefix)
            .send()
            .await
            .map_err(SdkError::from)?;

        let result = resp
            .contents()
            .unwrap_or_default()
            .iter()
            // Filter out any keys that don't match what a file info expects
            // instead of erroring
            .filter(|obj| FileInfo::matches(obj.key().unwrap_or_default()))
            .map(|obj| FileInfo::try_from(obj).unwrap())
            .filter(|info| after.map_or(true, |v| info.timestamp > v))
            .filter(|info| before.map_or(true, |v| info.timestamp < v))
            .collect::<Vec<FileInfo>>();

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    pub async fn put(&self, bucket: &str, file: &Path) -> Result {
        let byte_stream = ByteStream::from_path(&file)
            .await
            .map_err(|_| Error::not_found(format!("could not open {}", file.display())))?;
        self.client
            .put_object()
            .bucket(bucket)
            .key(file.file_name().map(|name| name.to_string_lossy()).unwrap())
            .body(byte_stream)
            .send()
            .await
            .map_err(SdkError::from)?;
        Ok(())
    }

    pub async fn remove(&self, bucket: &str, key: &str) -> Result {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(SdkError::from)?;
        Ok(())
    }

    pub async fn get(&self, bucket: &str, key: &str) -> Result<impl AsyncRead> {
        let output = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(SdkError::from)?;
        Ok(output.body.into_async_read())
    }
}
