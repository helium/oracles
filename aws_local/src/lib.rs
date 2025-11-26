use anyhow::Result;
use chrono::Utc;
use file_store::{BucketClient, GzippedFramedFile};
use std::env;
use uuid::Uuid;

pub const AWSLOCAL_ENDPOINT_ENV: &str = "AWSLOCAL_ENDPOINT";
pub const AWSLOCAL_DEFAULT_ENDPOINT: &str = "http://localhost:4566";

pub fn aws_local_default_endpoint() -> String {
    env::var(AWSLOCAL_ENDPOINT_ENV).unwrap_or_else(|_| AWSLOCAL_DEFAULT_ENDPOINT.to_string())
}

pub fn gen_bucket_name() -> String {
    format!("mvr-{}-{}", Uuid::new_v4(), Utc::now().timestamp_millis())
}

// Interacts with the locastack.
pub struct AwsLocal {
    client: BucketClient,
}

impl AwsLocal {
    pub fn builder() -> AwsLocalBuilder {
        AwsLocalBuilder::default()
    }

    pub fn bucket(&self) -> &str {
        &self.client.bucket
    }

    pub fn bucket_client(&self) -> BucketClient {
        self.client.clone()
    }

    pub fn aws_client(&self) -> aws_sdk_s3::Client {
        self.client.client.clone()
    }

    pub async fn create_bucket(&self) -> Result<()> {
        self.client
            .client
            .create_bucket()
            .bucket(&self.client.bucket)
            .send()
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }

    pub async fn delete_bucket(&self) -> Result<()> {
        let files = self.client.list_all_files("", None, None).await?;

        let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = files
            .into_iter()
            .map(|fi| {
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(fi.key)
                    .build()
            })
            .collect::<Result<_, _>>()?;

        self.client
            .client
            .delete_objects()
            .bucket(&self.client.bucket)
            .delete(
                aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(objects))
                    .build()?,
            )
            .send()
            .await?;

        self.client
            .client
            .delete_bucket()
            .bucket(&self.client.bucket)
            .send()
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }

    pub async fn put_protos<T: prost::Message>(
        &self,
        file_prefix: String,
        protos: Vec<T>,
    ) -> Result<String> {
        let tempdir = tempfile::tempdir()?;
        let mut file = GzippedFramedFile::builder()
            .path(&tempdir)
            .prefix(file_prefix)
            .build()
            .await?;

        let bytes: Vec<bytes::Bytes> = protos
            .into_iter()
            .map(|m| m.encode_to_vec().into())
            .collect();

        file.write_all(bytes).await?;
        let file_path = file.close().await?;

        self.client.put_file(&file_path).await?;

        tokio::fs::remove_file(&file_path).await?;

        Ok(file_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("invalid file name upload to s3")
            .into())
    }
}

#[derive(Debug, Clone, Default)]
pub struct AwsLocalBuilder {
    region: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
}

impl AwsLocalBuilder {
    pub fn region(mut self, region: String) -> Self {
        self.region = Some(region);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    pub async fn build(self) -> AwsLocal {
        let client = BucketClient::new(
            self.bucket.unwrap_or_else(gen_bucket_name),
            self.region.or(Some("us-east-1".to_string())),
            self.endpoint.or_else(|| Some(aws_local_default_endpoint())),
            Some("fake".to_string()),
            Some("fake".to_string()),
        )
        .await;

        AwsLocal { client }
    }
}

// Until AsyncDrop is stablized, ensure buckets are cleaned up in tests. This
// causes tests to fail unless they call `AwsLocal::delete_bucket()`.
impl Drop for AwsLocal {
    fn drop(&mut self) {
        panic!(
            "
=============== WARNING!!! ===============
Cannot autodrop AwsLocal, must call `AwsLocal::cleanup()`
=============== WARNING!!! ===============
            "
        );
    }
}

impl AwsLocal {
    /// Drop the AwsLocal Client deleting the test bucket in the process.
    pub async fn cleanup(self) -> Result<()> {
        let res = self.delete_bucket().await;

        // Secret sauce to bypasses the panic in the Drop impl for AwsLocal.
        let _ = std::mem::ManuallyDrop::new(self);

        res
    }

    /// WARNING: This function should only be used in active development if you
    /// want to run a test and inspect the contents of a test bucket.
    ///
    /// # Safety
    /// It is marked with unsafe to make the calling code extra clear that this
    /// should not be left in use.
    pub unsafe fn drop_without_bucket_delete(self) {
        let _ = std::mem::ManuallyDrop::new(self);
    }
}
