use crate::{BucketClient, GzippedFramedFile};
use chrono::{DateTime, Utc};
use std::env;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

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
    endpoint: String,
    gaurd_drop: bool,
}

impl AwsLocal {
    pub async fn new() -> AwsLocal {
        Self::builder().build().await
    }

    pub fn builder() -> AwsLocalBuilder {
        AwsLocalBuilder::default()
    }

    pub fn bucket(&self) -> &str {
        &self.client.bucket
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
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
            .await?;

        Ok(())
    }

    pub async fn delete_bucket(&self) -> Result<()> {
        let files = self.client.list_all_files("", None, None).await?;

        let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = files
            .into_iter()
            .map(|fi| {
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(fi.key)
                    .build()
                    .map_err(Into::into)
            })
            .collect::<Result<_>>()?;

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
            .await?;

        Ok(())
    }

    pub async fn put_protos<T: prost::Message>(
        &self,
        file_prefix: impl Into<String>,
        protos: Vec<T>,
    ) -> Result<String> {
        self.put_protos_at_time(file_prefix, protos, Utc::now())
            .await
    }

    pub async fn put_protos_at_time<T: prost::Message>(
        &self,
        file_prefix: impl Into<String>,
        protos: Vec<T>,
        timestamp: DateTime<Utc>,
    ) -> Result<String> {
        let tempdir = tempfile::tempdir()?;
        let mut file = GzippedFramedFile::builder()
            .path(&tempdir)
            .prefix(file_prefix)
            .time(timestamp)
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

    fn next_fake_credential(&self) -> String {
        // Generate unique credentials per AwsLocal instance to avoid CLIENT_MAP
        // cache collisions. This prevents "dispatch task is gone" errors in tests
        // where cached clients' dispatch tasks can outlive the test runtime.
        use std::sync::atomic::{AtomicUsize, Ordering};
        static BUILT_CLIENT_COUNT: AtomicUsize = AtomicUsize::new(0);
        let count = BUILT_CLIENT_COUNT.fetch_add(1, Ordering::Relaxed);
        format!("fake-{count}")
    }

    pub async fn build(self) -> AwsLocal {
        let fake_cred = self.next_fake_credential();
        let endpoint = self.endpoint.unwrap_or_else(aws_local_default_endpoint);

        let client = BucketClient::new(
            self.bucket.unwrap_or_else(gen_bucket_name),
            self.region.or(Some("us-east-1".to_string())),
            Some(endpoint.clone()),
            Some(fake_cred.clone()),
            Some(fake_cred),
        )
        .await;

        AwsLocal {
            client,
            endpoint,
            gaurd_drop: true,
        }
    }
}

// Until AsyncDrop is stablized, ensure buckets are cleaned up in tests. This
// causes tests to fail unless they call `AwsLocal::delete_bucket()`.
impl Drop for AwsLocal {
    fn drop(&mut self) {
        if self.gaurd_drop {
            println!(
                "
=============== WARNING!!! ===============
Cannot autodrop AwsLocal, must call `AwsLocal::cleanup()`
=============== WARNING!!! ===============
                "
            );
        }
    }
}

impl AwsLocal {
    /// Drop the AwsLocal Client deleting the test bucket in the process.
    pub async fn cleanup(mut self) -> Result<()> {
        self.gaurd_drop = false;
        self.delete_bucket().await
    }

    /// WARNING: This function should only be used in active development if you
    /// want to run a test and inspect the contents of a test bucket.
    ///
    /// # Safety
    /// It is marked with unsafe to make the calling code extra clear that this
    /// should not be left in use.
    pub unsafe fn drop_without_bucket_delete(mut self) {
        self.gaurd_drop = false;
    }
}
