use crate::{without_caching, BucketClient, GzippedFramedFile};
use chrono::{DateTime, Utc};

#[derive(thiserror::Error, Debug)]
#[error("aws local error: {0}")]
pub struct AwsLocalError(Box<dyn std::error::Error + Send + Sync>);

impl AwsLocalError {
    pub fn new<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self(Box::new(err))
    }
}

pub type Result<T> = std::result::Result<T, AwsLocalError>;

// Interacts with an S3-compatible object storage (RustFS).
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
            .await
            .map_err(AwsLocalError::new)?;

        Ok(())
    }

    pub async fn delete_bucket(&self) -> Result<()> {
        let files = self
            .client
            .list_all_files("", None, None)
            .await
            .map_err(AwsLocalError::new)?;

        let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = files
            .into_iter()
            .map(|fi| {
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(fi.key)
                    .build()
                    .map_err(AwsLocalError::new)
            })
            .collect::<Result<_>>()?;

        self.client
            .client
            .delete_objects()
            .bucket(&self.client.bucket)
            .delete(
                aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(objects))
                    .build()
                    .map_err(AwsLocalError::new)?,
            )
            .send()
            .await
            .map_err(AwsLocalError::new)?;

        self.client
            .client
            .delete_bucket()
            .bucket(&self.client.bucket)
            .send()
            .await
            .map_err(AwsLocalError::new)?;

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
        let tempdir = tempfile::tempdir().map_err(AwsLocalError::new)?;
        let mut file = GzippedFramedFile::builder()
            .path(&tempdir)
            .prefix(file_prefix)
            .time(timestamp)
            .build()
            .await
            .map_err(AwsLocalError::new)?;

        let bytes: Vec<bytes::Bytes> = protos
            .into_iter()
            .map(|m| m.encode_to_vec().into())
            .collect();

        file.write_all(bytes).await.map_err(AwsLocalError::new)?;
        let file_path = file.close().await.map_err(AwsLocalError::new)?;

        self.client
            .put_file(&file_path)
            .await
            .map_err(AwsLocalError::new)?;

        tokio::fs::remove_file(&file_path)
            .await
            .map_err(AwsLocalError::new)?;

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
    host: Option<String>,
    port: Option<u16>,
    bucket: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
}

impl AwsLocalBuilder {
    pub fn region(mut self, region: String) -> Self {
        self.region = Some(region);
        self
    }

    pub fn host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    pub fn credentials_same(mut self, cred: impl Into<String>) -> Self {
        let cred = cred.into();
        self.access_key_id = Some(cred.clone());
        self.secret_access_key = Some(cred.clone());
        self
    }

    pub fn access_key_id(mut self, key_id: impl Into<String>) -> Self {
        self.access_key_id = Some(key_id.into());
        self
    }

    pub fn secret_access_key(mut self, secret: impl Into<String>) -> Self {
        self.secret_access_key = Some(secret.into());
        self
    }

    pub async fn build(self) -> AwsLocal {
        let endpoint = format!(
            "http://{}:{}",
            self.host.unwrap_or_else(env_defaults::awslocal_host),
            self.port.unwrap_or_else(env_defaults::awslocal_port)
        );

        let client = without_caching(BucketClient::new(
            self.bucket.unwrap_or_else(gen_bucket_name),
            self.region.or(Some("us-east-1".to_string())),
            Some(endpoint.clone()),
            self.access_key_id.or(Some("admin".to_string())),
            self.secret_access_key.or(Some("admin".to_string())),
        ))
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
    #[allow(unsafe_code)]
    pub unsafe fn drop_without_bucket_delete(mut self) {
        self.gaurd_drop = false;
    }
}

fn gen_bucket_name() -> String {
    // Get current timestamp in milliseconds
    let now = chrono::Utc::now();
    let timestamp = now.timestamp_millis();

    // Extract only the function name (last part after ::) from thread name
    let test_name = std::thread::current()
        .name()
        .and_then(|name| name.split("::").last())
        .map(|name| {
            name.chars()
                .take(32) // Limit test name portion to 32 chars
                .map(|c| {
                    if c.is_alphanumeric() || c == '-' {
                        c
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
        })
        .filter(|name| !name.is_empty());

    match test_name {
        Some(name) => format!("{}-{}", name, timestamp),
        None => format!("test-{}", timestamp),
    }
}

pub mod env_defaults {
    const AWSLOCAL_HOST_ENV: &str = "AWSLOCAL_HOST";
    const AWSLOCAL_PORT_ENV: &str = "AWSLOCAL_PORT";

    pub fn awslocal_host() -> String {
        env_str(AWSLOCAL_HOST_ENV, "localhost")
    }

    pub fn awslocal_port() -> u16 {
        env_port(AWSLOCAL_PORT_ENV, 4566)
    }

    // ======= Helpers ====================
    fn to_u16(port: String, label: &str) -> u16 {
        port.parse::<u16>()
            .unwrap_or_else(|val| panic!("u16 parseable {label} port: {val}"))
    }
    fn env_str(var: &str, default: &str) -> String {
        std::env::var(var).unwrap_or_else(|_| default.to_string())
    }
    fn env_port(var: &str, default: u16) -> u16 {
        std::env::var(var)
            .map(|port| to_u16(port, var))
            .unwrap_or(default)
    }
}
