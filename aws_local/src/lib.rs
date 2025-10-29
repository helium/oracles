use anyhow::{anyhow, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use chrono::Utc;
use file_store::{file_sink, file_upload, Settings};
use std::env;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex;
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
// Used to create mocked aws buckets and files.
pub struct AwsLocal {
    pub fs_settings: Settings,
    pub file_store_client: file_store::Client,
    bucket: String,
}

impl AwsLocal {
    async fn create_aws_client(settings: &Settings) -> aws_sdk_s3::Client {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(settings.endpoint.as_ref().expect("endpoint"));

        let creds = aws_sdk_s3::config::Credentials::builder()
            .access_key_id(settings.access_key_id.as_ref().expect("access_key_id"))
            .secret_access_key(
                settings
                    .secret_access_key
                    .as_ref()
                    .expect("secret_access_key"),
            )
            .provider_name("Static")
            .build();
        s3_config = s3_config.credentials_provider(creds);

        Client::from_conf(s3_config.build())
    }

    pub async fn new(endpoint: &str, bucket: &str) -> AwsLocal {
        let settings = Settings {
            endpoint: Some(endpoint.into()),
            access_key_id: Some("random".into()),
            secret_access_key: Some("random2".into()),
        };
        let client = Self::create_aws_client(&settings).await;
        client.create_bucket().bucket(bucket).send().await.unwrap();
        AwsLocal {
            file_store_client: client,
            fs_settings: settings.clone(),
            bucket: bucket.to_string(),
        }
    }

    pub fn fs_settings(&self) -> Settings {
        self.fs_settings.clone()
    }

    pub async fn put_proto_to_aws<T: prost::Message>(
        &self,
        items: Vec<T>,
        file_type: impl ToString,
        metric_name: &'static str,
    ) -> Result<String> {
        let tmp_dir = TempDir::new()?;
        let tmp_dir_path = tmp_dir.path().to_owned();

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::new(self.file_store_client.clone(), self.bucket.clone()).await;

        let (item_sink, item_server) =
            file_sink::FileSinkBuilder::new(file_type, &tmp_dir_path, file_upload, metric_name)
                .auto_commit(false)
                .roll_time(std::time::Duration::new(15, 0))
                .create::<T>()
                .await
                .unwrap();

        for item in items {
            item_sink.write(item, &[]).await.unwrap();
        }
        let item_recv = item_sink.commit().await.unwrap();

        let uploaded_file = Arc::new(Mutex::new(String::default()));
        let up_2 = uploaded_file.clone();
        let mut timeout = std::time::Duration::new(5, 0);

        tokio::spawn(async move {
            let uploaded_files = item_recv.await.unwrap().unwrap();
            assert!(uploaded_files.len() == 1);
            let mut val = up_2.lock().await;
            *val = uploaded_files.first().unwrap().to_string();

            // After files uploaded to aws the must be removed.
            // So we wait when dir will be empty.
            // It means all files are uploaded to aws
            loop {
                if is_dir_has_files(&tmp_dir_path) {
                    let dur = std::time::Duration::from_millis(10);
                    tokio::time::sleep(dur).await;
                    timeout -= dur;
                    continue;
                }
                break;
            }

            shutdown_trigger.trigger();
        });

        tokio::try_join!(
            file_upload_server.run(shutdown_listener.clone()),
            item_server.run(shutdown_listener.clone())
        )
        .unwrap();

        tmp_dir.close()?;

        let res = uploaded_file.lock().await;
        Ok(res.clone())
    }

    pub async fn put_file_to_aws(&self, file_path: &Path) -> Result<()> {
        let path_str = file_path.display();
        if !file_path.exists() {
            return Err(anyhow!("File {path_str} is absent"));
        }
        if !file_path.is_file() {
            return Err(anyhow!("File {path_str} is not a file"));
        }
        file_store::put_file(&self.file_store_client, &self.bucket, file_path).await?;

        Ok(())
    }
}

fn is_dir_has_files(dir_path: &Path) -> bool {
    let entries = std::fs::read_dir(dir_path)
        .unwrap()
        .map(|res| res.map(|e| e.path().is_dir()))
        .collect::<Result<Vec<_>, std::io::Error>>()
        .unwrap();
    entries.contains(&false)
}
