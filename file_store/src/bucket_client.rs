use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};

use crate::{error::Result, BytesMutStream, FileInfo, FileInfoStream};

#[derive(Clone, Debug)]
pub struct BucketClient {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
}

impl BucketClient {
    pub async fn new(
        bucket: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    ) -> Self {
        let client = crate::new_client(region, endpoint, access_key_id, secret_access_key).await;
        Self { client, bucket }
    }

    pub fn list_files<A, B>(&self, prefix: impl Into<String>, after: A, before: B) -> FileInfoStream
    where
        A: Into<Option<DateTime<Utc>>> + Copy,
        B: Into<Option<DateTime<Utc>>> + Copy,
    {
        crate::list_files(&self.client, self.bucket.clone(), prefix, after, before)
    }

    pub async fn list_all_files<A, B>(
        &self,
        prefix: impl Into<String>,
        after: A,
        before: B,
    ) -> Result<Vec<FileInfo>>
    where
        A: Into<Option<DateTime<Utc>>> + Copy,
        B: Into<Option<DateTime<Utc>>> + Copy,
    {
        crate::list_all_files(&self.client, self.bucket.clone(), prefix, after, before).await
    }

    pub async fn put_file(&self, file: &Path) -> Result {
        crate::put_file(&self.client, self.bucket.clone(), file).await
    }

    pub async fn remove_file(&self, key: impl Into<String>) -> Result {
        crate::remove_file(&self.client, self.bucket.clone(), key).await
    }

    pub async fn get_raw_file(&self, key: impl Into<String>) -> Result<ByteStream> {
        crate::get_raw_file(&self.client, self.bucket.clone(), key).await
    }

    pub async fn get_file(&self, key: impl Into<String>) -> Result<BytesMutStream> {
        crate::get_file(&self.client, self.bucket.clone(), key).await
    }

    pub fn source_files(&self, infos: FileInfoStream) -> BytesMutStream {
        crate::source_files(&self.client, self.bucket.clone(), infos)
    }

    pub fn source_files_unordered(&self, workers: usize, infos: FileInfoStream) -> BytesMutStream {
        crate::source_files_unordered(&self.client, self.bucket.clone(), workers, infos)
    }

    pub async fn stream_single_file(&self, file_info: FileInfo) -> Result<BytesMutStream> {
        crate::stream_single_file(&self.client, self.bucket.clone(), file_info).await
    }
}
