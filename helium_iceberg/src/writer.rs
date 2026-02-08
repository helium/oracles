use crate::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde::Serialize;

#[async_trait]
pub trait DataWriter<T>: Send + Sync
where
    T: Serialize + Send + Sync + 'static,
{
    async fn write(&self, records: Vec<T>) -> Result;

    async fn write_stream<S>(&self, stream: S) -> Result
    where
        S: Stream<Item = T> + Send + 'static,
    {
        let records: Vec<T> = stream.collect().await;
        self.write(records).await
    }
}

#[async_trait]
pub trait BranchWriter<T>: Send + Sync
where
    T: Serialize + Send + Sync + 'static,
{
    async fn create_branch(&mut self, branch_name: &str) -> Result;
    async fn write_to_branch(&mut self, branch_name: &str, records: Vec<T>) -> Result;
    async fn write_stream_to_branch<S>(&mut self, branch_name: &str, stream: S) -> Result
    where
        S: Stream<Item = T> + Send + 'static,
    {
        let records: Vec<T> = stream.collect().await;
        self.write_to_branch(branch_name, records).await
    }
    async fn publish_branch(&mut self, branch_name: &str) -> Result;
    async fn delete_branch(&mut self, branch_name: &str) -> Result;
}
