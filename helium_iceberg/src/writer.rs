use crate::Result;
use async_trait::async_trait;
use serde::Serialize;

#[async_trait]
pub trait DataWriter<T>: Send + Sync
where
    T: Serialize + Send,
{
    async fn write(&self, records: Vec<T>) -> Result;
}

#[async_trait]
pub trait BranchWriter<T>: Send + Sync
where
    T: Serialize + Send,
{
    async fn create_branch(&self, branch_name: &str) -> Result;
    async fn write_to_branch(&self, branch_name: &str, records: Vec<T>, wap_id: &str) -> Result;
    async fn publish_branch(&self, branch_name: &str) -> Result;
    async fn delete_branch(&self, branch_name: &str) -> Result;
}
