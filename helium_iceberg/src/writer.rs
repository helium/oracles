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
pub(crate) trait BranchWriter<T>: Send + Sync
where
    T: Serialize + Send,
{
    async fn create_branch(&self, branch_name: &str) -> Result;
    async fn write_to_branch(&self, branch_name: &str, records: Vec<T>, wap_id: &str) -> Result;
    async fn publish_branch(&self, branch_name: &str) -> Result;
    async fn delete_branch(&self, branch_name: &str) -> Result;
}

/// Outcome of a [`StagedWriter::stage`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// Records were staged successfully.
    Written,
    /// The WAP was already completed in a prior attempt — nothing to do.
    AlreadyComplete,
}

/// Trait for idempotent write-audit-publish (WAP) workflows.
///
/// Implementors handle the full WAP lifecycle: creating a branch, writing
/// records, detecting prior completion, and publishing the branch.
#[async_trait]
pub trait StagedWriter<T>: Send + Sync
where
    T: Serialize + Send,
{
    /// Idempotently stage records for a WAP session.
    ///
    /// Creates a branch, writes records, and returns [`WriteOutcome::Written`].
    /// Returns [`WriteOutcome::AlreadyComplete`] if `wap_id` was previously published.
    async fn stage(&self, wap_id: &str, records: Vec<T>) -> Result<WriteOutcome>;

    /// Publish a staged WAP branch by fast-forwarding main.
    async fn publish(&self, wap_id: &str) -> Result;
}
