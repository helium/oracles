use crate::writer::BranchWriter;
use crate::Result;
use futures::{Stream, StreamExt};
use serde::Serialize;
use std::marker::PhantomData;

/// A session-based writer that manages the write-audit-publish branch lifecycle.
///
/// `StagedWriter` provides an ergonomic API over the WAP workflow:
/// 1. Creates a branch on construction
/// 2. Supports multiple writes to the branch
/// 3. Publishes (fast-forwards main) or aborts (deletes branch) on finalization
///
/// Both `publish()` and `abort()` consume the session, preventing
/// use-after-finalization at compile time. If the session is dropped without
/// being finalized, a warning is logged.
pub struct StagedWriter<'a, T, W>
where
    T: Serialize + Send + Sync + 'static,
    W: BranchWriter<T>,
{
    writer: &'a mut W,
    branch_name: String,
    finalized: bool,
    _marker: PhantomData<T>,
}

impl<'a, T, W> StagedWriter<'a, T, W>
where
    T: Serialize + Send + Sync + 'static,
    W: BranchWriter<T>,
{
    pub(crate) async fn new(
        writer: &'a mut W,
        branch_name: impl Into<String>,
    ) -> Result<StagedWriter<'a, T, W>> {
        let branch_name = branch_name.into();
        writer.create_branch(&branch_name).await?;
        tracing::debug!(branch_name, "staged writer created branch");
        Ok(Self {
            writer,
            branch_name,
            finalized: false,
            _marker: PhantomData,
        })
    }

    /// Returns the name of the staging branch.
    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }

    /// Write records to the staging branch.
    ///
    /// Multiple calls accumulate data on the branch. On failure, the branch
    /// is cleaned up automatically.
    pub async fn write(&mut self, records: Vec<T>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let result = self
            .writer
            .write_to_branch(&self.branch_name, records)
            .await;
        if result.is_err() {
            self.cleanup().await;
        }
        result
    }

    /// Write a stream of records to the staging branch.
    ///
    /// Collects the stream into a `Vec` and delegates to [`write()`](Self::write).
    pub async fn write_stream<S>(&mut self, stream: S) -> Result<()>
    where
        S: Stream<Item = T> + Send + 'static,
    {
        let records: Vec<T> = stream.collect().await;
        self.write(records).await
    }

    /// Publish the staging branch by fast-forwarding main to its snapshot,
    /// then deleting the branch. Consumes the session.
    ///
    /// On failure, the branch is cleaned up automatically.
    pub async fn publish(mut self) -> Result<()> {
        self.finalized = true;
        let result = self.writer.publish_branch(&self.branch_name).await;
        if result.is_err() {
            self.cleanup().await;
        }
        result
    }

    /// Abort the session by deleting the staging branch. Consumes the session.
    pub async fn abort(mut self) -> Result<()> {
        self.finalized = true;
        self.writer.delete_branch(&self.branch_name).await
    }

    /// Best-effort branch cleanup. Logs errors rather than propagating them.
    async fn cleanup(&mut self) {
        self.finalized = true;
        if let Err(err) = self.writer.delete_branch(&self.branch_name).await {
            tracing::warn!(
                branch_name = self.branch_name,
                error = %err,
                "failed to clean up staging branch"
            );
        }
    }
}

impl<T, W> Drop for StagedWriter<'_, T, W>
where
    T: Serialize + Send + Sync + 'static,
    W: BranchWriter<T>,
{
    fn drop(&mut self) {
        if !self.finalized {
            tracing::warn!(
                branch_name = self.branch_name,
                "StagedWriter dropped without calling publish() or abort(); \
                 branch may still exist"
            );
        }
    }
}
