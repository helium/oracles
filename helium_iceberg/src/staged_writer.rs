use crate::writer::BranchWriter;
use crate::Result;
use serde::Serialize;
use std::marker::PhantomData;

/// Outcome of an idempotent WAP attempt.
///
/// When using [`IcebergTable::idempotent_staged_writer`], the WAP may already
/// be complete from a prior attempt. This enum lets callers distinguish the
/// two cases without inspecting table state themselves.
pub enum IdempotentWapOutcome<'a, T, W>
where
    T: Serialize + Send,
    W: BranchWriter<T>,
{
    /// The WAP was already completed in a prior attempt — nothing to do.
    AlreadyComplete,
    /// A fresh (or recovered) writer ready for use.
    Writer(StagedWriter<'a, T, W>),
}

/// A session-based writer that manages the write-audit-publish branch lifecycle.
///
/// `StagedWriter` provides an ergonomic API over the WAP workflow:
/// 1. Creates a branch on construction
/// 2. Supports a single write to the branch
/// 3. Returns a [`StagedPublisher`] that publishes (fast-forwards main) on finalization
///
/// The `wap_id` is used as both the branch name and the snapshot summary
/// identifier, enabling idempotent retry detection.
///
/// `write` consumes `self` to enforce a single write per WAP session at
/// compile time.
pub struct StagedWriter<'a, T, W>
where
    T: Serialize + Send,
    W: BranchWriter<T>,
{
    writer: &'a W,
    wap_id: String,
    _marker: PhantomData<T>,
}

impl<'a, T, W> StagedWriter<'a, T, W>
where
    T: Serialize + Send,
    W: BranchWriter<T>,
{
    pub(crate) async fn new(
        writer: &'a W,
        wap_id: impl Into<String>,
    ) -> Result<StagedWriter<'a, T, W>> {
        let wap_id = wap_id.into();
        writer.create_branch(&wap_id).await?;
        tracing::debug!(wap_id, "staged writer created branch");
        Ok(Self {
            writer,
            wap_id,
            _marker: PhantomData,
        })
    }

    /// Returns the WAP identifier for this session (also used as the branch name).
    pub fn wap_id(&self) -> &str {
        &self.wap_id
    }

    /// Write records to the staging branch, consuming the writer.
    ///
    /// Returns a [`StagedPublisher`] that can be used to publish the branch.
    pub async fn write(self, records: Vec<T>) -> Result<StagedPublisher<'a, T, W>> {
        if !records.is_empty() {
            self.writer
                .write_to_branch(&self.wap_id, records, &self.wap_id)
                .await?;
        }

        Ok(StagedPublisher {
            writer: self.writer,
            wap_id: self.wap_id,
            _marker: PhantomData,
        })
    }

}

/// Handle returned after a successful write, exposing only `publish()`.
///
/// This type is the second half of the typestate pattern: once data has been
/// written via [`StagedWriter::write`], the
/// caller receives a `StagedPublisher` whose sole operation is to fast-forward
/// main and clean up the staging branch.
pub struct StagedPublisher<'a, T, W>
where
    T: Serialize + Send,
    W: BranchWriter<T>,
{
    writer: &'a W,
    wap_id: String,
    _marker: PhantomData<T>,
}

impl<'a, T, W> StagedPublisher<'a, T, W>
where
    T: Serialize + Send,
    W: BranchWriter<T>,
{
    /// Returns the WAP identifier for this session.
    pub fn wap_id(&self) -> &str {
        &self.wap_id
    }

    /// Publish the staging branch by fast-forwarding main to its snapshot,
    /// then deleting the branch. Consumes the publisher.
    pub async fn publish(self) -> Result<()> {
        self.writer.publish_branch(&self.wap_id).await
    }
}
