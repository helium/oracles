use crate::Result;
use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;

pub type BoxedDataWriter<T> = std::sync::Arc<dyn DataWriter<T>>;

#[async_trait]
pub trait DataWriter<T>: Send + Sync
where
    T: Serialize + Send,
{
    async fn write(&self, records: Vec<T>) -> Result;
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
    async fn stage(
        &self,
        wap_id: &str,
        records: Vec<T>,
        custom_properties: HashMap<String, String>,
    ) -> Result<WriteOutcome>;

    /// Publish a staged WAP branch by fast-forwarding main.
    async fn publish(&self, wap_id: &str) -> Result;

    /// Returns the `additional_properties` from the main branch's latest snapshot summary.
    /// Returns an empty map if no snapshot exists yet (fresh table).
    async fn snapshot_properties(&self) -> Result<HashMap<String, String>>;
}

pub trait IntoBoxedDataWriter<T> {
    fn boxed(self) -> BoxedDataWriter<T>;
}

impl<Msg, T> IntoBoxedDataWriter<Msg> for T
where
    T: DataWriter<Msg> + 'static,
    Msg: Serialize + Send,
{
    fn boxed(self) -> BoxedDataWriter<Msg> {
        std::sync::Arc::new(self)
    }
}
