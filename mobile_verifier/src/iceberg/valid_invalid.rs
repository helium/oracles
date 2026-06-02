//! Bundles a valid-table writer with its invalid-table sibling so the two
//! travel together as one handle.
//!
//! The two tables are ordinary Iceberg tables — the invalid one is defined in
//! its own `invalid_*` module, reusing the valid table's schema plus a `reason`
//! column (see [`crate::iceberg`]). This bundle only pairs the writers; it has
//! no special serialization, and each side writes a plain record struct.

use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, Catalog, IntoBoxedDataWriter, TableDefinition};
use serde::Serialize;

/// A writer pair: accepted records go to the valid table, rejected records to
/// the invalid sibling. Construct with [`ValidInvalidWriter::create`].
pub struct ValidInvalidWriter<V, I> {
    valid: BoxedDataWriter<V>,
    invalid: BoxedDataWriter<I>,
}

impl<V, I> ValidInvalidWriter<V, I>
where
    V: Serialize + Send + Sync + 'static,
    I: Serialize + Send + Sync + 'static,
{
    /// Create both tables (each only if it does not already exist) from their
    /// definitions and bundle their writers. The namespace must already exist.
    pub async fn create(
        catalog: &Catalog,
        valid_definition: TableDefinition,
        invalid_definition: TableDefinition,
    ) -> anyhow::Result<Self> {
        let valid = catalog
            .create_table_if_not_exists::<V>(valid_definition)
            .await
            .context("creating valid table")?
            .boxed();
        let invalid = catalog
            .create_table_if_not_exists::<I>(invalid_definition)
            .await
            .context("creating invalid table")?
            .boxed();

        Ok(Self { valid, invalid })
    }

    /// Idempotently append accepted records to the valid table.
    pub async fn write_valid(&self, id: &str, records: Vec<V>) -> anyhow::Result<()> {
        self.valid
            .write_idempotent(id, records)
            .await
            .context("writing valid records")
    }

    /// Idempotently append rejected records to the invalid table.
    pub async fn write_invalid(&self, id: &str, records: Vec<I>) -> anyhow::Result<()> {
        self.invalid
            .write_idempotent(id, records)
            .await
            .context("writing invalid records")
    }

    /// Idempotently append both sides, keyed by the same `id`. The two tables
    /// are independent, so the shared `id` is safe — each tracks its own
    /// idempotency. On partial failure a retry completes whichever write did
    /// not commit.
    pub async fn write(&self, id: &str, valid: Vec<V>, invalid: Vec<I>) -> anyhow::Result<()> {
        self.write_valid(id, valid).await?;
        self.write_invalid(id, invalid).await
    }
}
