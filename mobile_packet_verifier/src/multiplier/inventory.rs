//! Keeps `data_transfer.multiplier_ticket_inventory` up to date.
//!
//! The history table is the log; the inventory is what is currently in force.
//! This periodically merges the second out of the first, following the pattern
//! `network-dbt` uses for its `*_inventory` marts — latest-per-key by a
//! `row_number()` window, merged on the key — with the SQL issued by us rather
//! than by dbt.
//!
//! It runs as a `MERGE` through Trino rather than through the Rust iceberg
//! writer, because the writer is append-only and cannot update a row in place.
//!
//! **The burn reads this table**, so a refresh that stops running freezes the
//! multipliers burns apply — at the last merged value, not at 1. Tickets keep
//! landing in the history either way, so a resumed refresh catches up without
//! loss; the exposure is stale multipliers in the meantime, not lost grants.

use std::time::Duration;

use file_store_oracles::mobile::data_transfer_multiplier::VerifiedDataTransferMultiplierTicketStatus;
use helium_iceberg_oracles::data_transfer::{
    multiplier_ticket_history, multiplier_ticket_inventory,
};
use task_manager::Periodic;

pub struct InventoryRefresher {
    trino: trino_client::Client,
    interval: Duration,
    history_table: String,
    inventory_table: String,
}

impl InventoryRefresher {
    pub fn new(trino: trino_client::Client, interval: Duration) -> Self {
        Self::new_with_tables(
            trino,
            interval,
            format!(
                "{}.{}",
                multiplier_ticket_history::NAMESPACE,
                multiplier_ticket_history::TABLE_NAME
            ),
            format!(
                "{}.{}",
                multiplier_ticket_inventory::NAMESPACE,
                multiplier_ticket_inventory::TABLE_NAME
            ),
        )
    }

    /// Like [`new`](Self::new), with explicit table names so tests can point at
    /// a per-test catalog.
    pub fn new_with_tables(
        trino: trino_client::Client,
        interval: Duration,
        history_table: String,
        inventory_table: String,
    ) -> Self {
        Self {
            trino,
            interval,
            history_table,
            inventory_table,
        }
    }

    /// Run one refresh. Public so a test can drive it without a scheduler.
    pub async fn refresh(&self) -> anyhow::Result<()> {
        let sql = multiplier_ticket_inventory::merge_statement(
            &self.history_table,
            &self.inventory_table,
            VerifiedDataTransferMultiplierTicketStatus::Valid.as_str_name(),
        );

        self.trino.execute_raw(sql).await?;
        Ok(())
    }
}

impl Periodic for InventoryRefresher {
    type Error = anyhow::Error;

    fn interval(&self) -> Duration {
        self.interval
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        self.refresh().await?;
        tracing::info!("refreshed data transfer multiplier inventory");
        Ok(())
    }
}
