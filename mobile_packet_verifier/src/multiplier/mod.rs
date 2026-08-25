//! HIP-150 data transfer multiplier tickets.
//!
//! A ticket grants one on-chain hotspot a multiplier on the data credits derived
//! from its rewardable bytes. Ingest verifies who sent a ticket and when, then
//! writes it to s3 verbatim; this module reads those files, rules on each
//! ticket, and keeps the result.
//!
//! Deciding validity *here* rather than at ingest is deliberate. HIP-150 wants
//! every multiplier in force to be externally auditable, and a ticket rejected
//! at the gRPC boundary leaves no record — rejected here, it is written to a
//! verified report alongside the accepted ones.
//!
//! Tickets are not stored in Postgres. A verified ticket is written to an s3
//! report and appended to `data_transfer.multiplier_ticket_history`.
//!
//! Current state lives in `data_transfer.multiplier_ticket_inventory`, which
//! [`inventory`] keeps merged out of that history on a schedule. It follows the
//! shape `network-dbt` uses for `enabled_carriers_inventory` over
//! `enabled_carriers_history`, but the SQL is ours and the job runs here.
//!
//! The burn will read the inventory — whatever is current when a session
//! arrives, not what was in force when the data moved; see [`trino`]. Nothing
//! reads either table yet; no multiplier is applied until the burn is wired up.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use file_store::{file_sink::FileSinkClient, file_upload::FileUpload};
use file_store_oracles::{
    mobile::data_transfer_multiplier::{
        proto::VerifiedDataTransferMultiplierTicketReportV1, DataTransferMultiplier,
    },
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};
use helium_crypto::PublicKeyBinary;
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};

use crate::gateway::GatewayResolver;

pub mod ingestor;
pub mod inventory;
pub mod trino;

pub use trino::get_multipliers;

#[derive(Debug, Deserialize, Serialize)]
pub struct MultiplierSettings {
    /// Where to look in s3 for data transfer multiplier ticket files.
    pub input_bucket: file_store::BucketSettings,
    /// How far back to read ticket files on a cold start.
    #[serde(default = "default_ingest_start_after")]
    pub start_after: DateTime<Utc>,
    /// Public keys authorized to issue tickets, comma-separated b58.
    ///
    /// Checked again here even though ingest already checked it: ingest and this
    /// verifier are separately deployed and separately configured, and it is
    /// this verdict that lands on the record.
    ///
    /// May be empty, and is by default — no ticket can be issued until a key is
    /// provisioned. Empty rejects every ticket.
    #[serde(default)]
    pub authorized_keys: String,
    /// How often the inventory table is merged out of the history.
    ///
    /// This is burn-visible: a ticket takes effect once the next merge has run,
    /// so this interval is the lag between verifying a grant and it being worth
    /// anything. It also bounds how stale a burn's multipliers can be if the
    /// merge starts failing.
    #[serde(
        with = "humantime_serde",
        default = "default_inventory_refresh_interval"
    )]
    pub inventory_refresh_interval: Duration,
    /// How old a ticket's signed timestamp may be, measured against the time
    /// ingest stamped on it, before it is refused.
    ///
    /// A second check of what ingest already checked, configured separately, so
    /// a mistake in ingest's window does not silently widen the replay window
    /// everywhere. It is *not* an independent defence: both timestamps in the
    /// comparison come from the same file, so it cannot help against anyone able
    /// to write that file.
    #[serde(with = "humantime_serde", default = "default_ticket_max_age")]
    pub ticket_max_age: Duration,
}

fn default_ingest_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

fn default_inventory_refresh_interval() -> Duration {
    humantime::parse_duration("15 minutes").unwrap()
}

fn default_ticket_max_age() -> Duration {
    humantime::parse_duration("10 minutes").unwrap()
}

/// The set of keys allowed to issue tickets.
#[derive(Debug, Clone, Default)]
pub struct TicketSigners(std::collections::HashSet<PublicKeyBinary>);

impl TicketSigners {
    pub fn contains(&self, signer: &PublicKeyBinary) -> bool {
        self.0.contains(signer)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<PublicKeyBinary> for TicketSigners {
    fn from_iter<I: IntoIterator<Item = PublicKeyBinary>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// The multiplier in force per hotspot at a point in time.
///
/// Only ticketed hotspots appear. [`Multipliers::get`] is the single place the
/// "no ticket means 1" rule is written down — every other caller just asks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Multipliers(HashMap<PublicKeyBinary, DataTransferMultiplier>);

impl Multipliers {
    pub fn get(&self, hotspot_pubkey: &PublicKeyBinary) -> DataTransferMultiplier {
        self.0
            .get(hotspot_pubkey)
            .copied()
            .unwrap_or(DataTransferMultiplier::DEFAULT)
    }

    pub fn insert(&mut self, hotspot_pubkey: PublicKeyBinary, multiplier: DataTransferMultiplier) {
        self.0.insert(hotspot_pubkey, multiplier);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<(PublicKeyBinary, DataTransferMultiplier)> for Multipliers {
    fn from_iter<I: IntoIterator<Item = (PublicKeyBinary, DataTransferMultiplier)>>(
        iter: I,
    ) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn create_managed_task(
    pool: PgPool,
    file_upload: FileUpload,
    settings: &MultiplierSettings,
    signers: TicketSigners,
    resolver: GatewayResolver,
    store_base_path: &std::path::Path,
    history_writer: Option<crate::iceberg::MultiplierTicketWriter>,
    trino: trino_client::Client,
) -> anyhow::Result<impl ManagedTask> {
    if signers.is_empty() {
        tracing::warn!(
            "no data transfer multiplier ticket signers configured; all tickets will be rejected"
        );
    }

    let (verified_sink, verified_sink_server) =
        VerifiedDataTransferMultiplierTicketReportV1::file_sink(
            store_base_path,
            file_upload,
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (report_rx, report_server) = file_store::file_source::continuous_source()
        .state(pool.clone())
        .bucket_client(settings.input_bucket.connect().await)
        .lookback_start_after(settings.start_after)
        .prefix(FileType::DataTransferMultiplierTicketIngestReport.to_string())
        .create()
        .await?;

    let ingestor = ingestor::TicketIngestor::new(
        pool,
        report_rx,
        verified_sink,
        signers,
        resolver,
        settings.ticket_max_age,
        history_writer,
    );

    let inventory = inventory::InventoryRefresher::new(trino, settings.inventory_refresh_interval);

    Ok(TaskManager::builder()
        .add_task(report_server)
        .add_task(verified_sink_server)
        .add_task(task_manager::channel_consumer(ingestor))
        .add_task(task_manager::periodic(inventory))
        .build())
}

/// Type alias so the sink type is spelled once.
pub type VerifiedTicketSink = FileSinkClient<VerifiedDataTransferMultiplierTicketReportV1>;

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> PublicKeyBinary {
        PublicKeyBinary::from(vec![byte])
    }

    #[test]
    fn unknown_hotspot_gets_the_default_multiplier() {
        let multipliers = Multipliers::default();
        assert_eq!(
            multipliers.get(&key(1)),
            DataTransferMultiplier::DEFAULT,
            "a hotspot with no ticket must be unmultiplied"
        );
    }

    #[test]
    fn ticketed_hotspot_gets_its_multiplier() {
        let one_and_a_half = DataTransferMultiplier::new(rust_decimal::dec!(1.5)).expect("valid");
        let multipliers = Multipliers::from_iter([(key(1), one_and_a_half)]);

        assert_eq!(multipliers.get(&key(1)), one_and_a_half);
        // ...and its neighbour is unaffected.
        assert_eq!(multipliers.get(&key(2)), DataTransferMultiplier::DEFAULT);
    }
}
