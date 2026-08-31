//! HIP-150 data transfer multiplier tickets.
//!
//! A ticket grants one hotspot a multiplier on the data credits its rewardable
//! bytes convert to. Ingest checks who sent it and when, then writes it to s3
//! unchanged. This module reads those files and rules on each ticket.
//!
//! Validity is decided here rather than at ingest so that refusals leave a
//! record. HIP-150 wants every multiplier in force to be auditable, and a ticket
//! turned away at the gRPC boundary leaves nothing behind.
//!
//! A ruled-on ticket goes to two places:
//!
//! * `data_transfer.multiplier_ticket_history` in Iceberg, plus an s3 report.
//!   Every ticket, refusals included. This is the audit record.
//! * `data_transfer_multipliers` in Postgres, grants only. This is what the burn
//!   joins against, and it is in Postgres because `data_transfer_sessions` is.
//!   See [`db`].

use std::time::Duration;

use chrono::{DateTime, Utc};
use file_store::{file_sink::FileSinkClient, file_upload::FileUpload};
use file_store_oracles::{
    mobile::data_transfer_multiplier::proto::VerifiedDataTransferMultiplierTicketReportV1,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileType,
};
use helium_crypto::PublicKeyBinary;
use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use task_manager::{ManagedTask, TaskManager};

use crate::gateway::GatewayResolver;

pub mod db;
pub mod ingestor;

#[derive(Debug, Deserialize, Serialize)]
pub struct MultiplierSettings {
    /// Where to look in s3 for data transfer multiplier ticket files.
    pub input_bucket: file_store::BucketSettings,
    /// How far back to read ticket files on a cold start.
    #[serde(default = "default_ingest_start_after")]
    pub start_after: DateTime<Utc>,
    /// Public keys authorized to issue tickets, comma-separated b58.
    ///
    /// Checked again here even though ingest already checked it. The two are
    /// deployed and configured separately, and it is this verdict that lands on
    /// the record.
    ///
    /// Empty by default, which rejects every ticket. Nothing can be granted
    /// until a signer is provisioned.
    #[serde(default)]
    pub authorized_keys: String,
    /// How old a ticket's signed timestamp may be, measured against the time
    /// ingest stamped on it, before it is refused.
    ///
    /// Configured separately from ingest's own window so that a mistake there
    /// does not widen the replay window everywhere. It is not an independent
    /// defence: both timestamps come from the same file, so it does not help
    /// against anyone who can write that file.
    #[serde(with = "humantime_serde", default = "default_ticket_max_age")]
    pub ticket_max_age: Duration,
}

fn default_ingest_start_after() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
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

pub async fn create_managed_task(
    pool: PgPool,
    file_upload: FileUpload,
    settings: &MultiplierSettings,
    signers: TicketSigners,
    resolver: GatewayResolver,
    store_base_path: &std::path::Path,
    history_writer: Option<crate::iceberg::MultiplierTicketWriter>,
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

    Ok(TaskManager::builder()
        .add_task(report_server)
        .add_task(verified_sink_server)
        .add_task(task_manager::channel_consumer(ingestor))
        .build())
}

/// Type alias so the sink type is spelled once.
pub type VerifiedTicketSink = FileSinkClient<VerifiedDataTransferMultiplierTicketReportV1>;
