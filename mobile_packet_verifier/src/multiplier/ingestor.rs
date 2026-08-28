//! Reads ticket files from s3, decides whether each ticket is valid, and
//! records the result.
//!
//! Every ticket produces a verified report, accepted or rejected, so refusals
//! are on the record too. Every ticket also gets a row in the Iceberg history
//! table, tagged with the verdict.
//!
//! Accepted tickets also go to Postgres, via [`super::db`], which is what the
//! burn joins against. Rejected ones do not, because a refusal grants nothing.
//!
//! Nothing here holds state between files. It reads, decides, and appends.

use std::{ops::ControlFlow, time::Duration};

use chrono::Utc;
use file_store::file_info_poller::FileInfoStream;
use file_store_oracles::mobile::data_transfer_multiplier::{
    proto::VerifiedDataTransferMultiplierTicketReportV1, ticket_status_string,
    DataTransferMultiplier, DataTransferMultiplierTicketReport,
    VerifiedDataTransferMultiplierTicketReport,
    VerifiedDataTransferMultiplierTicketStatus as Status, MAX_CLOCK_DRIFT,
};
use futures::StreamExt;
use sqlx::PgPool;
use task_manager::ChannelConsumer;
use tokio::sync::mpsc::Receiver;

use crate::{
    gateway::GatewayResolver,
    iceberg::{IcebergMultiplierTicket, MultiplierTicketWriter},
};

use super::{
    db::{self, GrantedMultiplier},
    TicketSigners, VerifiedTicketSink,
};

pub struct TicketIngestor {
    /// Holds both the file poller's bookkeeping and the granted multipliers.
    /// They are written in one transaction, so a file is never marked processed
    /// without its grants.
    pool: PgPool,
    report_rx: Receiver<FileInfoStream<DataTransferMultiplierTicketReport>>,
    verified_sink: VerifiedTicketSink,
    signers: TicketSigners,
    resolver: GatewayResolver,
    ticket_max_age: Duration,
    history_writer: Option<MultiplierTicketWriter>,
}

impl ChannelConsumer for TicketIngestor {
    type Item = FileInfoStream<DataTransferMultiplierTicketReport>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.report_rx.recv().await
    }

    async fn handle(&mut self, file_info_stream: Self::Item) -> anyhow::Result<()> {
        self.process_file(file_info_stream).await
    }

    async fn on_receiver_closed(&mut self) -> anyhow::Result<ControlFlow<()>> {
        Err(anyhow::anyhow!(
            "data transfer multiplier ticket FileInfoPoller sender was dropped unexpectedly"
        ))
    }
}

impl TicketIngestor {
    pub fn new(
        pool: PgPool,
        report_rx: Receiver<FileInfoStream<DataTransferMultiplierTicketReport>>,
        verified_sink: VerifiedTicketSink,
        signers: TicketSigners,
        resolver: GatewayResolver,
        ticket_max_age: Duration,
        history_writer: Option<MultiplierTicketWriter>,
    ) -> Self {
        Self {
            pool,
            report_rx,
            verified_sink,
            signers,
            resolver,
            ticket_max_age,
            history_writer,
        }
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<DataTransferMultiplierTicketReport>,
    ) -> anyhow::Result<()> {
        let file = file_info_stream.file_info.key.clone();
        tracing::info!(%file, "processing data transfer multiplier tickets");

        // One transaction covers both "this file was processed" and the grants
        // it produced, so a crash cannot leave the first without the second.
        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        let mut history = Vec::new();
        let mut granted = Vec::new();

        while let Some(report) = stream.next().await {
            let verified = self.verify(report).await?;

            // Refusals get a history row too. A multiplier the column cannot
            // hold is stored as NULL, and the status says why.
            if self.history_writer.is_some() {
                history.push(IcebergMultiplierTicket::from(&verified));
            }

            if let Some(grant) = granted_multiplier(&verified) {
                granted.push(grant);
            }

            let status = ticket_status_string(verified.status);
            let proto = VerifiedDataTransferMultiplierTicketReportV1::from(verified);
            self.verified_sink
                .write(proto, &[("status", status)])
                .await?;
        }

        db::save(&mut txn, &granted).await?;

        // Keyed on the file, so reprocessing one cannot duplicate its rows.
        if let Some(writer) = self.history_writer.as_ref() {
            writer.write_idempotent(&file, history).await?;
        }

        txn.commit().await?;
        self.verified_sink.commit().await?;

        Ok(())
    }

    /// Rule on one ticket.
    async fn verify(
        &self,
        report: DataTransferMultiplierTicketReport,
    ) -> anyhow::Result<VerifiedDataTransferMultiplierTicketReport> {
        let status =
            ticket_status(&report, &self.signers, self.ticket_max_age, &self.resolver).await;

        let verified = VerifiedDataTransferMultiplierTicketReport {
            verified_timestamp: Utc::now(),
            report,
            status,
        };

        if !verified.is_valid() {
            tracing::warn!(
                hotspot_pubkey = %verified.hotspot_pubkey(),
                status = ticket_status_string(status),
                "rejecting data transfer multiplier ticket"
            );
        }

        Ok(verified)
    }
}

/// The grant a verified ticket makes, or `None` if it makes none.
///
/// Only tickets that passed [`ticket_status`] grant anything. That check is what
/// proves the multiplier is present and in range, so this is the one place the
/// `Option` is unwrapped without looking again.
///
/// A valid ticket whose multiplier will not convert is dropped rather than
/// guessed at. That needs `ticket_status` and `DataTransferMultiplier` to
/// disagree, which they should not, so it is logged as an error.
pub fn granted_multiplier(
    verified: &VerifiedDataTransferMultiplierTicketReport,
) -> Option<GrantedMultiplier> {
    if !verified.is_valid() {
        return None;
    }

    let ticket = &verified.report.report;
    match ticket.multiplier.map(DataTransferMultiplier::new) {
        Some(Ok(multiplier)) => Some(GrantedMultiplier {
            hotspot_pubkey: ticket.hotspot_pubkey.clone(),
            multiplier,
            effective_timestamp: ticket.timestamp,
        }),
        _ => {
            tracing::error!(
                hotspot_pubkey = %ticket.hotspot_pubkey,
                "ticket passed verification but its multiplier will not convert"
            );
            None
        }
    }
}

/// The verdict on one ticket.
///
/// A free function rather than a method: this is the rule that decides whether a
/// hotspot's rewards get multiplied, and it should be testable without a
/// channel, a file poller or a sink.
///
/// HIP-150 fixes the accepted range at 1 to 5 inclusive, "enforced by the
/// oracles" — and this is that enforcement. It is deliberately here rather than
/// at ingest: ingest keeps the range out of the wire format so policy can move
/// without a schema change, and refusing at the gRPC boundary would leave no
/// record. Refusing here writes a verified report and a history row, so a
/// rejected grant is as auditable as an accepted one.
pub async fn ticket_status(
    report: &DataTransferMultiplierTicketReport,
    signers: &TicketSigners,
    ticket_max_age: Duration,
    resolver: &GatewayResolver,
) -> Status {
    let ticket = &report.report;

    if !signers.contains(&ticket.signer_pubkey) {
        return Status::InvalidSigner;
    }

    // Absent, unparseable, out of range, or carrying more precision than we
    // store. All four mean the same thing to a submitter: not a multiplier we
    // will grant.
    match ticket.multiplier {
        Some(multiplier) if DataTransferMultiplier::new(multiplier).is_ok() => {}
        _ => return Status::InvalidMultiplier,
    }

    // A signature never expires, so a ticket is only as trustworthy as it is
    // fresh. Measured against when *ingest* received it, not against now: this
    // service may be replaying a backlog of files hours old, and every ticket in
    // them would otherwise look stale.
    //
    // A ticket can be stamped slightly ahead of the timestamp ingest gave it,
    // because the client's clock is not ingest's. Ingest accepts that drift, so
    // this must too — otherwise every ticket ingest let through from a fast
    // client would be refused here, and the two would disagree about the same
    // ticket. Hence the shared constant rather than two settings.
    let age = report.received_timestamp - ticket.timestamp;
    let age = if age < chrono::TimeDelta::zero() {
        match (-age).to_std() {
            Ok(drift) if drift <= MAX_CLOCK_DRIFT => std::time::Duration::ZERO,
            _ => return Status::InvalidTimestamp,
        }
    } else {
        match age.to_std() {
            Ok(age) => age,
            Err(_) => return Status::InvalidTimestamp,
        }
    };

    if age > ticket_max_age {
        return Status::InvalidTimestamp;
    }

    if !resolver
        .is_gateway_known(&ticket.hotspot_pubkey, &report.received_timestamp)
        .await
    {
        return Status::InvalidHotspotKey;
    }

    Status::Valid
}
