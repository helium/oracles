//! Reads ticket files from s3, rules on each ticket, records the verdict.
//!
//! Every ticket produces a verified report — accepted or rejected — so the
//! public record shows refusals as well as grants. Both also land in the
//! append-only history table, tagged with the verdict.
//!
//! Nothing here holds mutable state: this module only appends. What is
//! *currently* in force is derived from those rows separately, by
//! [`super::inventory`].

use std::{ops::ControlFlow, time::Duration};

use chrono::Utc;
use file_store::file_info_poller::FileInfoStream;
use file_store_oracles::mobile::data_transfer_multiplier::{
    proto::VerifiedDataTransferMultiplierTicketReportV1, DataTransferMultiplier,
    DataTransferMultiplierTicketReport, VerifiedDataTransferMultiplierTicketReport,
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

use super::{TicketSigners, VerifiedTicketSink};

pub struct TicketIngestor {
    /// Only for the file poller's own "which files have I processed" bookkeeping
    /// — no ticket data is stored in Postgres.
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

        // The transaction records only that this file was processed; the
        // tickets themselves go to s3 and Iceberg.
        let mut txn = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut txn).await?;

        let mut history = Vec::new();

        while let Some(report) = stream.next().await {
            let verified = self.verify(report).await?;

            // Every ticket gets a history row, refusals included — a value the
            // column cannot hold lands as NULL, with the status saying why.
            if self.history_writer.is_some() {
                history.push(IcebergMultiplierTicket::from(&verified));
            }

            let status = verified.status.as_str_name();
            let proto = VerifiedDataTransferMultiplierTicketReportV1::from(verified);
            self.verified_sink
                .write(proto, &[("status", status)])
                .await?;
        }

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
                status = status.as_str_name(),
                "rejecting data transfer multiplier ticket"
            );
        }

        Ok(verified)
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
