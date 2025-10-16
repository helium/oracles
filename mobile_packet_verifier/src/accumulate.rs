use std::collections::HashMap;

use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store_helium_proto::mobile_session::{
    DataTransferSessionIngestReport, VerifiedDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_proto::services::poc_mobile::{
    verified_data_transfer_ingest_report_v1::ReportStatus, DataTransferRadioAccessTechnology,
    VerifiedDataTransferIngestReportV1,
};
use sqlx::{Postgres, Transaction};

use crate::{
    banning::BannedRadios, bytes_to_dc, event_ids, pending_burns, MobileConfigResolverExt,
};

pub async fn accumulate_sessions(
    mobile_config: &impl MobileConfigResolverExt,
    banned_radios: BannedRadios,
    txn: &mut Transaction<'_, Postgres>,
    verified_data_session_report_sink: &FileSinkClient<VerifiedDataTransferIngestReportV1>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    tokio::pin!(reports);

    let mut metrics = AccumulateMetrics::new();

    while let Some(report) = reports.next().await {
        if report.report.data_transfer_usage.radio_access_technology
        // Eutran means CBRS radio
            == DataTransferRadioAccessTechnology::Eutran
        {
            continue;
        }

        let report_validity = verify_report(txn, mobile_config, &banned_radios, &report).await?;
        write_verified_report(
            verified_data_session_report_sink,
            report_validity,
            report.clone(),
        )
        .await?;

        if report_validity != ReportStatus::Valid {
            continue;
        }

        if report.report.rewardable_bytes == 0 {
            continue;
        }

        metrics.add_report(&report);
        pending_burns::save_data_transfer_session_req(&mut *txn, &report.report, curr_file_ts)
            .await?;
    }

    metrics.flush();

    Ok(())
}

async fn verify_report(
    txn: &mut Transaction<'_, Postgres>,
    mobile_config: &impl MobileConfigResolverExt,
    banned_radios: &BannedRadios,
    report: &DataTransferSessionIngestReport,
) -> anyhow::Result<ReportStatus> {
    if is_duplicate(txn, report).await? {
        return Ok(ReportStatus::Duplicate);
    }

    let gw_pub_key = &report.report.data_transfer_usage.pub_key;
    let routing_pub_key = &report.report.pub_key;

    if banned_radios.contains(gw_pub_key) {
        return Ok(ReportStatus::Banned);
    }

    if !mobile_config.is_gateway_known(gw_pub_key).await {
        return Ok(ReportStatus::InvalidGatewayKey);
    }

    if !mobile_config.is_routing_key_known(routing_pub_key).await {
        return Ok(ReportStatus::InvalidRoutingKey);
    }

    Ok(ReportStatus::Valid)
}

async fn is_duplicate(
    txn: &mut Transaction<'_, Postgres>,
    report: &DataTransferSessionIngestReport,
) -> anyhow::Result<bool> {
    event_ids::is_duplicate(
        txn,
        report.report.data_transfer_usage.event_id.clone(),
        report.received_timestamp,
    )
    .await
}

async fn write_verified_report(
    verified_data_session_report_sink: &FileSinkClient<VerifiedDataTransferIngestReportV1>,
    status: ReportStatus,
    report: DataTransferSessionIngestReport,
) -> Result<(), file_store::Error> {
    let proto: VerifiedDataTransferIngestReportV1 = VerifiedDataTransferIngestReport {
        report,
        status,
        timestamp: Utc::now(),
    }
    .into();

    verified_data_session_report_sink
        .write(proto, &[("status", status.as_str_name())])
        .await?;
    Ok(())
}

struct AccumulateMetrics(HashMap<helium_crypto::PublicKeyBinary, u64>);

impl AccumulateMetrics {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn add_report(&mut self, report: &DataTransferSessionIngestReport) {
        *self
            .0
            .entry(report.report.data_transfer_usage.payer.clone())
            .or_default() += report.report.rewardable_bytes;
    }

    fn flush(self) {
        for (payer, rewardable_bytes) in self.0 {
            let dc_to_burn = bytes_to_dc(rewardable_bytes);
            pending_burns::increment_metric(&payer, dc_to_burn);
        }
    }
}
