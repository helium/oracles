use std::collections::HashMap;

use chrono::{DateTime, Utc};
use file_store_oracles::mobile_session::{
    DataTransferSessionIngestReport, VerifiedDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_proto::services::poc_mobile::{
    verified_data_transfer_ingest_report_v1::ReportStatus, DataTransferRadioAccessTechnology,
    VerifiedDataTransferIngestReportV1,
};
use sqlx::{Postgres, Transaction};

use crate::{
    banning::BannedRadios,
    bytes_to_dc, event_ids,
    iceberg::session::IcebergDataTransferSession,
    pending_burns::{self, DataTransferSession},
    MobileConfigResolverExt,
};

#[derive(Default)]
pub struct AccumulatedSessions {
    pub iceberg_sessions: Vec<IcebergDataTransferSession>,
    pub proto_sessions: Vec<VerifiedDataTransferIngestReportV1>,
    pub db_sessions: Vec<DataTransferSession>,
}

pub async fn accumulate_sessions(
    mobile_config: &impl MobileConfigResolverExt,
    banned_radios: BannedRadios,
    txn: &mut Transaction<'_, Postgres>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
    curr_file_ts: DateTime<Utc>,
) -> anyhow::Result<AccumulatedSessions> {
    tokio::pin!(reports);

    let mut metrics = AccumulateMetrics::new();

    let mut result = AccumulatedSessions::default();

    while let Some(report) = reports.next().await {
        if report.is_cbrs() {
            continue;
        }

        let report_validity = report
            .report_status(txn, mobile_config, &banned_radios)
            .await?;
        result
            .proto_sessions
            .push(report.to_verified_proto(report_validity));

        // go to iceberg only if it's valid, even if it's zero rewardable bytes
        if report_validity == ReportStatus::Valid {
            result.iceberg_sessions.push(report.to_iceberg_session());
        }

        if report_validity != ReportStatus::Valid {
            continue;
        }

        if report.no_rewardable_bytes() {
            continue;
        }

        metrics.add_report(&report);
        result
            .db_sessions
            .push(report.to_data_transfer_session(curr_file_ts));
    }

    metrics.flush();

    Ok(result)
}

trait DataTransferIngestReportExt {
    fn to_verified_proto(&self, status: ReportStatus) -> VerifiedDataTransferIngestReportV1;

    fn to_data_transfer_session(&self, file_ts: DateTime<Utc>) -> DataTransferSession;

    fn to_iceberg_session(&self) -> IcebergDataTransferSession;

    fn no_rewardable_bytes(&self) -> bool;

    fn is_cbrs(&self) -> bool;

    async fn report_status(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        mobile_config: &impl MobileConfigResolverExt,
        banned_radios: &BannedRadios,
    ) -> anyhow::Result<ReportStatus>;

    async fn is_duplicate(&self, txn: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool>;
}

impl DataTransferIngestReportExt for DataTransferSessionIngestReport {
    fn to_verified_proto(&self, status: ReportStatus) -> VerifiedDataTransferIngestReportV1 {
        VerifiedDataTransferIngestReport {
            report: self.clone(),
            status,
            timestamp: Utc::now(),
        }
        .into()
    }

    fn to_data_transfer_session(&self, file_ts: DateTime<Utc>) -> DataTransferSession {
        DataTransferSession::from_req(&self.report, file_ts)
    }

    fn to_iceberg_session(&self) -> IcebergDataTransferSession {
        IcebergDataTransferSession::from(self.clone())
    }

    fn no_rewardable_bytes(&self) -> bool {
        self.report.rewardable_bytes == 0
    }

    fn is_cbrs(&self) -> bool {
        // Eutran means CBRS radio
        matches!(
            self.report.data_transfer_usage.radio_access_technology,
            DataTransferRadioAccessTechnology::Eutran
        )
    }

    async fn report_status(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        mobile_config: &impl MobileConfigResolverExt,
        banned_radios: &BannedRadios,
    ) -> anyhow::Result<ReportStatus> {
        if self.is_duplicate(txn).await? {
            return Ok(ReportStatus::Duplicate);
        }

        let gw_pub_key = &self.report.data_transfer_usage.pub_key;
        let routing_pub_key = &self.report.pub_key;

        if banned_radios.contains(gw_pub_key) {
            return Ok(ReportStatus::Banned);
        }

        if !mobile_config
            .is_gateway_known(gw_pub_key, &self.received_timestamp)
            .await
        {
            return Ok(ReportStatus::InvalidGatewayKey);
        }

        if !mobile_config.is_routing_key_known(routing_pub_key).await {
            return Ok(ReportStatus::InvalidRoutingKey);
        }

        Ok(ReportStatus::Valid)
    }

    async fn is_duplicate(&self, txn: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        event_ids::is_duplicate(
            txn,
            &self.report.data_transfer_usage.event_id,
            self.received_timestamp,
        )
        .await
    }
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
