use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store::mobile_session::{
    DataTransferSessionIngestReport, InvalidDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use helium_proto::services::poc_mobile::{
    invalid_data_transfer_ingest_report_v1::DataTransferIngestReportStatus,
    InvalidDataTransferIngestReportV1,
};
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, gateway_client::GatewayInfoResolver,
};
use sqlx::{Postgres, Transaction};

use crate::event_ids;

pub async fn accumulate_sessions(
    gateway_info_resolver: &impl GatewayInfoResolver,
    authorization_verifier: &impl AuthorizationVerifier,
    conn: &mut Transaction<'_, Postgres>,
    invalid_data_session_report_sink: &FileSinkClient<InvalidDataTransferIngestReportV1>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    tokio::pin!(reports);

    while let Some(report) = reports.next().await {
        // If the reward has been cancelled or it fails verification checks then skip
        // the report and write it out to s3 as invalid
        if report.report.rewardable_bytes == 0 {
            write_invalid_report(
                invalid_data_session_report_sink,
                DataTransferIngestReportStatus::Cancelled,
                report,
            )
            .await?;
            continue;
        }

        let report_validity =
            verify_report(conn, gateway_info_resolver, authorization_verifier, &report).await?;
        if report_validity != DataTransferIngestReportStatus::Valid {
            write_invalid_report(invalid_data_session_report_sink, report_validity, report).await?;
            continue;
        }
        let event = report.report.data_transfer_usage;
        sqlx::query(
            r#"
            INSERT INTO data_transfer_sessions (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $6)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
            last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
            "#
        )
            .bind(event.pub_key)
            .bind(event.payer)
            .bind(event.upload_bytes as i64)
            .bind(event.download_bytes as i64)
            .bind(report.report.rewardable_bytes as i64)
            .bind(curr_file_ts)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

async fn verify_report(
    txn: &mut Transaction<'_, Postgres>,
    gateway_info_resolver: &impl GatewayInfoResolver,
    authorization_verifier: &impl AuthorizationVerifier,
    report: &DataTransferSessionIngestReport,
) -> anyhow::Result<DataTransferIngestReportStatus> {
    if is_duplicate(txn, report).await? {
        return Ok(DataTransferIngestReportStatus::Duplicate);
    }

    if !verify_gateway(
        gateway_info_resolver,
        &report.report.data_transfer_usage.pub_key,
    )
    .await
    {
        return Ok(DataTransferIngestReportStatus::InvalidGatewayKey);
    };
    if !verify_known_routing_key(authorization_verifier, &report.report.pub_key).await {
        return Ok(DataTransferIngestReportStatus::InvalidRoutingKey);
    };
    Ok(DataTransferIngestReportStatus::Valid)
}

async fn verify_gateway(
    gateway_info_resolver: &impl GatewayInfoResolver,
    public_key: &PublicKeyBinary,
) -> bool {
    match gateway_info_resolver.resolve_gateway_info(public_key).await {
        Ok(res) => res.is_some(),
        Err(_err) => false,
    }
}

async fn verify_known_routing_key(
    authorization_verifier: &impl AuthorizationVerifier,
    public_key: &PublicKeyBinary,
) -> bool {
    match authorization_verifier
        .verify_authorized_key(public_key, NetworkKeyRole::MobileRouter)
        .await
    {
        Ok(res) => res,
        Err(_err) => false,
    }
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

async fn write_invalid_report(
    invalid_data_session_report_sink: &FileSinkClient<InvalidDataTransferIngestReportV1>,
    reason: DataTransferIngestReportStatus,
    report: DataTransferSessionIngestReport,
) -> Result<(), file_store::Error> {
    let proto: InvalidDataTransferIngestReportV1 = InvalidDataTransferIngestReport {
        report,
        reason,
        timestamp: Utc::now(),
    }
    .into();

    invalid_data_session_report_sink
        .write(proto, &[("reason", reason.as_str_name())])
        .await?;
    Ok(())
}
