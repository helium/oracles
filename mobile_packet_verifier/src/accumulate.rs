use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store::mobile_session::{
    DataTransferSessionIngestReport, DataTransferSessionReq, InvalidDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use helium_proto::services::poc_mobile::{
    invalid_data_transfer_ingest_report_v1::DataTransferIngestReportStatus,
    InvalidDataTransferIngestReportV1,
};
use mobile_config::{
    client::{AuthorizationClient, ClientError, GatewayClient},
    gateway_info::GatewayInfoResolver,
};
use sqlx::{Postgres, Transaction};

#[derive(thiserror::Error, Debug)]
pub enum AccumulationError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("reports stream dropped")]
    ReportsStreamDropped,
    #[error("config client error: {0}")]
    ConfigClientError(#[from] ClientError),
}

pub async fn accumulate_sessions(
    gateway_client: &GatewayClient,
    auth_client: &AuthorizationClient,
    conn: &mut Transaction<'_, Postgres>,
    invalid_data_session_report_sink: &FileSinkClient,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> Result<(), AccumulationError> {
    tokio::pin!(reports);

    while let Some(report) = reports.next().await {
        // If the reward has been cancelled or it fails verification checks then skip
        // the report and write it out to s3 as invalid
        if report.report.reward_cancelled {
            write_invalid_report(
                invalid_data_session_report_sink,
                DataTransferIngestReportStatus::Cancelled,
                report,
            )
            .await?;
            continue;
        }

        let report_validity = verify_report(gateway_client, auth_client, &report.report).await;
        if report_validity != DataTransferIngestReportStatus::Valid {
            write_invalid_report(invalid_data_session_report_sink, report_validity, report).await?;
            continue;
        }
        let event = report.report.data_transfer_usage;
        sqlx::query(
            r#"
            INSERT INTO data_transfer_sessions (pub_key, payer, uploaded_bytes, downloaded_bytes, first_timestamp, last_timestamp)
            VALUES ($1, $2, $3, $4, $5, $5)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
            "#
        )
            .bind(event.pub_key)
            .bind(event.payer)
            .bind(event.upload_bytes as i64)
            .bind(event.download_bytes as i64)
            .bind(curr_file_ts)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

async fn verify_report(
    gateway_client: &GatewayClient,
    auth_client: &AuthorizationClient,
    report: &DataTransferSessionReq,
) -> DataTransferIngestReportStatus {
    if !verify_gateway(gateway_client, &report.data_transfer_usage.pub_key).await {
        return DataTransferIngestReportStatus::InvalidGatewayKey;
    };
    if !verify_known_routing_key(auth_client, &report.pub_key).await {
        return DataTransferIngestReportStatus::InvalidRoutingKey;
    };
    DataTransferIngestReportStatus::Valid
}

async fn verify_gateway(gateway_client: &GatewayClient, public_key: &PublicKeyBinary) -> bool {
    match gateway_client.resolve_gateway_info(public_key).await {
        Ok(res) => res.is_some(),
        Err(_err) => false,
    }
}

async fn verify_known_routing_key(
    auth_client: &AuthorizationClient,
    public_key: &PublicKeyBinary,
) -> bool {
    match auth_client
        .verify_authorized_key(public_key, NetworkKeyRole::MobileRouter)
        .await
    {
        Ok(res) => res,
        Err(_err) => false,
    }
}

async fn write_invalid_report(
    invalid_data_session_report_sink: &FileSinkClient,
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
