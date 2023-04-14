use chrono::{DateTime, Utc};
use file_store::mobile_session::DataTransferSessionIngestReport;
use futures::{Stream, StreamExt};
use mobile_config::{
    client::{Client, ClientError},
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
    config_client: &mut Client,
    conn: &mut Transaction<'_, Postgres>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> Result<(), AccumulationError> {
    tokio::pin!(reports);

    while let Some(DataTransferSessionIngestReport { report, .. }) = reports.next().await {
        let event = report.data_transfer_usage;
        // If the reward has been cancelled or we cannot resolve this gateway, skip the
        // report
        if report.reward_cancelled
            || config_client
                .resolve_gateway_info(&event.pub_key)
                .await?
                .is_none()
        {
            continue;
        }
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
