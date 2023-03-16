use chrono::{DateTime, Utc};
use file_store::mobile_session::DataTransferSessionIngestReport;
use futures::{Stream, StreamExt};
use sqlx::{Postgres, Transaction};

#[derive(thiserror::Error, Debug)]
pub enum VerificationError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("reports stream dropped")]
    ReportsStreamDropped,
}

pub async fn verify(
    conn: &mut Transaction<'_, Postgres>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> Result<(), VerificationError> {
    tokio::pin!(reports);

    while let Some(DataTransferSessionIngestReport { report, .. }) = reports.next().await {
        sqlx::query(
            r#"
            INSERT INTO data_transfer_sessions (pub_key, payer, uploaded_bytes, downloaded_bytes, first_timestamp, last_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            last_timestamp = EXCLUDED.last_timestamp
            "#
        )
            .bind(report.pub_key)
            .bind(report.payer)
            .bind(report.upload_bytes as i64)
            .bind(report.download_bytes as i64)
            .bind(curr_file_ts)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}
