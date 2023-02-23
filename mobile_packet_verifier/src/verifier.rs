use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream, mobile_session::DataTransferSessionIngestReport,
};
use futures::Stream;
use sqlx::{Pool, Postgres, Transaction};
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex};

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
    conn: &mut Transaction<Postgres>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> Result<(), VerificationError> {
    tokio::pin!(reports);

    while let Some(DataTransferSessionIngestReport { report, .. }) = reorts.next().await {
        sqlx::query(
            r#"
            INSERT INTO data_transfer_sessions (pub_key, payer, uploaded_bytes, downloaded_bytes, first_timestamp, last_timestamp)
            VALUES ($1, $2, $3, $4, $5, $5)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            last_timestamp = EXCLUDED.last_timestamp
            "#
        )
            .bind(report.pub_key)
            .bind(report.payer)
            .bind(report.uploaded_bytes)
            .bind(report.download_bytes)
            .bind(curr_file_ts)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

pub struct Verifier {
    pool: Pool<Postgres>,
    reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
    db_lock: Arc<Mutex<()>>,
}

impl Verifier {
    pub fn new(
        pool: Pool<Postgres>,
        reports: Receiver<FileInfoStream<DataTransferSessionIngestReport>>,
        db_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            pool,
            reports,
            db_lock,
        }
    }

    pub async fn run(self, shutdown: &triggered::Listener) -> Result<(), VerificationError> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                file = self.reports.recv() => {
                    if let Some(file) = file {
                        let _db_lock = self.db_lock.lock().await;
                        tracing::info!("Verifying file: {}", file.file_info);
                        let ts = file.file_info.timestamp.clone();,
                        let mut transaction = self.pool.begin().await?;
                        let reports = file.into_stream(&mut transaction).await?;
                        verify(&mut transaction, ts, reports).await?;
                        transaction.commit().await?;
                    } else {
                        return Err(VerificationError::ReportsStreamDropped);
                    }
                }
            }
        }
    }
}
