use crate::entropy::Entropy;
use blake3::hash;
use file_store::{entropy_report::EntropyReport, file_info_poller::FileInfoStream};
use futures::{StreamExt, TryStreamExt};
use sqlx::PgPool;
use tokio::sync::mpsc::Receiver;

pub struct EntropyLoader {
    pub pool: PgPool,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

impl EntropyLoader {
    pub async fn run(
        &mut self,
        mut receiver: Receiver<FileInfoStream<EntropyReport>>,
        shutdown: &triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("started verifier entropy loader");
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                msg = receiver.recv() => if let Some(stream) =  msg {
                    self.handle_report(stream).await?;
                }
            }
        }
        tracing::info!("stopping verifier entropy_loader");
        Ok(())
    }

    async fn handle_report(
        &self,
        file_info_stream: FileInfoStream<EntropyReport>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async move {
                let id = hash(&report.data).as_bytes().to_vec();
                Entropy::insert_into(
                    &mut transaction,
                    &id,
                    &report.data,
                    &report.timestamp,
                    report.version as i32,
                )
                .await?;
                metrics::increment_counter!("oracles_iot_verifier_loader_entropy");
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}
