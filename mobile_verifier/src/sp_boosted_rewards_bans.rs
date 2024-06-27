use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_source,
    sp_boosted_rewards_bans::{KeyType, ServiceProviderBoostedRewardBannedRadioIngest},
    FileStore, FileType,
};
use futures::{prelude::future::LocalBoxFuture, StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::{PgPool, Postgres, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

pub struct ServiceProviderBoostedRewardsBanIngestor<AV> {
    pool: PgPool,
    authorization_verifier: AV,
    receiver: Receiver<FileInfoStream<ServiceProviderBoostedRewardBannedRadioIngest>>,
}

impl<AV> ManagedTask for ServiceProviderBoostedRewardsBanIngestor<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    AV::Error: std::error::Error + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<AV> ServiceProviderBoostedRewardsBanIngestor<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
    AV::Error: std::error::Error + Send + Sync + 'static,
{
    pub async fn create_managed_task(
        pool: PgPool,
        file_store: FileStore,
        authorization_verifier: AV,
        start_after: DateTime<Utc>,
    ) -> anyhow::Result<impl ManagedTask> {
        let (receiver, ingest_server) =
            file_source::continuous_source::<ServiceProviderBoostedRewardBannedRadioIngest, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(start_after))
                .prefix(FileType::ServiceProviderBoostedRewardsBannedRadioIngestReport.to_string())
                .create()
                .await?;

        let ingestor = Self {
            pool,
            authorization_verifier,
            receiver,
        };

        Ok(TaskManager::builder()
            .add_task(ingest_server)
            .add_task(ingestor)
            .build())
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("service provider boosted rewards ban ingestor starting");
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                Some(file) = self.receiver.recv() => {
                    self.process_file(file).await?;
                }
            }
        }
        tracing::info!("stopping service provider boosted rewards ban ingestor");

        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<ServiceProviderBoostedRewardBannedRadioIngest>,
    ) -> anyhow::Result<()> {
        tracing::info!(file = %file_info_stream.file_info.key, "processing sp boosted rewards ban file");
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut tx, ingest| async move {
                if self.is_authorized(&ingest.report.pubkey).await? {
                    save(&mut tx, ingest).await?;
                }

                Ok(tx)
            })
            .await?
            .commit()
            .await?;

        Ok(())
    }

    async fn is_authorized(&self, pubkey: &PublicKeyBinary) -> anyhow::Result<bool> {
        self.authorization_verifier
            .verify_authorized_key(pubkey, NetworkKeyRole::MobileCarrier)
            .await
            .map_err(anyhow::Error::from)
    }
}

async fn save(
    transaction: &mut Transaction<'_, Postgres>,
    ingest: ServiceProviderBoostedRewardBannedRadioIngest,
) -> anyhow::Result<()> {
    let (radio_type, radio_key) = match ingest.report.key_type {
        KeyType::CbsdId(cbsd_id) => ("cbrs", cbsd_id),
        KeyType::HotspotKey(pubkey) => ("wifi", pubkey.to_string()),
    };

    sqlx::query(
        r#"
        INSERT INTO sp_boosted_rewards_bans(radio_type, radio_key, received_timestamp, until)
        VALUES($1,$2,$3,$4)
    "#,
    )
    .bind(radio_type)
    .bind(radio_key)
    .bind(ingest.received_timestamp)
    .bind(ingest.report.until)
    .execute(transaction)
    .await
    .map(|_| ())
    .map_err(anyhow::Error::from)
}
