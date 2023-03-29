use crate::reward_share::GatewayDCShare;
use file_store::{file_info_poller::FileInfoStream, iot_packet::IotValidPacket};
use futures::{StreamExt, TryStreamExt};
use sqlx::PgPool;
use tokio::sync::mpsc::Receiver;

pub struct PacketLoader {
    pub pool: PgPool,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

impl PacketLoader {
    pub async fn run(
        &mut self,
        mut receiver: Receiver<FileInfoStream<IotValidPacket>>,
        shutdown: &triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("starting verifier iot packet loader");
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                msg = receiver.recv() => if let Some(stream) =  msg {
                    self.handle_packet(stream).await?;
                }
            }
        }
        tracing::info!("stopping verifier iot packet loader");
        Ok(())
    }

    async fn handle_packet(
        &self,
        file_info_stream: FileInfoStream<IotValidPacket>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(|valid_packet| GatewayDCShare::share_from_packet(&valid_packet))
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, reward_share| async move {
                reward_share.save(&mut transaction).await?;
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}
