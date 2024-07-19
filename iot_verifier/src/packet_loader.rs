use crate::{
    gateway_cache::GatewayCache, reward_share::GatewayDCShare, telemetry::LoaderMetricTracker,
    Settings,
};
use chrono::Utc;
use file_store::{file_info_poller::FileInfoStream, file_sink, iot_packet::IotValidPacket};
use futures::{future::LocalBoxFuture, StreamExt, TryStreamExt};
use helium_proto::services::packet_verifier::ValidPacket;
use helium_proto::services::poc_lora::{NonRewardablePacket, NonRewardablePacketReason};
use sqlx::PgPool;
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

pub struct PacketLoader {
    pub pool: PgPool,
    pub cache: String,
    gateway_cache: GatewayCache,
    file_receiver: Receiver<FileInfoStream<IotValidPacket>>,
    file_sink: file_sink::FileSinkClient<NonRewardablePacket>,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

impl ManagedTask for PacketLoader {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl PacketLoader {
    pub fn from_settings(
        settings: &Settings,
        pool: PgPool,
        gateway_cache: GatewayCache,
        file_receiver: Receiver<FileInfoStream<IotValidPacket>>,
        file_sink: file_sink::FileSinkClient<NonRewardablePacket>,
    ) -> Self {
        tracing::info!("from_settings packet loader");
        let cache = settings.cache.clone();
        Self {
            pool,
            cache,
            gateway_cache,
            file_receiver,
            file_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting packet loader");

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                msg = self.file_receiver.recv() => if let Some(stream) =  msg {
                    let metrics = LoaderMetricTracker::new();
                    match self.handle_packet_file(stream, &metrics).await {
                        Ok(()) => {
                            metrics.record_metrics();
                            self.file_sink.commit().await?;

                        },
                        Err(err) => { return Err(err)}
                    }
                }
            }
        }
        tracing::info!("stopping packet loader");
        Ok(())
    }

    async fn handle_packet_file(
        &self,
        file_info_stream: FileInfoStream<IotValidPacket>,
        metrics: &LoaderMetricTracker,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;

        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .filter_map(|valid_packet| async move {
                if valid_packet.num_dcs > 0 {
                    Some((
                        ValidPacket::from(valid_packet.clone()),
                        GatewayDCShare::share_from_packet(&valid_packet),
                    ))
                } else {
                    None
                }
            })
            .map(anyhow::Ok)
            .try_fold(
                transaction,
                |mut transaction, (valid_packet, reward_share)| async move {
                    if self
                        .gateway_cache
                        .resolve_gateway_info(&reward_share.hotspot_key)
                        .await
                        .is_ok()
                    {
                        reward_share.save(&mut transaction).await?;
                        metrics.increment_packets();
                    } else {
                        // the gateway doesnt exist, dont reward
                        // write out a paper trail for an unrewardable packet
                        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
                        let reason = NonRewardablePacketReason::GatewayNotFoundForPacket;
                        let non_rewardable_packet_proto = NonRewardablePacket {
                            packet: Some(valid_packet),
                            reason: reason as i32,
                            timestamp,
                        };
                        self.file_sink
                            .write(
                                non_rewardable_packet_proto,
                                &[("reason", reason.as_str_name())],
                            )
                            .await?;
                        metrics.increment_non_rewardable_packets();
                    };
                    Ok(transaction)
                },
            )
            .await?
            .commit()
            .await?;

        Ok(())
    }
}
