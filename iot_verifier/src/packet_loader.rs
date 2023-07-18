use crate::{
    gateway_cache::GatewayCache, reward_share::GatewayDCShare, telemetry::LoaderMetricTracker,
    Settings,
};
use chrono::{Duration as ChronoDuration, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink, file_sink::FileSinkClient,
    file_upload::MessageSender as FileUploadSender, iot_packet::IotValidPacket, FileType,
};
use futures::{StreamExt, TryStreamExt};
use helium_proto::services::packet_verifier::ValidPacket;
use helium_proto::services::poc_lora::{NonRewardablePacket, NonRewardablePacketReason};
use sqlx::PgPool;
use std::path::Path;
use tokio::sync::mpsc::Receiver;

pub struct PacketLoader {
    pub pool: PgPool,
    pub cache: String,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

impl PacketLoader {
    pub fn from_settings(settings: &Settings, pool: PgPool) -> Self {
        tracing::info!("from_settings packet loader");
        let cache = settings.cache.clone();
        Self { pool, cache }
    }

    pub async fn run(
        &self,
        mut receiver: Receiver<FileInfoStream<IotValidPacket>>,
        shutdown: &triggered::Listener,
        gateway_cache: &GatewayCache,
        file_upload_tx: FileUploadSender,
    ) -> anyhow::Result<()> {
        tracing::info!("starting verifier iot packet loader");
        let store_base_path = Path::new(&self.cache);
        let (non_rewardable_packet_sink, mut non_rewardable_packet_server) =
            file_sink::FileSinkBuilder::new(
                FileType::NonRewardablePacket,
                store_base_path,
                concat!(env!("CARGO_PKG_NAME"), "_non_rewardable_packet"),
                shutdown.clone(),
            )
            .deposits(Some(file_upload_tx.clone()))
            .roll_time(ChronoDuration::minutes(5))
            .create()
            .await?;
        tokio::spawn(async move { non_rewardable_packet_server.run().await });

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                msg = receiver.recv() => if let Some(stream) =  msg {
                    let metrics = LoaderMetricTracker::new();
                    match self.handle_packet_file(stream, gateway_cache, &non_rewardable_packet_sink, &metrics).await {
                        Ok(()) => {
                            // todo: maybe two actions below can occur in handle_packet
                            // but wasnt able to get it to work ?
                            metrics.record_metrics();
                            non_rewardable_packet_sink.commit().await?;

                        },
                        Err(err) => { return Err(err)}
                    }
                }
            }
        }
        tracing::info!("stopping verifier iot packet loader");
        Ok(())
    }

    async fn handle_packet_file(
        &self,
        file_info_stream: FileInfoStream<IotValidPacket>,
        gateway_cache: &GatewayCache,
        non_rewardable_packet_sink: &FileSinkClient,
        metrics: &LoaderMetricTracker,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;

        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(|valid_packet| {
                (
                    ValidPacket::from(valid_packet.clone()),
                    GatewayDCShare::share_from_packet(&valid_packet),
                )
            })
            .map(anyhow::Ok)
            .try_fold(
                transaction,
                |mut transaction, (valid_packet, reward_share)| async move {
                    if gateway_cache
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
                        non_rewardable_packet_sink
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
