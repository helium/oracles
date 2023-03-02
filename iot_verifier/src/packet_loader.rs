use crate::{packet, Settings};

use chrono::{Duration, TimeZone, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_info_poller::LookbackBehavior, file_source,
    iot_packet::IotValidPacket, FileStore, FileType,
};
use futures::StreamExt;
use sqlx::PgPool;

const DB_POOL_SIZE: usize = 100;

pub struct Loader {
    packet_store: FileStore,
    packet_interval: Duration,
    pool: PgPool,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

pub enum ValidPacketResult {
    Valid,
    Denied,
    Unknown,
}

impl Loader {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewLoaderError> {
        let pool = settings.database.connect(DB_POOL_SIZE).await?;
        let packet_store = FileStore::from_settings(&settings.packet_ingest).await?;
        let packet_interval = settings.packet_interval();
        Ok(Self {
            pool,
            packet_store,
            packet_interval,
        })
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting verifier iot packet loader");
        let (mut receiver, _source_join_handle) =
            file_source::continuous_source::<IotValidPacket>()
                .db(self.pool.clone()) // TODO: fix this cloning!!!
                .store(self.packet_store.clone())
                .file_type(FileType::IotValidPacket)
                .lookback(LookbackBehavior::StartAfter(
                    Utc.timestamp_opt(0, 0).single().unwrap(),
                ))
                .poll_duration(self.packet_interval)
                .offset(self.packet_interval * 2)
                .build()?
                .start(shutdown.clone())
                .await?;
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
        let mut stream = file_info_stream.into_stream(&mut transaction).await?;
        while let Some(valid_packet) = stream.next().await {
            packet::insert(
                &mut transaction,
                valid_packet.payload_size as i32,
                valid_packet.gateway.into(),
                valid_packet.payload_hash,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}
