use crate::{Result, Settings};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::{FileStore, FileType};
use futures::{stream, StreamExt};
use helium_crypto::public_key::PublicKey;
use helium_proto::services::router::PacketRouterPacketReportV1;
use helium_proto::Message;
use tokio::time;

/// cadence for how often to look for new beacon and witness reports from s3 bucket
const REPORTS_POLL_TIME: time::Duration = time::Duration::from_secs(63);

pub struct Loader {
    /// Where to read reports from Helium Packet Router
    ingest_store: FileStore,
    // FIXME add results store for persisting our results
    // FIXME add follower service for resolving gateway pubkey or OUI to owner
}

impl Loader {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        tracing::info!("from_settings");
        let ingest_store = FileStore::from_settings(&settings.ingest).await?;
        Ok(Self { ingest_store })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("started loader");

        let mut report_timer = time::interval(REPORTS_POLL_TIME);
        report_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let store = &self.ingest_store;
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = report_timer.tick() => match self.process_events(&store, shutdown.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal report loader error: {err:?}");
                    }
                },
            }
        }
        tracing::info!("stopping loader");
        Ok(())
    }

    /// Iterate through Helium Packet Router "packetreport" protobuf files in S3.
    pub async fn process_events(&self, store: &FileStore, shutdown: triggered::Listener) -> Result {
        let file_type = FileType::IotPacketReport;
        // FIXME: get prev timestamp from persisted state, otherwise processes all.
        let last_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1666777888, 9), Utc);
        let infos = store.list_all(file_type, last_time, None).await?;
        if infos.is_empty() {
            tracing::info!("no ingest {file_type} files to process from: {last_time}");
            return Ok(());
        }
        let last_time = infos.last().map(|v| v.timestamp);
        let infos_len = infos.len();
        tracing::info!("processing {infos_len} {file_type} files");
        let handler = store
            // FIXME: increase concurrency beyond hardcoded `1`
            .source_unordered(1, stream::iter(infos).map(Ok).boxed())
            .for_each_concurrent(1, |msg| async move {
                match msg {
                    Err(err) => tracing::warn!("skipping entry in {file_type} stream: {err:?}"),
                    Ok(buf) => match self.store_update(&buf).await {
                        Ok(()) => {
                            tracing::info!("UPDATED last_time={last_time:?}"); // DELETE ME
                            ()
                        }
                        Err(err) => {
                            tracing::warn!("failed to update store: {err:?}")
                        }
                    },
                }
            });
        tokio::select! {
            _ = handler => {
                tracing::info!("completed processing {infos_len} {file_type} files");
                // FIXME update last_timestamp
                Ok(())
            },
            _ = shutdown.clone() => Ok(()),
        }
    }

    pub async fn store_update(&self, buf: &[u8]) -> Result {
        let decoded = PacketRouterPacketReportV1::decode(buf)?;
        self.update_counters(&decoded);
        Ok(())
    }

    pub fn update_counters(&self, ingest: &PacketRouterPacketReportV1) {
        let PacketRouterPacketReportV1 {
            oui,
            net_id,
            gateway,
            payload_hash,
            ..
        } = ingest;
        println!(
            "updating: oui={} netid=0x{:04x} hash=0x{}...",
            oui,
            net_id,
            &payload_hash[0..5]
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect::<String>()
        );
        if let Ok(pubkeybin) = PublicKey::from_bytes(gateway) {
            println!("pubkeybin={:?}", pubkeybin);
            /* FIXME send msg instead
            let _gw_count = counters
                .gateway
                .entry(pubkey)
                .and_modify(|n| *n += 1)
                .or_insert(1);
            let _oui_count = counters
                .oui
                .entry(oui.to_owned())
                .and_modify(|n| *n += 1)
                .or_insert(1);
            */
        }
    }
}
