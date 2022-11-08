/// Run the primary counters.
/// See a packet, count a packet for billing purposes.
use crate::{Result, Settings};
//use chrono::{Duration, Utc};
use file_store::{file_sink, file_sink::MessageSender, file_sink_write, file_upload, FileType};
use helium_crypto::public_key::PublicKey;
// helium/proto/src/blockchain_txn_subnetwork_rewards_v1.proto
use helium_proto::SubnetworkReward;
//FIXME use node_follower::follower_service;
use std::collections::HashMap;
use std::path::Path;
use tokio::time;

/// Organizational ID associated with each customer, used as input:
#[allow(clippy::upper_case_acronyms)]
type OUI = u32;

/// Cadence in seconds when to persist file upload and reset counters
const POLL_TIME: time::Duration = time::Duration::from_secs(303);

/// Simple per-packet traffic counting.
#[derive(Debug, Default)]
pub struct PacketCounters {
    pub gateway: HashMap<PublicKey, u32>,
    pub oui: HashMap<OUI, u32>,
}

impl PacketCounters {
    pub fn new() -> Self {
        Self {
            gateway: HashMap::new(),
            oui: HashMap::new(),
        }
    }
}

pub struct Runner {
    counters: PacketCounters,
    settings: Settings,
}

impl Runner {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let counters = PacketCounters::new();
        let settings = settings.clone();
        Ok(Self { counters, settings })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("starting runner");

        let mut timer = time::interval(POLL_TIME);
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let rewards_path = Path::new(&self.settings.rewards.bucket);
        let debits_path = Path::new(&self.settings.debits.bucket);
        let (rewards_tx, rewards_rx) = file_sink::message_channel(50);
        let (debits_tx, debits_rx) = file_sink::message_channel(50);
        let (rewards_upload_tx, rewards_upload_rx) = file_upload::message_channel();
        let (debits_upload_tx, debits_upload_rx) = file_upload::message_channel();
        let rewards_upload =
            file_upload::FileUpload::from_settings(&self.settings.rewards, rewards_upload_rx)
                .await?;
        let mut rewards_sink =
            file_sink::FileSinkBuilder::new(FileType::SubnetworkRewards, rewards_path, rewards_rx)
                .deposits(Some(rewards_upload_tx.clone()))
                .create()
                .await?;
        let debits_upload =
            file_upload::FileUpload::from_settings(&self.settings.debits, debits_upload_rx).await?;
        let mut debits_sink =
            file_sink::FileSinkBuilder::new(FileType::SubnetworkDebits, debits_path, debits_rx)
                .deposits(Some(debits_upload_tx.clone()))
                .create()
                .await?;

        let shutdown2 = shutdown.clone();
        let shutdown3 = shutdown.clone();
        let shutdown4 = shutdown.clone();
        let shutdown5 = shutdown.clone();
        tokio::spawn(async move { rewards_sink.run(&shutdown2).await });
        tokio::spawn(async move { rewards_upload.run(&shutdown3).await });
        tokio::spawn(async move { debits_sink.run(&shutdown4).await });
        tokio::spawn(async move { debits_upload.run(&shutdown5).await });
        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = timer.tick() =>
                    match self.handle_tick(rewards_tx.clone(), debits_tx.clone()).await {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::error!("fatal runner error: {err:?}");
                            return Err(err)
                        }
                    }
            }
        }
        tracing::info!("stopping runner");
        Ok(())
    }

    /// Handle crediting gateway owners with rewards for providing the
    /// network and debiting DC from OUI owners for using the network.
    async fn handle_tick(&self, rewards_tx: MessageSender, debits_tx: MessageSender) -> Result {
        for (pubkeybin, count) in self.counters.gateway.iter() {
            // FIXME resolve pubkeybin to owner via follower_service
            let protobuf = SubnetworkReward {
                account: pubkeybin.into(),
                amount: self.calc_rewards(*count),
            };
            let _ = file_sink_write!("credit_rewards", &rewards_tx, protobuf).await?;
        }
        for (oui, count) in self.counters.oui.iter() {
            // FIXME resolve OUI to owner via follower_service
            let protobuf = SubnetworkReward {
                account: oui.to_be_bytes().to_vec(),
                amount: self.calc_debits(*count),
            };
            let _ = file_sink_write!("debit_dc", &debits_tx, protobuf).await?;
        }
        Ok(())
    }

    /// Calculate rewards to credit gateway's owner for providing the network.
    pub fn calc_rewards(&self, counter: u32) -> u64 {
        // FIXME use multiplier from settings.toml
        counter as u64
    }

    /// Calculate DC to debit from OUI's owner for using the network.
    pub fn calc_debits(&self, counter: u32) -> u64 {
        // FIXME use multiplier from settings.toml
        counter as u64
    }
}
