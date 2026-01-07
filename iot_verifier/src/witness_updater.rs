//
// Manages a short lived cache and  bulk updating of last witness timestamps to the DB
// Each time a POC is processed, the 'update' API is called with a vec of witness timestamps
// These timestamps get appended to the cache and that cache is then flushed to the DB every 60 seconds
// The 'get_last_witness' API provides a read through API to get the last witness timestamp for a given gateway
// The primary value of this setup is to reduce the number of DB read/writes associated with POC verification
// The last witness timestamp values are used as part of beacon reciprocity verification
//

use crate::last_witness::LastWitness;
use helium_crypto::PublicKeyBinary;
use metrics::Gauge;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use task_manager::ManagedTask;
use tokio::{
    sync::{mpsc, RwLock},
    time::{self, MissedTickBehavior},
};

const WRITE_INTERVAL: time::Duration = time::Duration::from_secs(60);

pub type WitnessMap = HashMap<PublicKeyBinary, LastWitness>;
pub type MessageSender = mpsc::Sender<Vec<LastWitness>>;
pub type MessageReceiver = mpsc::Receiver<Vec<LastWitness>>;

#[derive(Clone)]
struct Telemetry {
    queue_gauge: Gauge,
}

impl Telemetry {
    fn new() -> Self {
        let gauge = metrics::gauge!("iot_verifier_witness_updater_queue");
        gauge.set(0.0);
        Self { queue_gauge: gauge }
    }

    fn increment_queue(&self) {
        self.queue_gauge.increment(1.0);
    }

    fn decrement_queue(&self) {
        self.queue_gauge.decrement(1.0);
    }
}

pub struct WitnessUpdater {
    pool: PgPool,
    cache: Arc<RwLock<WitnessMap>>,
    sender: MessageSender,
    telemetry: Telemetry,
}

pub struct WitnessUpdaterServer {
    pool: PgPool,
    cache: Arc<RwLock<WitnessMap>>,
    receiver: MessageReceiver,
    telemetry: Telemetry,
}

impl ManagedTask for WitnessUpdaterServer {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl WitnessUpdater {
    pub async fn new(pool: PgPool) -> anyhow::Result<(Self, WitnessUpdaterServer)> {
        let cache = Arc::new(RwLock::new(WitnessMap::new()));
        let (sender, receiver) = mpsc::channel(500);
        let telemetry = Telemetry::new();
        Ok((
            Self {
                pool: pool.clone(),
                cache: cache.clone(),
                sender,
                telemetry: telemetry.clone(),
            },
            WitnessUpdaterServer {
                pool,
                cache,
                receiver,
                telemetry,
            },
        ))
    }

    pub async fn get_last_witness(
        &self,
        pubkey: &PublicKeyBinary,
    ) -> anyhow::Result<Option<LastWitness>> {
        match self.cache.read().await.get(pubkey) {
            Some(witness) => Ok(Some(witness.clone())),
            None => LastWitness::get(&self.pool, pubkey).await,
        }
    }

    pub async fn update(&self, witnesses: Vec<LastWitness>) -> anyhow::Result<()> {
        self.telemetry.increment_queue();
        self.sender
            .send(witnesses)
            .await
            .map_err(anyhow::Error::from)
    }
}

impl WitnessUpdaterServer {
    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting witness updater process");
        let mut write_timer = time::interval(WRITE_INTERVAL);
        write_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = write_timer.tick() => {
                    self.write_cache().await?;
                }
                message = self.receiver.recv() => {
                    if let Some(updates) = message {
                        self.telemetry.decrement_queue();
                        self.update_cache(updates).await;
                    }
                }
            }
        }
        tracing::info!("stopping witness updater process");
        Ok(())
    }

    pub async fn write_cache(&mut self) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        if !cache.is_empty() {
            let updates = cache.values().collect::<Vec<_>>();
            tracing::info!("writing {} updates to db", updates.len());
            LastWitness::bulk_update_last_timestamps(&self.pool, updates).await?;
            cache.clear();
        }
        Ok(())
    }

    pub async fn update_cache(&mut self, updates: Vec<LastWitness>) {
        tracing::debug!("updating cache with {} entries", updates.len());
        let mut cache = self.cache.write().await;
        updates.into_iter().for_each(|update| {
            cache.insert(update.id.clone(), update);
        });
    }
}
