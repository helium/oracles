use futures::{Stream, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::services::router::PacketRouterPacketReportV1;
use std::{collections::HashMap, hash::Hash};
use tokio::sync::RwLock;

type Oui = u64;

#[derive(Debug)]
pub struct Counters<K> {
    counters: RwLock<HashMap<K, u64>>,
}

impl<K> Default for Counters<K> {
    fn default() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
        }
    }
}

impl<K> Counters<K>
where
    K: Hash + Ord,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn inc(&self, key: K, bytes: u64) {
        // Since we are calling this function from a concurrent context, we
        // should never have to yield on this await.
        *self.counters.write().await.entry(key).or_insert(bytes) += bytes;
    }
}

pub struct Verifier<A> {
    ingest: A,
}

pub const PACKET_VERIFIER_WORKERS: usize = 100;

impl<A> Verifier<A> {
    pub fn new(ingest: A) -> Self {
        Self { ingest }
    }
}

impl<A> Verifier<A>
where
    A: Stream<Item = PacketRouterPacketReportV1>,
{
    pub async fn verify_epoch(
        self,
    ) -> VerifiedEpoch<impl Iterator<Item = (PublicKey, u64)>, impl Iterator<Item = (Oui, u64)>>
    {
        let gateway_counters = Counters::new();
        let oui_counters = Counters::new();

        self.ingest
            .for_each_concurrent(PACKET_VERIFIER_WORKERS, |report| {
                let gateway_counters = &gateway_counters;
                let oui_counters = &oui_counters;
                async move {
                    let bytes = report.payload_size;
                    let gateway = PublicKey::try_from(report.gateway).unwrap();
                    gateway_counters.inc(gateway, bytes as u64).await;
                    oui_counters.inc(report.oui, bytes as u64).await;
                }
            })
            .await;

        VerifiedEpoch {
            gateway_counters: gateway_counters.counters.into_inner().into_iter(),
            oui_counters: oui_counters.counters.into_inner().into_iter(),
        }
    }
}

pub struct VerifiedEpoch<A, B> {
    pub gateway_counters: A,
    pub oui_counters: B,
}
