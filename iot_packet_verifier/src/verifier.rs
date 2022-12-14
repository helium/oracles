use futures::{Stream, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::services::router::PacketRouterPacketReportV1;
use std::cell::RefCell;
use std::{collections::HashMap, hash::Hash};

type Oui = u64;

#[derive(Debug)]
pub struct Counters<K> {
    counters: RefCell<HashMap<K, u64>>,
}

impl<K> Default for Counters<K> {
    fn default() -> Self {
        Self {
            counters: RefCell::new(HashMap::new()),
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

    pub fn inc(&self, key: K, bytes: u64) {
        *self.counters.borrow_mut().entry(key).or_default() += bytes;
    }
}

pub struct Verifier<A> {
    ingest: A,
}

impl<A> Verifier<A> {
    pub fn new(ingest: A) -> Self {
        Self { ingest }
    }
}

impl<A> Verifier<A>
where
    A: Stream<Item = PacketRouterPacketReportV1>,
{
    pub async fn verify_stream(
        self,
    ) -> VerifiedStream<impl Iterator<Item = (PublicKey, u64)>, impl Iterator<Item = (Oui, u64)>>
    {
        let Self { ingest } = self;
        let gateway_counters = Counters::new();
        let oui_counters = Counters::new();

        tokio::pin!(ingest);

        // Incrementing the counters should be basically instantaneous so there's no reason
        // to fan them out to separate threads or to increment them asynchronously.
        while let Some(report) = ingest.next().await {
            let bytes = report.payload_size as u64;
            let gateway = match PublicKey::try_from(report.gateway) {
                Ok(gateway) => gateway,
                Err(err) => {
                    tracing::error!("Invalid gateway pub key: {}", err);
                    continue;
                }
            };
            gateway_counters.inc(gateway, bytes);
            oui_counters.inc(report.oui, bytes);
        }

        VerifiedStream {
            gateway_counters: gateway_counters.counters.into_inner().into_iter(),
            oui_counters: oui_counters.counters.into_inner().into_iter(),
        }
    }
}

pub struct VerifiedStream<A, B> {
    pub gateway_counters: A,
    pub oui_counters: B,
}
