use crate::balances::Balances;
use futures::{Stream, StreamExt};
use helium_crypto::PublicKey;
use helium_proto::{
    services::packet_verifier::VerifiedPacket, services::router::PacketRouterPacketReportV1,
};

pub struct Verifier<A> {
    ingest: A,
}

impl<A> Verifier<A> {
    pub fn new(ingest: A) -> Self {
        Self { ingest }
    }
}

pub fn payload_size_to_dc(payload_size: u64) -> u64 {
    payload_size.min(24) / 24
}

impl<'a, A> Verifier<A>
where
    A: Stream<Item = PacketRouterPacketReportV1> + 'a,
{
    pub fn verify_stream(self, balances: &'a Balances) -> impl Stream<Item = VerifiedPacket> + 'a {
        let Self { ingest } = self;

        ingest.filter_map(move |report| async move {
            let payload_size: u64 = todo!();
            let Ok(gateway) = PublicKey::from_bytes(&report.gateway) else {
                return None;
            };
            let Ok(is_valid) = balances.debit(&gateway, payload_size_to_dc(payload_size)).await else {
                return None;
            };
            Some(VerifiedPacket {
                payload_size: payload_size as u32,
                gateway: report.gateway,
                payload_hash: report.payload_hash,
                is_valid,
            })
        })
    }
}
