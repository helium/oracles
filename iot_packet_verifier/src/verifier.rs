use crate::balances::Balances;
use futures::{Stream, StreamExt};
use helium_proto::{services::router::PacketRouterPacketReportV1, VerifiedPacket};

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

        ingest.then(move |report| async move {
            let payload_size: u64 = todo!();
            let is_valid = balances
                .debit_balance_if_sufficient(&report.oui, payload_size_to_dc(payload_size))
                .await;
            VerifiedPacket {
                oui: report.oui,
                payload_size: payload_size as u32,
                gateway: report.gateway,
                payload_hash: report.payload_hash,
                is_valid,
            }
        })
    }
}
