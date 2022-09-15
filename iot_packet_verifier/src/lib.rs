use futures::stream::StreamExt;
use std::collections::HashMap;

use helium_crypto::public_key::PublicKey;
use helium_proto::services::router::PacketRouterPacketReportV1;
use helium_proto::Message;
use poc_store::{file_source, Error, Result};
// FIXME for writing to S3:
//use poc_store::FileStore;

/// Organizational ID associated with each customer, used as input:
#[allow(clippy::upper_case_acronyms)]
type OUI = u32;

// Outputs:
// type Bookkeeping = helium_proto::IotPacketVerifierBookkeeping;
// type OuiBooks = helium_proto::OuiBookkeeping;
// type GatewayBooks = helium_proto::GatewayBookkeeping;

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

/// Iterate through Helium Packet Router "reports" (Protobufs) in S3.
pub async fn run() -> Result {
    // FIXME open file on S3, which has length encoding and then gzip'd.
    // Until then, run: cargo test --features=sample-data

    let filenames = ["tests/HPR-report.stream.gz"];
    let mut file_stream = file_source::source(&filenames);

    let mut counters = PacketCounters::new();
    let mut i: usize = 0;
    while let Some(record) = file_stream.next().await {
        i += 1;
        println!("ingesting: nth-record={}", i);
        let msg = match record {
            Ok(msg) => msg,
            // TODO is there a more graceful way of ending the stream?
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                println!("error: {:?}", e);
                break;
            }
        };
        println!("decoding: nth-record={}", i);
        let decoded = PacketRouterPacketReportV1::decode(msg)?;
        println!("counting: nth-record={}", i);
        update_counters(&decoded, &mut counters)
    }

    // FIXME populate bookkeeping structs and write to S3 via ../../store/file_sink.rs

    println!("completed: n-records={} filenames={:?}", i, &filenames);
    Ok(())
}

pub fn update_counters(ingest: &PacketRouterPacketReportV1, counters: &mut PacketCounters) {
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
        &payload_hash[0..5].iter().map(|x| format!("{x:02x}")).collect::<String>()
    );
    if let Ok(pubkey) = PublicKey::from_bytes(gateway) {
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
    }
}
