use futures::stream::StreamExt;
use slog::{self, info, o, Drain, Level, Logger};
use std::collections::HashMap;
//use std::io::{ErrorKind, Result};
//use tokio_stream::StreamExt;
// use tokio::sync::mpsc;

use helium_proto::services::router::PacketRouterPacketReportV1;
use helium_proto::Message;
use poc_store::{file_source, Result};
// FIXME for writing to S3:
//use poc_store::FileStore;

// Inputs:
type GatewayPubKey = Vec<u8>;
#[allow(clippy::upper_case_acronyms)]
type OUI = u32;

// Outputs:
// type Bookkeeping = helium_proto::IotPacketVerifierBookkeeping;
// type OuiBooks = helium_proto::OuiBookkeeping;
// type GatewayBooks = helium_proto::GatewayBookkeeping;

// While processing:

pub struct Counts {
    pub gateway: GatewayPubKey,
    pub oui: OUI,
}

// TODO move to Settings.toml
// Same var names as used in HPR's .env files.
/*
static AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
static AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
static AWS_DEFAULT_REGION: &str = "AWS_DEFAULT_REGION";
static PACKET_REPORTER_BUCKET_NAME: &str = "PACKET_REPORTER_BUCKET_NAME";
*/

#[derive(Debug)]
pub struct PacketCounters {
    pub gateway: HashMap<GatewayPubKey, u32>,
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

impl Default for PacketCounters {
    fn default() -> Self {
        Self::new()
    }
}

// TODO replicate mk_logger() from ~/helium/gateway-rs/src/main.rs or similar
pub fn mk_logger() -> Logger {
    let drain = slog_syslog::unix_3164(slog_syslog::Facility::LOG_USER)
        .expect("syslog drain")
        .fuse();
    let async_drain = slog_async::Async::new(drain)
        .build()
        .filter_level(Level::Info)
        .fuse();
    slog::Logger::root(async_drain, o!())
}

pub async fn run(logger: &Logger) -> Result {
    // FIXME open file on S3, which is gz compressed
    // Until then, run: cargo test --features=sample-data && gzip tests/*.data
    //let filenames = ["tests/HPR-report-stream.data.gz"];
    let filenames: Vec<String> = (0..6)
        .into_iter()
        .map(|i| format!("iot_packet_verifier/tests/HPR-report-{i:02x}.data.gz"))
        .collect();
    let mut file_stream = file_source::source(&filenames);

    let mut counters = PacketCounters::new();
    let mut i: usize = 0;
    while let Some(record) = file_stream.next().await {
        i += 1;
        info!(logger, "ingesting: nth-record={}", i);
        // FIXME Error:
        // Io(Custom { kind: InvalidData, error: LengthDelimitedCodecError })
        let msg = record?;
        info!(logger, "decoding: nth-record={}", i);
        let decoded = PacketRouterPacketReportV1::decode(msg)?;
        info!(logger, "counting: nth-record={}", i);
        update_counters(logger, &decoded, &mut counters)
    }

    // FIXME populate bookkeeping structs and write to S3 via ../../store/file_sink.rs

    info!(
        logger,
        "completed: n-records={} filenames={:?}", i, &filenames
    );
    Ok(())
}

pub fn update_counters(
    logger: &Logger,
    ingest: &PacketRouterPacketReportV1,
    counters: &mut PacketCounters,
) {
    let PacketRouterPacketReportV1 {
        oui,
        net_id,
        gateway,
        payload_hash,
        ..
    } = ingest;
    info!(
        logger,
        "ingesting: oui={} netid={:#x} hash={:#x?}", oui, net_id, &payload_hash[0..9];
        "oui" => oui
    );
    let _gw_count = counters
        .gateway
        .entry(gateway.to_owned())
        .and_modify(|n| *n += 1)
        .or_insert(1);
    let _oui_count = counters
        .oui
        .entry(oui.to_owned())
        .and_modify(|n| *n += 1)
        .or_insert(1);
}
