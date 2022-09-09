extern crate iot_packet_verifier;

use slog::info;
use poc_store::Result;
use iot_packet_verifier::{mk_logger, run};

#[tokio::main]
async fn main() -> Result {
    let logger = mk_logger();
    info!(logger, "starting");
    run(&logger).await
}
