extern crate iot_packet_verifier;

use iot_packet_verifier::{mk_logger, run};
use poc_store::Result;
use slog::info;

#[tokio::main]
async fn main() -> Result {
    let logger = mk_logger();
    info!(logger, "starting");
    run(&logger).await
}
