extern crate iot_packet_verifier;

use iot_packet_verifier::run;
use poc_store::Result;

#[tokio::main]
async fn main() -> Result {
    run().await
}
