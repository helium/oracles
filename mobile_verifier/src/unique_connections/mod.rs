pub mod db;
pub mod ingestor;

use coverage_point_calculator::RadioType;
use helium_crypto::PublicKeyBinary;
use std::collections::HashMap;

pub type UniqueConnectionCounts = HashMap<PublicKeyBinary, u64>;

// hip-134:
// https://github.com/helium/HIP/blob/main/0134-reward-mobile-carrier-offload-hotspots.md
// A Hotspot serving >25 unique connections, as defined by the Carrier utlizing the hotspots for Carrier Offload, on a seven day rolling average.
const MINIMUM_UNIQUE_CONNECTIONS: u64 = 25;

pub fn is_qualified(
    unique_connections: &UniqueConnectionCounts,
    pubkey: &PublicKeyBinary,
    radio_type: &RadioType,
) -> bool {
    let uniq_conns = unique_connections.get(pubkey).cloned().unwrap_or_default();
    radio_type.is_wifi() && uniq_conns > MINIMUM_UNIQUE_CONNECTIONS
}
