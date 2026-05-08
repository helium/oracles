pub mod db;
pub mod ingestor;

use helium_crypto::PublicKeyBinary;
use std::collections::HashMap;

pub type UniqueConnectionCounts = HashMap<PublicKeyBinary, u64>;

// hip-134:
// https://github.com/helium/HIP/blob/main/0134-reward-mobile-carrier-offload-hotspots.md
// A Hotspot serving >25 unique connections, as defined by the Carrier utlizing the hotspots for Carrier Offload, on a seven day rolling average.
pub const MINIMUM_UNIQUE_CONNECTIONS: u64 = 25;

pub fn is_qualified(unique_connections: &UniqueConnectionCounts, pubkey: &PublicKeyBinary) -> bool {
    let uniq_conns = unique_connections.get(pubkey).cloned().unwrap_or_default();
    uniq_conns > MINIMUM_UNIQUE_CONNECTIONS
}
