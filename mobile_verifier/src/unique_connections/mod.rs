pub mod db;
pub mod ingestor;

use helium_crypto::PublicKeyBinary;
use std::collections::HashMap;

pub type UniqueConnectionCounts = HashMap<PublicKeyBinary, u64>;
