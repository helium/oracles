//! The routing-key allow-list used to verify data transfer reports.

use std::collections::HashSet;

use helium_crypto::PublicKeyBinary;

/// The set of routing (carrier) keys authorized to submit data transfer
/// sessions. A report whose routing key is not in this set is rejected as
/// `InvalidRoutingKey`. Built from settings — see
/// [`Settings::routing_keys`](crate::settings::Settings::routing_keys).
#[derive(Debug, Clone, Default)]
pub struct RoutingKeys(HashSet<PublicKeyBinary>);

impl RoutingKeys {
    pub fn contains(&self, routing_key: &PublicKeyBinary) -> bool {
        self.0.contains(routing_key)
    }
}

impl FromIterator<PublicKeyBinary> for RoutingKeys {
    fn from_iter<I: IntoIterator<Item = PublicKeyBinary>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
