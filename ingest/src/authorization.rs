//! Static, settings-backed authorization allow-lists.
//!
//! Replaces the mobile-config gRPC authorization service. mobile-config served
//! authorized keys from an admin-registered `registered_keys` table —
//! effectively static config — so ingest now reads the same allow-list straight
//! from its settings. Mirrors `mobile_verifier::authorization` and the
//! routing-key allow-list mobile-packet-verifier uses
//! (`mobile_packet_verifier::routing`).

use std::collections::HashSet;

use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;

/// The set of keys authorized for each role ingest checks. Built from settings —
/// see [`Settings::authorized_keys`](crate::Settings::authorized_keys).
#[derive(Debug, Clone, Default)]
pub struct AuthorizedKeys {
    carrier: HashSet<PublicKeyBinary>,
}

impl AuthorizedKeys {
    pub fn new(carrier: HashSet<PublicKeyBinary>) -> Self {
        Self { carrier }
    }
}

/// A role-scoped authorization check. Kept as a trait (rather than using
/// [`AuthorizedKeys`] directly at call sites) so tests can substitute a mock.
pub trait AuthorizationVerifier: Send + Sync + 'static {
    fn is_authorized(&self, address: &PublicKeyBinary, role: NetworkKeyRole) -> bool;
}

impl AuthorizationVerifier for AuthorizedKeys {
    fn is_authorized(&self, address: &PublicKeyBinary, role: NetworkKeyRole) -> bool {
        match role {
            NetworkKeyRole::MobileCarrier => self.carrier.contains(address),
            // Ingest only authorizes the role above.
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> PublicKeyBinary {
        PublicKeyBinary::from(vec![byte])
    }

    #[test]
    fn authorizes_only_configured_keys_per_role() {
        let keys = AuthorizedKeys::new(HashSet::from([key(1)]));

        assert!(keys.is_authorized(&key(1), NetworkKeyRole::MobileCarrier));
        assert!(!keys.is_authorized(&key(2), NetworkKeyRole::MobileCarrier));

        // Roles ingest doesn't manage are never authorized.
        assert!(!keys.is_authorized(&key(1), NetworkKeyRole::Banning));
        assert!(!keys.is_authorized(&key(1), NetworkKeyRole::MobileRouter));
    }
}
