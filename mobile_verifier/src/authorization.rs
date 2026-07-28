//! Static, settings-backed authorization allow-lists.
//!
//! Replaces the mobile-config gRPC authorization service. mobile-config served
//! authorized keys from an admin-registered `registered_keys` table —
//! effectively static config — so the verifier now reads the same allow-lists
//! straight from its settings, one set per role. Mirrors the routing-key
//! allow-list mobile-packet-verifier uses (`mobile_packet_verifier::routing`).

use std::collections::HashSet;

use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;

/// The set of keys authorized for each role the verifier checks. Built from
/// settings — see [`Settings::authorized_keys`](crate::Settings::authorized_keys).
#[derive(Debug, Clone, Default)]
pub struct AuthorizedKeys {
    banning: HashSet<PublicKeyBinary>,
    mobile_carrier: HashSet<PublicKeyBinary>,
}

impl AuthorizedKeys {
    pub fn new(
        banning: HashSet<PublicKeyBinary>,
        mobile_carrier: HashSet<PublicKeyBinary>,
    ) -> Self {
        Self {
            banning,
            mobile_carrier,
        }
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
            NetworkKeyRole::Banning => self.banning.contains(address),
            NetworkKeyRole::MobileCarrier => self.mobile_carrier.contains(address),
            // The verifier only authorizes the two roles above.
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
        let keys = AuthorizedKeys::new(HashSet::from([key(1)]), HashSet::from([key(2)]));

        assert!(keys.is_authorized(&key(1), NetworkKeyRole::Banning));
        assert!(!keys.is_authorized(&key(2), NetworkKeyRole::Banning));

        assert!(keys.is_authorized(&key(2), NetworkKeyRole::MobileCarrier));
        assert!(!keys.is_authorized(&key(1), NetworkKeyRole::MobileCarrier));

        // A role the verifier doesn't manage is never authorized.
        assert!(!keys.is_authorized(&key(1), NetworkKeyRole::MobileRouter));
    }
}
