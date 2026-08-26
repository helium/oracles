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
    /// HIP-150 ticket issuers. Kept separate from `carrier` so that holding a
    /// carrier key does not confer the ability to grant a hotspot a reward
    /// multiplier. May be empty, in which case no ticket is accepted.
    data_transfer_multiplier: HashSet<PublicKeyBinary>,
}

impl AuthorizedKeys {
    pub fn new(
        carrier: HashSet<PublicKeyBinary>,
        data_transfer_multiplier: HashSet<PublicKeyBinary>,
    ) -> Self {
        Self {
            carrier,
            data_transfer_multiplier,
        }
    }
}

/// A role-scoped authorization check. Kept as a trait (rather than using
/// [`AuthorizedKeys`] directly at call sites) so tests can substitute a mock.
pub trait AuthorizationVerifier: Send + Sync + 'static {
    fn is_authorized(&self, address: &PublicKeyBinary, role: NetworkKeyRole) -> bool;

    /// HIP-150: may this key issue data transfer multiplier tickets?
    ///
    /// Deliberately not a `NetworkKeyRole` arm. That enum comes from
    /// `mobile_config.proto`, and mobile-config is decommissioned in this
    /// stack — it survives only as this trait's role parameter. Adding a
    /// variant would grow a dead service's enum, so ticket authorization gets
    /// its own check instead.
    fn is_ticket_signer(&self, address: &PublicKeyBinary) -> bool;
}

impl AuthorizationVerifier for AuthorizedKeys {
    fn is_authorized(&self, address: &PublicKeyBinary, role: NetworkKeyRole) -> bool {
        match role {
            NetworkKeyRole::MobileCarrier => self.carrier.contains(address),
            // Ingest only authorizes the role above.
            _ => false,
        }
    }

    fn is_ticket_signer(&self, address: &PublicKeyBinary) -> bool {
        self.data_transfer_multiplier.contains(address)
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
        let keys = AuthorizedKeys::new(HashSet::from([key(1)]), HashSet::new());

        assert!(keys.is_authorized(&key(1), NetworkKeyRole::MobileCarrier));
        assert!(!keys.is_authorized(&key(2), NetworkKeyRole::MobileCarrier));

        // Roles ingest doesn't manage are never authorized.
        assert!(!keys.is_authorized(&key(1), NetworkKeyRole::Banning));
        assert!(!keys.is_authorized(&key(1), NetworkKeyRole::MobileRouter));
    }

    #[test]
    fn ticket_signing_is_separate_from_the_carrier_allow_list() {
        let keys = AuthorizedKeys::new(HashSet::from([key(1)]), HashSet::from([key(2)]));

        // A carrier key cannot grant a multiplier...
        assert!(keys.is_authorized(&key(1), NetworkKeyRole::MobileCarrier));
        assert!(!keys.is_ticket_signer(&key(1)));

        // ...and a ticket signer is not thereby a carrier.
        assert!(keys.is_ticket_signer(&key(2)));
        assert!(!keys.is_authorized(&key(2), NetworkKeyRole::MobileCarrier));
    }

    #[test]
    fn empty_ticket_allow_list_rejects_every_signer() {
        let keys = AuthorizedKeys::new(HashSet::from([key(1)]), HashSet::new());

        assert!(!keys.is_ticket_signer(&key(1)));
        assert!(!keys.is_ticket_signer(&key(2)));
    }
}
