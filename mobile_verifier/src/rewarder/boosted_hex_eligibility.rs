use coverage_point_calculator::{RadioType, SPBoostedRewardEligibility};
use helium_crypto::PublicKeyBinary;

use crate::{
    radio_threshold::VerifiedRadioThresholds,
    unique_connections::{self, UniqueConnectionCounts},
};

#[derive(Debug, Default)]
pub struct BoostedHexEligibility {
    radio_thresholds: VerifiedRadioThresholds,
    unique_connections: UniqueConnectionCounts,
}

impl BoostedHexEligibility {
    pub fn new(
        radio_thresholds: VerifiedRadioThresholds,
        unique_connections: UniqueConnectionCounts,
    ) -> Self {
        Self {
            radio_thresholds,
            unique_connections,
        }
    }

    pub fn eligibility(
        &self,
        radio_type: RadioType,
        key: PublicKeyBinary,
    ) -> SPBoostedRewardEligibility {
        //TODO only check radio thresholds if in mexico?
        if unique_connections::is_qualified(&self.unique_connections, &key, &radio_type) {
            SPBoostedRewardEligibility::Eligible
        } else {
            SPBoostedRewardEligibility::NotEnoughConnections
        }
    }
}

#[cfg(test)]
mod tests {
    use helium_crypto::{KeyTag, Keypair};
    use rand::rngs::OsRng;
    use unique_connections::MINIMUM_UNIQUE_CONNECTIONS;

    use super::*;

    #[test]
    fn eligible() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();

        let mut unique_connections = UniqueConnectionCounts::default();
        unique_connections.insert(pub_key.clone(), MINIMUM_UNIQUE_CONNECTIONS + 1);

        let boosted_hex_eligibility = BoostedHexEligibility::new(unique_connections);

        let eligibility = boosted_hex_eligibility.eligibility(RadioType::OutdoorWifi, pub_key);

        assert_eq!(SPBoostedRewardEligibility::Eligible, eligibility);
    }

    #[test]
    fn not_enough_connections() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();

        let mut unique_connections = UniqueConnectionCounts::default();
        unique_connections.insert(pub_key.clone(), MINIMUM_UNIQUE_CONNECTIONS);

        let boosted_hex_eligibility = BoostedHexEligibility::new(unique_connections);

        let eligibility = boosted_hex_eligibility.eligibility(RadioType::OutdoorWifi, pub_key);

        assert_eq!(
            SPBoostedRewardEligibility::NotEnoughConnections,
            eligibility
        );
    }

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }
}
