use coverage_map::UnrankedCoverage;
use coverage_point_calculator::{RadioType, SPBoostedRewardEligibility};
use helium_crypto::PublicKeyBinary;
use hex_assignments::Assignment;

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
        cbsd_id_opt: Option<String>,
        covered_hexes: &[UnrankedCoverage],
    ) -> SPBoostedRewardEligibility {
        if Self::in_united_states(covered_hexes) {
            self.check_unique_connections(&key, &radio_type)
        } else {
            self.check_radio_thresholds(key, cbsd_id_opt)
        }
    }

    fn check_unique_connections(
        &self,
        key: &PublicKeyBinary,
        radio_type: &RadioType,
    ) -> SPBoostedRewardEligibility {
        if unique_connections::is_qualified(&self.unique_connections, key, radio_type) {
            SPBoostedRewardEligibility::Eligible
        } else {
            SPBoostedRewardEligibility::NotEnoughConnections
        }
    }

    fn check_radio_thresholds(
        &self,
        key: PublicKeyBinary,
        cbsd_id_opt: Option<String>,
    ) -> SPBoostedRewardEligibility {
        if self.radio_thresholds.is_verified(key, cbsd_id_opt) {
            SPBoostedRewardEligibility::Eligible
        } else {
            SPBoostedRewardEligibility::RadioThresholdNotMet
        }
    }

    fn in_united_states(covered_hexes: &[UnrankedCoverage]) -> bool {
        covered_hexes
            .iter()
            .any(|uc| uc.assignments.urbanized != Assignment::C)
    }
}

#[cfg(test)]
mod tests {
    use helium_crypto::{KeyTag, Keypair};
    use hex_assignments::assignment::HexAssignments;
    use hextree::Cell;
    use rand::rngs::OsRng;
    use unique_connections::MINIMUM_UNIQUE_CONNECTIONS;

    use super::*;

    #[test]
    fn eligible_in_united_states() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();

        let mut unique_connections = UniqueConnectionCounts::default();
        unique_connections.insert(pub_key.clone(), MINIMUM_UNIQUE_CONNECTIONS + 1);

        let boosted_hex_eligibility =
            BoostedHexEligibility::new(VerifiedRadioThresholds::default(), unique_connections);

        let covered_hexes = vec![unranked_coverage(Assignment::A)];

        let eligibility = boosted_hex_eligibility.eligibility(
            RadioType::OutdoorWifi,
            pub_key,
            None,
            &covered_hexes,
        );

        assert_eq!(SPBoostedRewardEligibility::Eligible, eligibility);
    }

    #[test]
    fn eligible_outside_states() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();

        let unique_connections = UniqueConnectionCounts::default();
        let mut verified_thresholds = VerifiedRadioThresholds::default();
        verified_thresholds.insert(pub_key.clone(), None);

        let boosted_hex_eligibility =
            BoostedHexEligibility::new(verified_thresholds, unique_connections);

        let covered_hexes = vec![unranked_coverage(Assignment::C)];

        let eligibility = boosted_hex_eligibility.eligibility(
            RadioType::OutdoorWifi,
            pub_key,
            None,
            &covered_hexes,
        );

        assert_eq!(SPBoostedRewardEligibility::Eligible, eligibility);
    }

    #[test]
    fn radio_thresholds_not_met() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();

        let unique_connections = UniqueConnectionCounts::default();
        let verified_thresholds = VerifiedRadioThresholds::default();

        let boosted_hex_eligibility =
            BoostedHexEligibility::new(verified_thresholds, unique_connections);

        let covered_hexes = vec![unranked_coverage(Assignment::C)];

        let eligibility = boosted_hex_eligibility.eligibility(
            RadioType::OutdoorWifi,
            pub_key,
            None,
            &covered_hexes,
        );

        assert_eq!(
            SPBoostedRewardEligibility::RadioThresholdNotMet,
            eligibility
        );
    }

    #[test]
    fn not_enough_connections() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();

        let mut unique_connections = UniqueConnectionCounts::default();
        unique_connections.insert(pub_key.clone(), MINIMUM_UNIQUE_CONNECTIONS);

        let boosted_hex_eligibility =
            BoostedHexEligibility::new(VerifiedRadioThresholds::default(), unique_connections);

        let covered_hexes = vec![unranked_coverage(Assignment::A)];

        let eligibility = boosted_hex_eligibility.eligibility(
            RadioType::OutdoorWifi,
            pub_key,
            None,
            &covered_hexes,
        );

        assert_eq!(
            SPBoostedRewardEligibility::NotEnoughConnections,
            eligibility
        );
    }

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }

    fn unranked_coverage(urbanized: Assignment) -> UnrankedCoverage {
        UnrankedCoverage {
            location: Cell::from_raw(631236586635449855).expect("invalid cell"),
            signal_power: 0,
            signal_level: coverage_map::SignalLevel::High,
            assignments: HexAssignments {
                footfall: Assignment::A,
                landtype: Assignment::A,
                urbanized,
            },
        }
    }
}
