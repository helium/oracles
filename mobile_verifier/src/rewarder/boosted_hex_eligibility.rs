use coverage_point_calculator::{RadioType, SPBoostedRewardEligibility};
use helium_crypto::PublicKeyBinary;

use crate::{
    radio_threshold::VerifiedRadioThresholds,
    sp_boosted_rewards_bans::BannedRadios,
    unique_connections::{self, UniqueConnectionCounts},
};

#[derive(Debug, Default)]
pub struct BoostedHexEligibility {
    radio_thresholds: VerifiedRadioThresholds,
    banned_radios: BannedRadios,
    unique_connections: UniqueConnectionCounts,
}

impl BoostedHexEligibility {
    pub fn new(
        radio_thresholds: VerifiedRadioThresholds,
        banned_radios: BannedRadios,
        unique_connections: UniqueConnectionCounts,
    ) -> Self {
        Self {
            radio_thresholds,
            banned_radios,
            unique_connections,
        }
    }

    pub fn eligibility(
        &self,
        radio_type: RadioType,
        key: PublicKeyBinary,
        cbsd_id_opt: Option<String>,
    ) -> SPBoostedRewardEligibility {
        if unique_connections::is_qualified(&self.unique_connections, &key, &radio_type) {
            SPBoostedRewardEligibility::Eligible
        } else if self.banned_radios.contains(&key, cbsd_id_opt.as_deref()) {
            SPBoostedRewardEligibility::ServiceProviderBanned
        } else if self.radio_thresholds.is_verified(key, cbsd_id_opt) {
            SPBoostedRewardEligibility::Eligible
        } else {
            SPBoostedRewardEligibility::RadioThresholdNotMet
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
    fn wifi_eligible_with_unique_connections_even_if_banned() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();
        let cbsd_id = "cbsd-id-1".to_string();

        let mut banned_radios = BannedRadios::default();
        banned_radios.insert_wifi(pub_key.clone());
        banned_radios.insert_cbrs(cbsd_id.clone());

        let mut unique_connections = UniqueConnectionCounts::default();
        unique_connections.insert(pub_key.clone(), MINIMUM_UNIQUE_CONNECTIONS + 1);

        let boosted_hex_eligibility = BoostedHexEligibility::new(
            VerifiedRadioThresholds::default(),
            banned_radios,
            unique_connections,
        );

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorWifi, pub_key.clone(), None);

        assert_eq!(SPBoostedRewardEligibility::Eligible, eligibility);

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorCbrs, pub_key, Some(cbsd_id));

        assert_eq!(
            SPBoostedRewardEligibility::ServiceProviderBanned,
            eligibility
        );
    }

    #[test]
    fn banned() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();
        let cbsd_id = "cbsd-id-1".to_string();

        let mut banned_radios = BannedRadios::default();
        banned_radios.insert_wifi(pub_key.clone());
        banned_radios.insert_cbrs(cbsd_id.clone());

        let boosted_hex_eligibility = BoostedHexEligibility::new(
            VerifiedRadioThresholds::default(),
            banned_radios,
            UniqueConnectionCounts::default(),
        );

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorWifi, pub_key.clone(), None);

        assert_eq!(
            SPBoostedRewardEligibility::ServiceProviderBanned,
            eligibility
        );

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorCbrs, pub_key, Some(cbsd_id));

        assert_eq!(
            SPBoostedRewardEligibility::ServiceProviderBanned,
            eligibility
        );
    }

    #[test]
    fn radio_threshold_not_met() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();
        let cbsd_id = "cbsd-id-1".to_string();

        let boosted_hex_eligibility = BoostedHexEligibility::new(
            VerifiedRadioThresholds::default(),
            BannedRadios::default(),
            UniqueConnectionCounts::default(),
        );

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorWifi, pub_key.clone(), None);

        assert_eq!(
            SPBoostedRewardEligibility::RadioThresholdNotMet,
            eligibility
        );

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorCbrs, pub_key, Some(cbsd_id));

        assert_eq!(
            SPBoostedRewardEligibility::RadioThresholdNotMet,
            eligibility
        );
    }

    #[test]
    fn eligible() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();
        let cbsd_id = "cbsd-id-1".to_string();

        let mut verified_radio_thresholds = VerifiedRadioThresholds::default();
        verified_radio_thresholds.insert(pub_key.clone(), None);
        verified_radio_thresholds.insert(pub_key.clone(), Some(cbsd_id.clone()));

        let boosted_hex_eligibility = BoostedHexEligibility::new(
            verified_radio_thresholds,
            BannedRadios::default(),
            UniqueConnectionCounts::default(),
        );

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorWifi, pub_key.clone(), None);

        assert_eq!(SPBoostedRewardEligibility::Eligible, eligibility);

        let eligibility =
            boosted_hex_eligibility.eligibility(RadioType::OutdoorCbrs, pub_key, Some(cbsd_id));

        assert_eq!(SPBoostedRewardEligibility::Eligible, eligibility);
    }

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }
}
