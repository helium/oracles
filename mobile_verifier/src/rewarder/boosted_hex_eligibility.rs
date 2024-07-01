use coverage_point_calculator::ServiceProviderBoostedRewardEligibility;
use helium_crypto::PublicKeyBinary;

use crate::{radio_threshold::VerifiedRadioThresholds, sp_boosted_rewards_bans::BannedRadios};

#[derive(Debug, Default)]
pub struct BoostedHexEligibility {
    radio_thresholds: VerifiedRadioThresholds,
    banned_radios: BannedRadios,
}

impl BoostedHexEligibility {
    pub fn new(radio_thresholds: VerifiedRadioThresholds, banned_radios: BannedRadios) -> Self {
        Self {
            radio_thresholds,
            banned_radios,
        }
    }

    pub fn eligibility(
        &self,
        key: PublicKeyBinary,
        cbsd_id_opt: Option<String>,
    ) -> ServiceProviderBoostedRewardEligibility {
        if self.banned_radios.contains(&key, cbsd_id_opt.as_deref()) {
            ServiceProviderBoostedRewardEligibility::ServiceProviderBanned
        } else if self.radio_thresholds.is_verified(key, cbsd_id_opt) {
            ServiceProviderBoostedRewardEligibility::Eligible
        } else {
            ServiceProviderBoostedRewardEligibility::RadioThresholdNotMet
        }
    }
}

#[cfg(test)]
mod tests {
    use helium_crypto::{KeyTag, Keypair};
    use rand::rngs::OsRng;

    use super::*;

    #[test]
    fn banned() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();
        let cbsd_id = "cbsd-id-1".to_string();

        let mut banned_radios = BannedRadios::default();
        banned_radios.insert_wifi(pub_key.clone());
        banned_radios.insert_cbrs(cbsd_id.clone());

        let boosted_hex_eligibility =
            BoostedHexEligibility::new(VerifiedRadioThresholds::default(), banned_radios);

        let eligibility = boosted_hex_eligibility.eligibility(pub_key.clone(), None);

        assert_eq!(
            ServiceProviderBoostedRewardEligibility::ServiceProviderBanned,
            eligibility
        );

        let eligibility = boosted_hex_eligibility.eligibility(pub_key, Some(cbsd_id));

        assert_eq!(
            ServiceProviderBoostedRewardEligibility::ServiceProviderBanned,
            eligibility
        );
    }

    #[test]
    fn radio_threshold_not_met() {
        let keypair = generate_keypair();

        let pub_key: PublicKeyBinary = keypair.public_key().to_vec().into();
        let cbsd_id = "cbsd-id-1".to_string();

        let boosted_hex_eligibility =
            BoostedHexEligibility::new(VerifiedRadioThresholds::default(), BannedRadios::default());

        let eligibility = boosted_hex_eligibility.eligibility(pub_key.clone(), None);

        assert_eq!(
            ServiceProviderBoostedRewardEligibility::RadioThresholdNotMet,
            eligibility
        );

        let eligibility = boosted_hex_eligibility.eligibility(pub_key, Some(cbsd_id));

        assert_eq!(
            ServiceProviderBoostedRewardEligibility::RadioThresholdNotMet,
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

        let boosted_hex_eligibility =
            BoostedHexEligibility::new(verified_radio_thresholds, BannedRadios::default());

        let eligibility = boosted_hex_eligibility.eligibility(pub_key.clone(), None);

        assert_eq!(
            ServiceProviderBoostedRewardEligibility::Eligible,
            eligibility
        );

        let eligibility = boosted_hex_eligibility.eligibility(pub_key, Some(cbsd_id));

        assert_eq!(
            ServiceProviderBoostedRewardEligibility::Eligible,
            eligibility
        );
    }

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }
}
