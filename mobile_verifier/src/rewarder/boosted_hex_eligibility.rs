use std::collections::HashSet;

use coverage_point_calculator::ServiceProviderBoostedRewardEligibility;
use helium_crypto::PublicKeyBinary;

use crate::radio_threshold::VerifiedRadioThresholds;

#[derive(Debug, Default)]
pub struct BoostedHexEligibility {
    radio_thresholds: VerifiedRadioThresholds,
    sp_bans: HashSet<(Option<PublicKeyBinary>, Option<String>)>,
}

impl BoostedHexEligibility {
    pub fn new(
        radio_thresholds: VerifiedRadioThresholds,
        sp_bans: HashSet<(Option<PublicKeyBinary>, Option<String>)>,
    ) -> Self {
        Self {
            radio_thresholds,
            sp_bans,
        }
    }

    pub fn eligibility(
        &self,
        key: PublicKeyBinary,
        cbsd_id_opt: Option<String>,
    ) -> ServiceProviderBoostedRewardEligibility {
        let ban_key = match cbsd_id_opt.clone() {
            Some(cbsd_id) => (None, Some(cbsd_id)),
            None => (Some(key.clone()), None),
        };

        if self.sp_bans.contains(&ban_key) {
            ServiceProviderBoostedRewardEligibility::ServiceProviderBanned
        } else if self.radio_thresholds.is_verified(key, cbsd_id_opt) {
            ServiceProviderBoostedRewardEligibility::Eligible
        } else {
            ServiceProviderBoostedRewardEligibility::RadioThresholdNotMet
        }
    }
}
