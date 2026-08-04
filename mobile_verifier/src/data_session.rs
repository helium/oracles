use helium_crypto::PublicKeyBinary;
use std::collections::HashMap;

use crate::reward_shares::data_transfer;

/// Per-hotspot rewardable data-transfer totals — the aggregated *input* to data
/// transfer rewards, not a reward itself.
///
/// Populated by the Trino reader in [`crate::iceberg::burned_session`] and
/// consumed by [`crate::rewarder::reward_dc`].
#[derive(Default, PartialEq, Eq)]
pub struct RewardableData {
    pub rewardable_bytes: u64,
    pub rewardable_dc: u64,
}

#[derive(Default)]
pub struct RewardableDataByHotspot(HashMap<PublicKeyBinary, RewardableData>);

impl RewardableDataByHotspot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_gw_data_transfer(self) -> Vec<data_transfer::GatewayDataTransfer<PublicKeyBinary>> {
        self.0
            .into_iter()
            .map(|(hotspot_key, r)| data_transfer::GatewayDataTransfer {
                hotspot_key,
                rewardable_dc: r.rewardable_dc,
                rewardable_bytes: r.rewardable_bytes,
            })
            .collect()
    }

    /// Total data credits burned across all hotspots. The demand metric scales
    /// this by the HNT price (see `rewarder::reward_dc`); the price isn't needed
    /// here — `demand = dc_to_hnt_bones(total_dc)` since the conversion is linear.
    pub fn total_dc(&self) -> u64 {
        self.values().map(|r| r.rewardable_dc).sum()
    }

    pub fn total_bytes(&self) -> u64 {
        self.0.values().map(|r| r.rewardable_bytes).sum()
    }
}

impl std::ops::Deref for RewardableDataByHotspot {
    type Target = HashMap<PublicKeyBinary, RewardableData>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for RewardableDataByHotspot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromIterator<(PublicKeyBinary, RewardableData)> for RewardableDataByHotspot {
    fn from_iter<T: IntoIterator<Item = (PublicKeyBinary, RewardableData)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for RewardableDataByHotspot {
    type Item = (PublicKeyBinary, RewardableData);
    type IntoIter = std::collections::hash_map::IntoIter<PublicKeyBinary, RewardableData>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
