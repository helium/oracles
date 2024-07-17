use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// In order for the Wi-Fi access point to be eligible for boosted hex rewards
// as described in HIP84 the location trust score needs to be 0.75 or higher.
//
// [HIP-93: Add Wifi to Mobile Dao][add-wifi-aps]
//
// [add-wifi-aps]: https://github.com/helium/HIP/blob/main/0093-addition-of-wifi-aps-to-mobile-subdao.md#341-indoor-access-points-rewards
pub(crate) const MIN_WIFI_TRUST_MULTIPLIER: Decimal = dec!(0.75);

// In order for access points to be eligible for boosted Service Provider
// rewards defined in HIP-84, the asserted distances must be 50 meters or
// less than the reported location from external services for both indoor
// and outdoor Access Points.
//
// [HIP-119: Gaming Loopholes][gaming-loopholes]
//
// [gaming-loopholes]: https://github.com/helium/HIP/blob/main/0119-closing-gaming-loopholes-within-the-mobile-network.md#maximum-asserted-distance-for-boosted-hexes
pub(crate) const MAX_AVERAGE_DISTANCE: Decimal = dec!(50);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SPBoostedRewardEligibility {
    Eligible,
    /// Service Provider can invalidate boosted rewards of a hotspot
    ///
    /// [HIP-125: Anti gaming measures][anti-gaming]
    ///
    /// [anti-gaming]: https://github.com/helium/HIP/blob/main/0125-temporary-anti-gaming-measures-for-boosted-hexes.md
    ServiceProviderBanned,
    /// Radio must pass at least 1mb of data from 3 unique phones.
    ///
    /// [HIP-84: Provider Hex Boosting][provider-boosting]
    ///
    /// [provider-boosting]: https://github.com/helium/HIP/blob/main/0084-service-provider-hex-boosting.md
    RadioThresholdNotMet,
}
