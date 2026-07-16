use rust_decimal::{prelude::*, RoundingStrategy};
use rust_decimal_macros::dec;
use strum_macros::{Display, EnumString};

pub mod data_transfer;
pub mod emissions_split;

pub use emissions_split::hip_149_reward_pools;

/// The fixed price of a mobile data credit
const DC_USD_PRICE: Decimal = dec!(0.00001);

/// Default precision used for rounding
pub const DEFAULT_PREC: u32 = 15;

// Percent of total emissions allocated for service provider rewards
const SERVICE_PROVIDER_PERCENT: Decimal = dec!(0.24);

// Fixed price of service provider rewards to be given to Helium Mobile Service Rewards
pub const HELIUM_MOBILE_SERVICE_REWARD_BONES: u64 = 45_000_000_000;

/// Returns the equivalent amount of Hnt bones for a specified amount of Data Credits
pub fn dc_to_hnt_bones(dc_amount: Decimal, hnt_bone_price: Decimal) -> Decimal {
    let dc_in_usd = dc_amount * DC_USD_PRICE;
    (dc_in_usd / hnt_bone_price)
        .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::ToPositiveInfinity)
}

/// Floor a non-negative `Decimal` to `u64` bones using the reward rounding
/// convention (`ToZero`). Shared by every reward pool so percentage splits always
/// round the same direction — never up, which is what lets the residual data
/// pool absorb the dropped sub-bone without over-allocating. Values above
/// `u64::MAX` saturate, which never happens for a real reward pool.
pub(crate) fn floor_to_u64(value: Decimal) -> u64 {
    value
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
}

#[derive(Display, EnumString)]
pub enum RewardableEntityKey {
    #[strum(serialize = "Helium Mobile")]
    Network,
    #[strum(serialize = "Helium Mobile Service Rewards")]
    Subscriber,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::PriceInfo;
    use solana::Token;

    #[test]
    fn ensure_correct_conversion_of_bytes_to_bones() {
        assert_eq!(dc_to_hnt_bones(Decimal::from(1), dec!(1.0)), dec!(0.00001));
        assert_eq!(dc_to_hnt_bones(Decimal::from(2), dec!(1.0)), dec!(0.00002));
    }

    #[test]
    fn test_price_conversion() {
        let token = Token::Hnt;
        let hnt_dollar_price = dec!(1.0);
        let hnt_price_from_pricer = 100000000_u64;
        let hnt_dollar_bone_price = dec!(0.00000001);

        let hnt_price = PriceInfo::new(hnt_price_from_pricer, token.decimals());

        assert_eq!(hnt_dollar_bone_price, hnt_price.price_per_bone);
        assert_eq!(hnt_price_from_pricer, hnt_price.price_in_bones);
        assert_eq!(hnt_dollar_price, hnt_price.price_per_token);
    }
}
