use crate::cell_type::CellType;
use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

// 10 ^ 8
const BONES: u64 = 100_000_000;
// For rounding up rewards only _once_ after calculation is done
const PRECISION: u32 = 2;
// 100M genesis rewards per day
const GENESIS_REWARD_PER_DAY: u64 = 100 * 1_000_000 * BONES;

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> = Utc.ymd(2022, 7, 11).and_hms(0, 0, 0);
}

pub fn get_emissions_per_model(
    models: HashMap<CellType, u64>,
    datetime: DateTime<Utc>,
) -> HashMap<CellType, Decimal> {
    let total_rewards = get_scheduled_tokens(datetime)
        .expect("Failed to supply valid date on the emission schedule");

    let nova436h_units = models.get(&CellType::Nova436H).unwrap_or(&0);
    let nova430i_units = models.get(&CellType::Nova430I).unwrap_or(&0);
    let sercommo_units = models.get(&CellType::SercommOutdoor).unwrap_or(&0);
    let sercommi_units = models.get(&CellType::SercommIndoor).unwrap_or(&0);
    let neut430_units = models.get(&CellType::Neutrino430).unwrap_or(&0);

    let nova436h_shares = CellType::Nova436H.reward_shares(*nova436h_units);
    let nova430i_shares = CellType::Nova430I.reward_shares(*nova430i_units);
    let sercommo_shares = CellType::SercommOutdoor.reward_shares(*sercommo_units);
    let sercommi_shares = CellType::SercommIndoor.reward_shares(*sercommi_units);
    let neut430_shares = CellType::Neutrino430.reward_shares(*neut430_units);

    let total_shares =
        nova436h_shares + nova430i_shares + sercommo_shares + sercommi_shares + neut430_shares;
    let base_reward = Decimal::from(total_rewards) / total_shares;

    let nova436h_rewards = calc_rewards(CellType::Nova436H, base_reward, *nova436h_units);
    let nova430i_rewards = calc_rewards(CellType::Nova430I, base_reward, *nova430i_units);
    let sercommo_rewards = calc_rewards(CellType::SercommOutdoor, base_reward, *sercommo_units);
    let sercommi_rewards = calc_rewards(CellType::SercommIndoor, base_reward, *sercommi_units);
    let neut430_rewards = calc_rewards(CellType::Neutrino430, base_reward, *neut430_units);

    HashMap::from([
        (CellType::Nova436H, nova436h_rewards),
        (CellType::Nova430I, nova430i_rewards),
        (CellType::SercommOutdoor, sercommo_rewards),
        (CellType::SercommIndoor, sercommi_rewards),
        (CellType::Neutrino430, neut430_rewards),
    ])
}

fn calc_rewards(hotspot: CellType, base_reward: Decimal, num_units: u64) -> Decimal {
    if num_units > 0 {
        let rewards = hotspot.rewards(base_reward) * Decimal::from(num_units);
        rewards.round_dp(PRECISION)
    } else {
        dec!(0)
    }
}

fn get_scheduled_tokens(datetime: DateTime<Utc>) -> Option<u64> {
    if *GENESIS_START < datetime {
        Some(GENESIS_REWARD_PER_DAY)
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn genesis_reward() {
        let expected = HashMap::from([
            (CellType::SercommOutdoor, dec!(3208556149732620.32)),
            (CellType::Nova430I, dec!(2406417112299465.24)),
            (CellType::Nova436H, dec!(2139037433155080.21)),
            (CellType::SercommIndoor, dec!(1390374331550802.14)),
            (CellType::Neutrino430, dec!(855614973262032.09)),
        ]);
        let date = Utc.ymd(2022, 7, 17).and_hms(0, 0, 0);
        let input = HashMap::from([
            (CellType::SercommOutdoor, 20),
            (CellType::Nova430I, 15),
            (CellType::Nova436H, 10),
            (CellType::SercommIndoor, 13),
            (CellType::Neutrino430, 8),
        ]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }

    // #[test]
    // fn post_genesis_reward() {
    //     let expected = HashMap::from([
    //         (CellModel::SercommOutdoor, 6111534 * BONES),
    //         (CellModel::Nova430I, 6111534 * BONES),
    //         (CellModel::Nova436H, 8148712 * BONES),
    //         (CellModel::SercommIndoor, 4074356 * BONES),
    //         (CellModel::Neutrino430, 4074356 * BONES),
    //     ]);
    //     let date = Utc.ymd(2023, 1, 1).and_hms(0, 0, 0);
    //     let input = HashMap::from([
    //         (CellModel::SercommOutdoor, 20),
    //         (CellModel::Nova430I, 15),
    //         (CellModel::Nova436H, 10),
    //         (CellModel::SercommIndoor, 13),
    //         (CellModel::Neutrino430, 8),
    //     ]);
    //     assert_eq!(expected, get_emissions_per_model(input, date))
    // }

    #[test]
    fn no_reporting_model_reward() {
        let expected = HashMap::from([
            (CellType::SercommOutdoor, dec!(4081632653061224.49)),
            (CellType::Nova430I, dec!(3061224489795918.37)),
            (CellType::Nova436H, dec!(0)),
            (CellType::SercommIndoor, dec!(1768707482993197.28)),
            (CellType::Neutrino430, dec!(1088435374149659.86)),
        ]);
        let date = Utc.ymd(2022, 7, 17).and_hms(0, 0, 0);
        let input = HashMap::from([
            (CellType::SercommOutdoor, 20),
            (CellType::Nova430I, 15),
            (CellType::SercommIndoor, 13),
            (CellType::Neutrino430, 8),
        ]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }
}
