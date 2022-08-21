use crate::{cell_type::CellType, decimal_scalar::Mobile};
use chrono::{DateTime, Duration, TimeZone, Utc};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

// 100M genesis rewards per day
const GENESIS_REWARDS_PER_DAY: i64 = 100_000_000;

pub type Model = HashMap<CellType, u64>;
pub type Emission = HashMap<CellType, Mobile>;

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> = Utc.ymd(2022, 7, 11).and_hms(0, 0, 0);
}

pub fn get_emissions_per_model(
    model: &Model,
    datetime: DateTime<Utc>,
    duration: Duration,
) -> Option<Emission> {
    let total_rewards = get_scheduled_tokens(datetime, duration)
        .expect("Failed to supply valid date on the emission schedule");

    let nova436h_units = model.get(&CellType::Nova436H).unwrap_or(&0);
    let nova430i_units = model.get(&CellType::Nova430I).unwrap_or(&0);
    let sercommo_units = model.get(&CellType::SercommOutdoor).unwrap_or(&0);
    let sercommi_units = model.get(&CellType::SercommIndoor).unwrap_or(&0);
    let neut430_units = model.get(&CellType::Neutrino430).unwrap_or(&0);

    let nova436h_shares = CellType::Nova436H.reward_shares(*nova436h_units);
    let nova430i_shares = CellType::Nova430I.reward_shares(*nova430i_units);
    let sercommo_shares = CellType::SercommOutdoor.reward_shares(*sercommo_units);
    let sercommi_shares = CellType::SercommIndoor.reward_shares(*sercommi_units);
    let neut430_shares = CellType::Neutrino430.reward_shares(*neut430_units);

    let total_shares =
        nova436h_shares + nova430i_shares + sercommo_shares + sercommi_shares + neut430_shares;

    if total_shares > dec!(0) {
        let base_reward = total_rewards / total_shares;

        let nova436h_rewards = calc_rewards(CellType::Nova436H, base_reward, *nova436h_units);
        let nova430i_rewards = calc_rewards(CellType::Nova430I, base_reward, *nova430i_units);
        let sercommo_rewards = calc_rewards(CellType::SercommOutdoor, base_reward, *sercommo_units);
        let sercommi_rewards = calc_rewards(CellType::SercommIndoor, base_reward, *sercommi_units);
        let neut430_rewards = calc_rewards(CellType::Neutrino430, base_reward, *neut430_units);

        return Some(HashMap::from([
            (CellType::Nova436H, nova436h_rewards),
            (CellType::Nova430I, nova430i_rewards),
            (CellType::SercommOutdoor, sercommo_rewards),
            (CellType::SercommIndoor, sercommi_rewards),
            (CellType::Neutrino430, neut430_rewards),
        ]));
    }
    None
}

fn calc_rewards(cell_type: CellType, base_reward: Decimal, num_units: u64) -> Mobile {
    if num_units > 0 {
        Mobile::from(cell_type.rewards(base_reward) * Decimal::from(num_units))
    } else {
        Mobile::from(0)
    }
}

fn get_scheduled_tokens(start: DateTime<Utc>, duration: Duration) -> Option<Decimal> {
    if *GENESIS_START <= start {
        // Get tokens from start - duration
        Some(
            (Decimal::from(GENESIS_REWARDS_PER_DAY)
                / Decimal::from(Duration::hours(24).num_seconds()))
                * Decimal::from(duration.num_seconds()),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn genesis_reward() {
        let expected = HashMap::from([
            (
                CellType::SercommOutdoor,
                Mobile::from(dec!(197628.45849802)),
            ),
            (CellType::Nova430I, Mobile::from(dec!(26284584.98023715))),
            (CellType::Nova436H, Mobile::from(dec!(316205.53359684))),
            (
                CellType::SercommIndoor,
                Mobile::from(dec!(73043478.26086957)),
            ),
            (CellType::Neutrino430, Mobile::from(dec!(158102.76679842))),
        ]);
        let input = HashMap::from([
            (CellType::SercommOutdoor, 1),
            (CellType::Nova430I, 133),
            (CellType::Nova436H, 1),
            (CellType::SercommIndoor, 924),
            (CellType::Neutrino430, 2),
        ]);
        let output = get_emissions_per_model(&input, *GENESIS_START, Duration::hours(24)).unwrap();
        assert_eq!(expected, output);
    }

    #[test]
    fn emissions_per_second() {
        // NOTE: The number of cells does not matter.
        let input = HashMap::from([
            (CellType::SercommOutdoor, 1),
            (CellType::Nova430I, 133),
            (CellType::Nova436H, 1),
            (CellType::SercommIndoor, 924),
            (CellType::Neutrino430, 2),
        ]);
        let output = get_emissions_per_model(&input, *GENESIS_START, Duration::seconds(1))
            .expect("unable to get emissions");

        // The emission per second is ALWAYS 1157.40740741 (in MOBILE)
        let expected = Mobile::from(dec!(1157.40740741));

        let sum: Mobile = output
            .values()
            .fold(Decimal::from(0), |acc, val| acc + val.get_decimal())
            .into();

        assert_eq!(expected, sum);
    }

    #[test]
    fn no_reporting_model_reward() {
        let expected = HashMap::from([
            (
                CellType::SercommOutdoor,
                Mobile::from(dec!(198255.35289453)),
            ),
            (CellType::Nova430I, Mobile::from(dec!(26367961.93497224))),
            (CellType::Nova436H, Mobile::from(0)),
            (
                CellType::SercommIndoor,
                Mobile::from(dec!(73275178.42981761)),
            ),
            (CellType::Neutrino430, Mobile::from(dec!(158604.28231562))),
        ]);
        let input = HashMap::from([
            (CellType::SercommOutdoor, 1),
            (CellType::Nova430I, 133),
            (CellType::SercommIndoor, 924),
            (CellType::Neutrino430, 2),
        ]);
        let output = get_emissions_per_model(&input, *GENESIS_START, Duration::hours(24)).unwrap();
        assert_eq!(expected, output)
    }
}
