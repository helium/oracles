use crate::hotspot::{Hotspot, Model};
use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use std::collections::HashMap;

const BONES: u64 = 100_000_000;
const REWARD_WT_MULTIPLIER: u64 = 10;
const GENESIS_REWARD: u64 = 100 * 1_000_000 * BONES;

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> = Utc.ymd(2022, 7, 11).and_hms(0, 0, 0);
}

pub fn get_emissions_per_model(
    models: HashMap<Model, u64>,
    datetime: DateTime<Utc>,
) -> HashMap<Model, u64> {
    let total_rewards = get_scheduled_tokens(datetime)
        .expect("Failed to supply valid date on the emission schedule");

    let nova436h_units = models.get(&Model::Nova436H).unwrap_or(&0);
    let nova430i_units = models.get(&Model::Nova430I).unwrap_or(&0);
    let sercommo_units = models.get(&Model::SercommOutdoor).unwrap_or(&0);
    let sercommi_units = models.get(&Model::SercommIndoor).unwrap_or(&0);
    let neut430_units = models.get(&Model::Neutrino430).unwrap_or(&0);

    let nova436h_shares = Hotspot::NOVA436H.reward_shares(*nova436h_units);
    let nova430i_shares = Hotspot::NOVA430I.reward_shares(*nova430i_units);
    let sercommo_shares = Hotspot::SERCOMMOUTDOOR.reward_shares(*sercommo_units);
    let sercommi_shares = Hotspot::SERCOMMINDOOR.reward_shares(*sercommi_units);
    let neut430_shares = Hotspot::NEUTRINO430.reward_shares(*neut430_units);

    let total_shares =
        nova436h_shares + nova430i_shares + sercommo_shares + sercommi_shares + neut430_shares;
    let base_reward = total_rewards * REWARD_WT_MULTIPLIER / total_shares;

    let nova436h_rewards = calc_rewards(Hotspot::NOVA436H, base_reward, *nova436h_units);
    let nova430i_rewards = calc_rewards(Hotspot::NOVA430I, base_reward, *nova430i_units);
    let sercommo_rewards = calc_rewards(Hotspot::SERCOMMOUTDOOR, base_reward, *sercommo_units);
    let sercommi_rewards = calc_rewards(Hotspot::SERCOMMINDOOR, base_reward, *sercommi_units);
    let neut430_rewards = calc_rewards(Hotspot::NEUTRINO430, base_reward, *neut430_units);

    HashMap::from([
        (Model::Nova436H, nova436h_rewards),
        (Model::Nova430I, nova430i_rewards),
        (Model::SercommOutdoor, sercommo_rewards),
        (Model::SercommIndoor, sercommi_rewards),
        (Model::Neutrino430, neut430_rewards),
    ])
}

fn calc_rewards(hotspot: Hotspot, base_reward: u64, num_units: u64) -> u64 {
    if num_units > 0 {
        hotspot.rewards(base_reward) * num_units
    } else {
        0
    }
}

fn get_scheduled_tokens(datetime: DateTime<Utc>) -> Option<u64> {
    if *GENESIS_START < datetime {
        Some(GENESIS_REWARD)
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
            (Model::SercommOutdoor, 32085561497326200),
            (Model::Nova430I, 24064171122994650),
            (Model::Nova436H, 21390374331550800),
            (Model::SercommIndoor, 13903743315508020),
            (Model::Neutrino430, 8556149732620320),
        ]);
        let date = Utc.ymd(2022, 7, 17).and_hms(0, 0, 0);
        let input = HashMap::from([
            (Model::SercommOutdoor, 20),
            (Model::Nova430I, 15),
            (Model::Nova436H, 10),
            (Model::SercommIndoor, 13),
            (Model::Neutrino430, 8),
        ]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }

    // #[test]
    // fn post_genesis_reward() {
    //     let expected = HashMap::from([
    //         (Model::SercommOutdoor, 6111534 * BONES),
    //         (Model::Nova430I, 6111534 * BONES),
    //         (Model::Nova436H, 8148712 * BONES),
    //         (Model::SercommIndoor, 4074356 * BONES),
    //         (Model::Neutrino430, 4074356 * BONES),
    //     ]);
    //     let date = Utc.ymd(2023, 1, 1).and_hms(0, 0, 0);
    //     let input = HashMap::from([
    //         (Model::SercommOutdoor, 20),
    //         (Model::Nova430I, 15),
    //         (Model::Nova436H, 10),
    //         (Model::SercommIndoor, 13),
    //         (Model::Neutrino430, 8),
    //     ]);
    //     assert_eq!(expected, get_emissions_per_model(input, date))
    // }

    // #[test]
    // fn no_reporting_model_reward() {
    //     let expected = HashMap::from([
    //         (Model::SercommOutdoor, 3887267 * BONES),
    //         (Model::Nova430I, 3887267 * BONES),
    //         (Model::Nova436H, 0),
    //         (Model::SercommIndoor, 2591511 * BONES),
    //         (Model::Neutrino430, 2591511 * BONES),
    //     ]);
    //     let date = Utc.ymd(2022, 7, 17).and_hms(0, 0, 0);
    //     let input = HashMap::from([
    //         (Model::SercommOutdoor, 20),
    //         (Model::Nova430I, 15),
    //         (Model::SercommIndoor, 13),
    //         (Model::Neutrino430, 8),
    //     ]);
    //     assert_eq!(expected, get_emissions_per_model(input, date))
    // }
}
