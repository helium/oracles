use crate::hotspot::{Hotspot, Model};
use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use std::collections::HashMap;

const REWARDS_PER_WEEK: u64 = 336;
const GENESIS_REWARD: u64 = 640_000_000 / REWARDS_PER_WEEK;
const PRE_POC_REWARD: u64 = 1_280_000_000 / REWARDS_PER_WEEK;
const PRECISION: f64 = 100.0;

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> = Utc.ymd(2022, 7, 11).and_hms(0, 0, 0);
    static ref PRE_POC_START: DateTime<Utc> = Utc.ymd(2022, 8, 1).and_hms(0, 0, 0);
    static ref PRE_POC_END: DateTime<Utc> = Utc.ymd(2023, 7, 17).and_hms(0, 0, 0);
}

pub fn get_emissions_per_model(
    models: HashMap<Model, u64>,
    datetime: DateTime<Utc>,
) -> HashMap<Model, f64> {
    let total_rewards = get_scheduled_tokens(datetime)
        .expect("Failed to supply valid date on the emission schedule");

    let nova436h = models.get(&Model::Nova436H).unwrap_or(&0);
    let nova430i = models.get(&Model::Nova430I).unwrap_or(&0);
    let sercommos = models.get(&Model::SercommOutdoor).unwrap_or(&0);
    let sercommis = models.get(&Model::SercommIndoor).unwrap_or(&0);
    let neut430s = models.get(&Model::Neutrino430).unwrap_or(&0);

    let nova436_shares = Hotspot::NOVA436H.reward_shares(*nova436h);
    let nova430_shares = Hotspot::NOVA430I.reward_shares(*nova430i);
    let sercommo_shares = Hotspot::SERCOMMOUTDOOR.reward_shares(*sercommos);
    let sercommi_shares = Hotspot::SERCOMMINDOOR.reward_shares(*sercommis);
    let neut430_shares = Hotspot::NEUTRINO430.reward_shares(*neut430s);

    let total_weights =
        nova436_shares + nova430_shares + sercommo_shares + sercommi_shares + neut430_shares;
    let base_reward = total_rewards as f64 / total_weights;
    let nova436_rewards = if nova436h > &0 {
        Hotspot::NOVA436H.rewards(base_reward, PRECISION)
    } else {
        0.0
    };
    let nova430_rewards = if nova430i > &0 {
        Hotspot::NOVA430I.rewards(base_reward, PRECISION)
    } else {
        0.0
    };
    let sercommo_rewards = if sercommos > &0 {
        Hotspot::SERCOMMOUTDOOR.rewards(base_reward, PRECISION)
    } else {
        0.0
    };
    let sercommi_rewards = if sercommis > &0 {
        Hotspot::SERCOMMINDOOR.rewards(base_reward, PRECISION)
    } else {
        0.0
    };
    let neut430_rewards = if neut430s > &0 {
        Hotspot::NEUTRINO430.rewards(base_reward, PRECISION)
    } else {
        0.0
    };

    HashMap::from([
        (Model::Nova436H, nova436_rewards),
        (Model::Nova430I, nova430_rewards),
        (Model::SercommOutdoor, sercommo_rewards),
        (Model::SercommIndoor, sercommi_rewards),
        (Model::Neutrino430, neut430_rewards),
    ])
}

fn get_scheduled_tokens(datetime: DateTime<Utc>) -> Option<u64> {
    if *GENESIS_START < datetime && datetime < *PRE_POC_START {
        Some(GENESIS_REWARD)
    } else if *PRE_POC_START < datetime && datetime < *PRE_POC_END {
        Some(PRE_POC_REWARD)
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
            (Model::SercommOutdoor, 30557.66),
            (Model::Nova430I, 30557.66),
            (Model::Nova436H, 40743.55),
            (Model::SercommIndoor, 20371.77),
            (Model::Neutrino430, 20371.77),
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

    #[test]
    fn post_genesis_reward() {
        let expected = HashMap::from([
            (Model::SercommOutdoor, 61115.34),
            (Model::Nova430I, 61115.34),
            (Model::Nova436H, 81487.12),
            (Model::SercommIndoor, 40743.56),
            (Model::Neutrino430, 40743.56),
        ]);
        let date = Utc.ymd(2023, 1, 1).and_hms(0, 0, 0);
        let input = HashMap::from([
            (Model::SercommOutdoor, 20),
            (Model::Nova430I, 15),
            (Model::Nova436H, 10),
            (Model::SercommIndoor, 13),
            (Model::Neutrino430, 8),
        ]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }

    #[test]
    fn no_reporting_model_reward() {
        let expected = HashMap::from([
            (Model::SercommOutdoor, 38872.67),
            (Model::Nova430I, 38872.67),
            (Model::Nova436H, 0.0),
            (Model::SercommIndoor, 25915.11),
            (Model::Neutrino430, 25915.11),
        ]);
        let date = Utc.ymd(2022, 7, 17).and_hms(0, 0, 0);
        let input = HashMap::from([
            (Model::SercommOutdoor, 20),
            (Model::Nova430I, 15),
            (Model::SercommIndoor, 13),
            (Model::Neutrino430, 8),
        ]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }
}
