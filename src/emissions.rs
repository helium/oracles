use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;

const REWARDS_PER_WEEK: u64 = 336;
const SCHEDULE: [((i32, u32, u32), (i32, u32, u32), u64); 3] =
    [
        ((2022, 7, 7), (2022, 7, 28), 640_000_000 / REWARDS_PER_WEEK),
        ((2022, 7, 28), (2022, 8, 4), 920_000_000 / REWARDS_PER_WEEK),
        ((2022, 8, 4), (2023, 7, 13), 1_280_000_000 / REWARDS_PER_WEEK),
    ];
const FF436WT: f64 = 2.0;
const FF430WT: f64 = 1.5;
const FFSERWT: f64 = 1.0;
const PRECISION: f64 = 100.0;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum Model {
    FF436,
    FF430,
    FFSercomm,
}

pub fn get_emissions_per_model(models: HashMap<Model, u64>, datetime: DateTime<Utc>) -> HashMap<Model, f64> {
    let total_rewards = get_scheduled_tokens(datetime).expect("Failed to supply valid date on the emission schedule");
    let ff436s = models.get(&Model::FF436).unwrap_or_else(|| &0);
    let ff430s = models.get(&Model::FF430).unwrap_or_else(|| &0);
    let ffsercomms = models.get(&Model::FFSercomm).unwrap_or_else(|| &0);
    let ff436_shares = *ff436s as f64 * FF436WT;
    let ff430_shares = *ff430s as f64 * FF430WT;
    let ffser_shares = *ffsercomms as f64 * FFSERWT;
    let total_weights = ff436_shares + ff430_shares + ffser_shares;
    let base_reward = total_rewards as f64 / total_weights;
    HashMap::from([
        (Model::FF436, f64::trunc((base_reward * FF436WT) * PRECISION) / PRECISION),
        (Model::FF430, f64::trunc((base_reward * FF430WT) * PRECISION) / PRECISION),
        (Model::FFSercomm, f64::trunc((base_reward * FFSERWT) * PRECISION) / PRECISION),
        // In case we need to fully truncate the rewards and not denominate tokens in decimals
        // (Model::FF436, (base * FF436WT).round() as f64),
        // (Model::FF430, (base * FF430WT).round() as f64),
        // (Model::FFSercomm, (base * FFSERWT).round() as f64),
    ])
}

fn get_scheduled_tokens(datetime: DateTime<Utc>) -> Option<u64> {
    for ((start_y, start_m, start_d), (end_y, end_m, end_d), val) in SCHEDULE.iter() {
        let start_date = Utc.ymd(*start_y, *start_m, *start_d).and_hms(0, 0, 0);
        let end_date = Utc.ymd(*end_y, *end_m, *end_d).and_hms(0, 0, 0);
        if start_date < datetime && datetime < end_date {
            return Some(*val);
        }
    }
    return None
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn genesis_reward() {
        let expected = HashMap::from([(Model::FFSercomm, 30476.17), (Model::FF430, 45714.26), (Model::FF436, 60952.35)]);
        let date = Utc.ymd(2022, 7, 17).and_hms(0, 0, 0);
        let input = HashMap::from([(Model::FFSercomm, 20), (Model::FF430, 15), (Model::FF436, 10)]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }

    #[test]
    fn transition_reward() {
        let expected = HashMap::from([(Model::FFSercomm, 43809.52), (Model::FF430, 65714.28), (Model::FF436, 87619.04)]);
        let date = Utc.ymd(2022, 8, 1).and_hms(0, 0, 0);
        let input = HashMap::from([(Model::FFSercomm, 20), (Model::FF430, 15), (Model::FF436, 10)]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }

    #[test]
    fn poc_5g_start_reward() {
        let expected = HashMap::from([(Model::FFSercomm, 60952.36), (Model::FF430, 91428.55), (Model::FF436, 121904.73)]);
        let date = Utc.ymd(2023, 1, 1).and_hms(0, 0, 0);
        let input = HashMap::from([(Model::FFSercomm, 20), (Model::FF430, 15), (Model::FF436, 10)]);
        assert_eq!(expected, get_emissions_per_model(input, date))
    }

    // fn no_436s_reward() {
    //     let expected = HashMap::from([(Model::FFSercomm, ), (Model::FF430, ), (Model::FF436, )]);
    //     let date = Utc.ymd(2022, 7, 17).and_hms(0, 0, 0);
    //     let input = HashMap::from([(Model::FFSercomm, 20), (Model::FF430, 15), (Model::FF436, 0)]);
    //     assert_eq!(expected, get_emissions_per_model(input, date))
    // }
}
