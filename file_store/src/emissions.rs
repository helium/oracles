use crate::Result;
use chrono::{DateTime, Datelike, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Read};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Emission {
    pub start_time: DateTime<Utc>,
    pub yearly_emissions: Decimal,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EmissionsSchedule {
    pub schedule: Vec<Emission>,
}

impl EmissionsSchedule {
    pub async fn from_file(filepath: String) -> Result<EmissionsSchedule> {
        let mut file = File::open(filepath)?;
        let mut data = String::new();
        file.read_to_string(&mut data)?;
        let emissions: Vec<Emission> = serde_json::from_str(&data)?;
        Ok(EmissionsSchedule {
            schedule: emissions,
        })
    }

    pub fn yearly_emissions(&self, datetime: DateTime<Utc>) -> Result<Decimal> {
        let mut schedule = self.schedule.clone();
        // todo: move sort to struct initialisation, having it here makes tests a lil easier for time being
        schedule.sort_by(|a, b| b.start_time.cmp(&a.start_time));
        let res = schedule
        .into_iter()
        .find(| emission | { datetime >= emission.start_time})
        .ok_or("failed to find emissions for specified date {datetime}").unwrap();
        Ok(res.yearly_emissions * Decimal::from(1_000_000))
    }

    pub fn daily_emissions(&self, datetime: DateTime<Utc>) -> Result<Decimal> {
        let cur_year = datetime.year();
        let cur_month = datetime.month();
        let num_days = if is_leap_year(cur_year, cur_month) {
            dec!(366)
        } else {
            dec!(365)
        };
        let yearly_emissions = self.yearly_emissions(datetime)?;
        Ok(yearly_emissions / num_days)
    }
}

fn is_leap_year(cur_year: i32, cur_month: u32) -> bool {
    let next_year = cur_year + 1;
    let is_current_year_a_leap = is_year_a_leap(cur_year) && cur_month < 8;
    let is_next_year_a_leap = is_year_a_leap(next_year) && cur_month >= 8;
    is_current_year_a_leap || is_next_year_a_leap
}

fn is_year_a_leap(year: i32) -> bool {
    year % 4 == 0
        && (year % 100 != 0 || (year % 100 == 0 && year % 400 == 0))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_leap_year() {

        assert_eq!(false, is_leap_year(2023, 7));
        assert!(is_leap_year(2023, 8));
        assert!(is_leap_year(2024, 7));
        assert_eq!(false, is_leap_year(2024, 8));

        assert_eq!(false, is_leap_year(2027, 7));
        assert!(is_leap_year(2027, 8));
        assert!(is_leap_year(2028, 7));
        assert_eq!(false, is_leap_year(2028, 8));
    }
}

