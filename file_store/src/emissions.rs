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

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct EmissionsSchedule {
    pub schedule: Vec<Emission>,
}

impl EmissionsSchedule {
    pub fn default() -> Result<Self> {
        let data = schedule();
        let emissions: Vec<Emission> = serde_json::from_str(&data)?;
        Ok(EmissionsSchedule {
            schedule: emissions,
        })
    }

    pub async fn from_file(filepath: String) -> Result<Self> {
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
            .find(|emission| datetime >= emission.start_time)
            .ok_or("failed to find emissions for specified date {datetime}")
            .unwrap();
        Ok(res.yearly_emissions * Decimal::from(1_000_000))
    }

    pub fn daily_emissions(&self, datetime: DateTime<Utc>) -> Result<Decimal> {
        let cur_year = datetime.year();
        let cur_month = datetime.month();
        let num_days = num_days_this_period(cur_year, cur_month);
        let yearly_emissions = self.yearly_emissions(datetime)?;
        Ok(yearly_emissions / num_days)
    }
}

fn num_days_this_period(year: i32, month: u32) -> Decimal {
    if is_leap_epoch(year, month) {
        dec!(366)
    } else {
        dec!(365)
    }
}

fn is_leap_epoch(cur_year: i32, cur_month: u32) -> bool {
    let next_year = cur_year + 1;
    let is_current_year_a_leap = is_year_a_leap(cur_year) && cur_month < 8;
    let is_next_year_a_leap = is_year_a_leap(next_year) && cur_month >= 8;
    is_current_year_a_leap || is_next_year_a_leap
}

fn is_year_a_leap(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || (year % 100 == 0 && year % 400 == 0))
}

fn schedule() -> String {
    "[{
        \"start_time\": \"2022-08-01T00:00:01Z\",
        \"yearly_emissions\": 65000000000
      },
      {
        \"start_time\": \"2023-08-01T00:00:01Z\",
        \"yearly_emissions\": 32500000000
      },
      {
          \"start_time\": \"2024-08-01T00:00:01Z\",
          \"yearly_emissions\": 16250000000
      }]"
    .to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_leap_year() {
        assert_eq!(false, is_leap_epoch(2023, 7));
        assert!(is_leap_epoch(2023, 8));
        assert!(is_leap_epoch(2024, 7));
        assert_eq!(false, is_leap_epoch(2024, 8));

        assert_eq!(false, is_leap_epoch(2027, 7));
        assert!(is_leap_epoch(2027, 8));
        assert!(is_leap_epoch(2028, 7));
        assert_eq!(false, is_leap_epoch(2028, 8));
    }
}
