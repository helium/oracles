use chrono::{DateTime, Duration, Utc};
use std::ops::Range;

#[derive(Debug)]
pub struct Scheduler {
    pub reward_period_length: Duration,
    pub reward_period: Range<DateTime<Utc>>,
    pub reward_offset: Duration,
}

#[derive(thiserror::Error, Debug)]
#[error("sleep duration cannot be converted to an std::time::Duration")]
pub struct OutOfRangeError;

impl Scheduler {
    pub fn new(
        reward_period_length: Duration,
        last_rewarded_end_time: DateTime<Utc>,
        next_rewarded_end_time: DateTime<Utc>,
        reward_offset: Duration,
    ) -> Self {
        Self {
            reward_period_length,
            reward_period: last_rewarded_end_time..next_rewarded_end_time,
            reward_offset,
        }
    }

    pub fn should_reward(&self, now: DateTime<Utc>) -> bool {
        now >= self.reward_period.end + self.reward_offset
    }

    pub fn next_reward_period(&self) -> Range<DateTime<Utc>> {
        self.reward_period.end..(self.reward_period.end + self.reward_period_length)
    }

    pub fn sleep_duration(
        &self,
        now: DateTime<Utc>,
    ) -> Result<std::time::Duration, OutOfRangeError> {
        let next_reward_period = self.next_reward_period();

        let duration = if self.reward_period.end + self.reward_offset > now {
            self.reward_period.end + self.reward_offset - now
        } else if next_reward_period.end + self.reward_offset <= now {
            Duration::zero()
        } else {
            (next_reward_period.end + self.reward_offset) - now
        };

        duration.to_std().map_err(|_| OutOfRangeError)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn dt(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.ymd(y, m, d).and_hms(h, min, s)
    }

    fn reward_period_length() -> Duration {
        Duration::hours(24)
    }

    fn standard_duration(minutes: i64) -> Result<std::time::Duration, OutOfRangeError> {
        Duration::minutes(minutes)
            .to_std()
            .map_err(|_| OutOfRangeError)
    }

    #[test]
    fn boot_mid_period_with_no_reward() {
        let scheduler = Scheduler::new(
            reward_period_length(),
            dt(2022, 12, 1, 0, 0, 0),
            dt(2022, 12, 2, 0, 0, 0),
            Duration::minutes(30),
        );

        let now = dt(2022, 12, 1, 1, 0, 0);

        assert!(!scheduler.should_reward(now));
        assert_eq!(
            standard_duration(1410).unwrap(),
            scheduler
                .sleep_duration(now)
                .expect("failed sleep duration check")
        );
    }

    #[test]
    fn reward_after_period() {
        let scheduler = Scheduler::new(
            reward_period_length(),
            dt(2022, 12, 1, 0, 0, 0),
            dt(2022, 12, 2, 0, 0, 0),
            Duration::minutes(30),
        );

        let now = dt(2022, 12, 2, 0, 30, 0);

        assert_eq!(
            dt(2022, 12, 1, 0, 0, 0)..dt(2022, 12, 2, 0, 0, 0),
            scheduler.reward_period
        );
        assert!(scheduler.should_reward(now));
        assert_eq!(
            standard_duration(1440).unwrap(),
            scheduler
                .sleep_duration(now)
                .expect("failed sleep duration check")
        );
    }

    #[test]
    fn check_after_reward_period_but_before_offset() {
        let scheduler = Scheduler::new(
            reward_period_length(),
            dt(2022, 12, 1, 0, 0, 0),
            dt(2022, 12, 2, 0, 0, 0),
            Duration::minutes(30),
        );

        let now = dt(2022, 12, 2, 0, 15, 0);

        assert_eq!(
            dt(2022, 12, 1, 0, 0, 0)..dt(2022, 12, 2, 0, 0, 0),
            scheduler.reward_period
        );
        assert!(!scheduler.should_reward(now));
        assert_eq!(
            standard_duration(15).unwrap(),
            scheduler
                .sleep_duration(now)
                .expect("failed sleep duration check")
        );
    }
}
