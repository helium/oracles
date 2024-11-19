use chrono::{DateTime, Utc};
use std::{ops::Range, time::Duration};

#[derive(Debug)]
pub struct Scheduler {
    pub period_duration: Duration,
    pub schedule_period: Range<DateTime<Utc>>,
    pub period_offset: Duration,
}

#[derive(thiserror::Error, Debug)]
#[error("sleep duration cannot be converted to an std::time::Duration")]
pub struct OutOfRangeError;

impl Scheduler {
    pub fn new(
        period_duration: Duration,
        schedule_start_time: DateTime<Utc>,
        schedule_end_time: DateTime<Utc>,
        period_offset: Duration,
    ) -> Self {
        Self {
            period_duration,
            schedule_period: schedule_start_time..schedule_end_time,
            period_offset,
        }
    }

    pub fn should_trigger(&self, now: DateTime<Utc>) -> bool {
        now >= self.schedule_period.end + self.period_offset
    }

    pub fn next_trigger_period(&self) -> Range<DateTime<Utc>> {
        self.schedule_period.end..(self.schedule_period.end + self.period_duration)
    }

    pub fn sleep_duration(
        &self,
        now: DateTime<Utc>,
    ) -> Result<std::time::Duration, OutOfRangeError> {
        let next_period = self.next_trigger_period();

        let duration = if self.schedule_period.end + self.period_offset > now {
            self.schedule_period.end + self.period_offset - now
        } else if next_period.end + self.period_offset <= now {
            chrono::Duration::zero()
        } else {
            (next_period.end + self.period_offset) - now
        };

        duration.to_std().map_err(|_| OutOfRangeError)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn dt(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, s).unwrap()
    }

    fn period_length() -> Duration {
        chrono::Duration::hours(24).to_std().unwrap()
    }

    fn standard_duration(minutes: i64) -> Result<std::time::Duration, OutOfRangeError> {
        chrono::Duration::minutes(minutes)
            .to_std()
            .map_err(|_| OutOfRangeError)
    }

    #[test]
    fn boot_mid_period_with_no_reward() {
        let scheduler = Scheduler::new(
            period_length(),
            dt(2022, 12, 1, 0, 0, 0),
            dt(2022, 12, 2, 0, 0, 0),
            chrono::Duration::minutes(30).to_std().unwrap(),
        );

        let now = dt(2022, 12, 1, 1, 0, 0);

        assert!(!scheduler.should_trigger(now));
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
            period_length(),
            dt(2022, 12, 1, 0, 0, 0),
            dt(2022, 12, 2, 0, 0, 0),
            chrono::Duration::minutes(30).to_std().unwrap(),
        );

        let now = dt(2022, 12, 2, 0, 30, 0);

        assert_eq!(
            dt(2022, 12, 1, 0, 0, 0)..dt(2022, 12, 2, 0, 0, 0),
            scheduler.schedule_period
        );
        assert!(scheduler.should_trigger(now));
        assert_eq!(
            standard_duration(1440).unwrap(),
            scheduler
                .sleep_duration(now)
                .expect("failed sleep duration check")
        );
    }

    #[test]
    fn check_after_trigger_period_but_before_offset() {
        let scheduler = Scheduler::new(
            period_length(),
            dt(2022, 12, 1, 0, 0, 0),
            dt(2022, 12, 2, 0, 0, 0),
            chrono::Duration::minutes(30).to_std().unwrap(),
        );

        let now = dt(2022, 12, 2, 0, 15, 0);

        assert_eq!(
            dt(2022, 12, 1, 0, 0, 0)..dt(2022, 12, 2, 0, 0, 0),
            scheduler.schedule_period
        );
        assert!(!scheduler.should_trigger(now));
        assert_eq!(
            standard_duration(15).unwrap(),
            scheduler
                .sleep_duration(now)
                .expect("failed sleep duration check")
        );
    }
}
