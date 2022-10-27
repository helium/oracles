use chrono::{DateTime, Duration, Utc};
use std::ops::Range;

#[derive(Debug)]
pub struct Scheduler {
    pub verification_period_length: Duration,
    pub reward_period_length: Duration,
    pub verification_period: Range<DateTime<Utc>>,
    pub reward_period: Range<DateTime<Utc>>,
}

// I don't want to have to define this struct, but otherwise I'd have to
// import the deprecated time crate.
#[derive(thiserror::Error, Debug)]
#[error("sleep duration cannot be converted to an std::time::Duration")]
pub struct OutOfRangeError;

impl Scheduler {
    pub fn new(
        verification_period_length: Duration,
        reward_period_length: Duration,
        last_verified_end_time: DateTime<Utc>,
        last_rewarded_end_time: DateTime<Utc>,
        next_rewarded_end_time: DateTime<Utc>,
    ) -> Self {
        let verification_period = last_verified_end_time
            ..((last_verified_end_time + verification_period_length).min(next_rewarded_end_time));

        Self {
            verification_period_length,
            reward_period_length,
            verification_period,
            reward_period: last_rewarded_end_time..next_rewarded_end_time,
        }
    }

    pub fn should_verify(&self, now: DateTime<Utc>) -> bool {
        now >= self.verification_period.end
    }

    pub fn should_reward(&self, now: DateTime<Utc>) -> bool {
        self.verification_period.end == self.reward_period.end && now >= self.reward_period.end
    }

    pub fn next_verification_period(&self) -> Range<DateTime<Utc>> {
        self.verification_period.end
            ..(self.verification_period.end + self.verification_period_length)
    }

    pub fn next_reward_period(&self) -> Range<DateTime<Utc>> {
        self.reward_period.end..(self.reward_period.end + self.reward_period_length)
    }

    pub fn sleep_duration(
        &self,
        now: DateTime<Utc>,
    ) -> Result<std::time::Duration, OutOfRangeError> {
        let next_verification_period = self.next_verification_period();
        let next_reward_period = self.next_reward_period();

        let duration = if next_verification_period.end <= now {
            Duration::zero()
        } else {
            (next_verification_period.end.min(next_reward_period.end)) - now
        };

        duration.to_std().map_err(|_| OutOfRangeError)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use futures::stream::StreamExt;

    use super::*;

    fn dt(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.ymd(y, m, d).and_hms(h, min, s)
    }

    fn verification_period_length() -> Duration {
        Duration::hours(3)
    }

    fn reward_period_length() -> Duration {
        Duration::hours(24)
    }

    fn standard_duration(minutes: i64) -> Result<std::time::Duration> {
        Duration::minutes(minutes)
            .to_std()
            .map_err(|_| Error::OutOfRangeError)
    }

    #[test]
    fn first_verification_in_reward_period() {
        let scheduler = Scheduler::new(
            verification_period_length(),
            reward_period_length(),
            dt(2022, 10, 1, 0, 0, 0),
            dt(2022, 10, 1, 0, 0, 0),
            dt(2022, 10, 2, 0, 0, 0),
        );

        let now = dt(2022, 10, 1, 3, 0, 0);

        assert_eq!(
            dt(2022, 10, 1, 0, 0, 0)..dt(2022, 10, 1, 3, 0, 0),
            scheduler.verification_period
        );
        assert_eq!(true, scheduler.should_verify(now));
        assert_eq!(false, scheduler.should_reward(now));
        assert_eq!(
            standard_duration(180).unwrap(),
            scheduler.sleep_duration(now).unwrap()
        );
    }

    #[test]
    fn last_verification_in_reward_period() {
        let scheduler = Scheduler::new(
            verification_period_length(),
            reward_period_length(),
            dt(2022, 10, 1, 21, 0, 0),
            dt(2022, 10, 1, 0, 0, 0),
            dt(2022, 10, 2, 0, 0, 0),
        );

        let now = dt(2022, 10, 2, 0, 0, 0);

        assert_eq!(
            dt(2022, 10, 1, 21, 0, 0)..dt(2022, 10, 2, 0, 0, 0),
            scheduler.verification_period
        );
        assert_eq!(true, scheduler.should_verify(now));
        assert_eq!(true, scheduler.should_reward(now));
        assert_eq!(
            standard_duration(180).unwrap(),
            scheduler.sleep_duration(now).unwrap()
        );
    }

    #[test]
    fn offset_verification_period_from_reward_period() {
        let scheduler = Scheduler::new(
            verification_period_length(),
            reward_period_length(),
            dt(2022, 10, 1, 22, 0, 0),
            dt(2022, 10, 1, 0, 0, 0),
            dt(2022, 10, 2, 0, 0, 0),
        );

        let now = dt(2022, 10, 2, 0, 0, 0);

        assert_eq!(
            dt(2022, 10, 1, 22, 0, 0)..dt(2022, 10, 2, 0, 0, 0),
            scheduler.verification_period
        );
        assert_eq!(true, scheduler.should_verify(now));
        assert_eq!(true, scheduler.should_reward(now));
        assert_eq!(
            standard_duration(180).unwrap(),
            scheduler.sleep_duration(now).unwrap()
        );
    }

    #[test]
    fn missed_verification_periods() {
        let scheduler = Scheduler::new(
            verification_period_length(),
            reward_period_length(),
            dt(2022, 10, 1, 12, 0, 0),
            dt(2022, 10, 1, 0, 0, 0),
            dt(2022, 10, 2, 0, 0, 0),
        );

        let now = dt(2022, 10, 1, 18, 0, 0);

        assert_eq!(
            dt(2022, 10, 1, 12, 0, 0)..dt(2022, 10, 1, 15, 0, 0),
            scheduler.verification_period
        );
        assert_eq!(true, scheduler.should_verify(now));
        assert_eq!(false, scheduler.should_reward(now));
        assert_eq!(
            standard_duration(0).unwrap(),
            scheduler.sleep_duration(now).unwrap()
        );
    }

    #[test]
    fn missed_verification_periods_ran_on_end_of_reward_period() {
        let scheduler = Scheduler::new(
            verification_period_length(),
            reward_period_length(),
            dt(2022, 10, 1, 12, 0, 0),
            dt(2022, 10, 1, 0, 0, 0),
            dt(2022, 10, 2, 0, 0, 0),
        );

        let now = dt(2022, 10, 2, 0, 0, 0);

        assert_eq!(
            dt(2022, 10, 1, 12, 0, 0)..dt(2022, 10, 1, 15, 0, 0),
            scheduler.verification_period
        );
        assert_eq!(true, scheduler.should_verify(now));
        assert_eq!(false, scheduler.should_reward(now));
        assert_eq!(
            standard_duration(0).unwrap(),
            scheduler.sleep_duration(now).unwrap()
        );
    }

    #[tokio::test]
    async fn verification_gets_realigned_at_end_of_reward_period() {
        let init = Scheduler::new(
            verification_period_length(),
            reward_period_length(),
            dt(2022, 10, 1, 14, 38, 12),
            dt(2022, 10, 1, 0, 0, 0),
            dt(2022, 10, 2, 0, 0, 0),
        );

        let now = dt(2022, 10, 2, 0, 0, 0);

        let schedules = futures::stream::unfold((init, false), |(sch, rewarded)| async move {
            if rewarded {
                None
            } else {
                let next = Scheduler::new(
                    sch.verification_period_length,
                    sch.reward_period_length,
                    sch.verification_period.end,
                    if sch.should_reward(now) {
                        sch.next_reward_period().start
                    } else {
                        sch.reward_period.start
                    },
                    if sch.should_reward(now) {
                        sch.next_reward_period().end
                    } else {
                        sch.reward_period.end
                    },
                );

                let rewarded = sch.should_reward(now);
                Some((sch, (next, rewarded)))
            }
        })
        .collect::<Vec<Scheduler>>()
        .await;

        let last = schedules.last().expect("no scheduler available");

        assert_eq!(
            dt(2022, 10, 1, 23, 38, 12)..dt(2022, 10, 2, 0, 0, 0),
            last.verification_period
        );
    }
}
