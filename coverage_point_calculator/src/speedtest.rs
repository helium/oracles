use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::MaxOneMultplier;

const MIN_REQUIRED_SPEEDTEST_SAMPLES: usize = 2;
const MAX_ALLOWED_SPEEDTEST_SAMPLES: usize = 6;

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct BytesPs(u64);

impl BytesPs {
    pub fn new(bytes_per_second: u64) -> Self {
        Self(bytes_per_second)
    }

    pub fn mbps(megabytes_per_second: u64) -> Self {
        Self(megabytes_per_second * 12500)
    }

    fn as_mbps(&self) -> u64 {
        self.0 / 12500
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Millis(u32);

impl Millis {
    pub fn new(milliseconds: u32) -> Self {
        Self(milliseconds)
    }
}

#[derive(Debug, Clone)]
pub struct Speedtests {
    pub multiplier: Decimal,
    pub speedtests: Vec<Speedtest>,
}

impl Speedtests {
    pub fn new(speedtests: Vec<Speedtest>) -> Self {
        // sort Newest to Oldest
        let mut sorted_speedtests = speedtests;
        sorted_speedtests.sort_by_key(|test| std::cmp::Reverse(test.timestamp));

        let sorted_speedtests: Vec<_> = sorted_speedtests
            .into_iter()
            .take(MAX_ALLOWED_SPEEDTEST_SAMPLES)
            .collect();

        let multiplier = if sorted_speedtests.len() < MIN_REQUIRED_SPEEDTEST_SAMPLES {
            SpeedtestTier::Fail.multiplier()
        } else {
            Speedtest::avg(&sorted_speedtests).multiplier()
        };

        Self {
            multiplier,
            speedtests: sorted_speedtests,
        }
    }

    pub fn avg(&self) -> Speedtest {
        Speedtest::avg(&self.speedtests)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Speedtest {
    pub upload_speed: BytesPs,
    pub download_speed: BytesPs,
    pub latency: Millis,
    pub timestamp: DateTime<Utc>,
}

impl Speedtest {
    pub fn multiplier(&self) -> Decimal {
        let upload = SpeedtestTier::from_upload(&self.upload_speed);
        let download = SpeedtestTier::from_download(&self.download_speed);
        let latency = SpeedtestTier::from_latency(&self.latency);

        let tier = upload.min(download).min(latency);
        tier.multiplier()
    }

    pub fn avg(speedtests: &[Self]) -> Self {
        let mut download = 0;
        let mut upload = 0;
        let mut latency = 0;

        for test in speedtests {
            upload += test.upload_speed.0;
            download += test.download_speed.0;
            latency += test.latency.0;
        }

        let count = speedtests.len();
        Self {
            upload_speed: BytesPs::new(upload / count as u64),
            download_speed: BytesPs::new(download / count as u64),
            latency: Millis::new(latency / count as u32),
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpeedtestTier {
    Good = 4,
    Acceptable = 3,
    Degraded = 2,
    Poor = 1,
    Fail = 0,
}

impl SpeedtestTier {
    pub fn multiplier(&self) -> MaxOneMultplier {
        match self {
            SpeedtestTier::Good => dec!(1.00),
            SpeedtestTier::Acceptable => dec!(0.75),
            SpeedtestTier::Degraded => dec!(0.50),
            SpeedtestTier::Poor => dec!(0.25),
            SpeedtestTier::Fail => dec!(0),
        }
    }

    fn from_download(bytes: &BytesPs) -> Self {
        match bytes.as_mbps() {
            100.. => Self::Good,
            75.. => Self::Acceptable,
            50.. => Self::Degraded,
            30.. => Self::Poor,
            _ => Self::Fail,
        }
    }

    fn from_upload(bytes: &BytesPs) -> Self {
        match bytes.as_mbps() {
            10.. => Self::Good,
            8.. => Self::Acceptable,
            5.. => Self::Degraded,
            2.. => Self::Poor,
            _ => Self::Fail,
        }
    }

    fn from_latency(Millis(millis): &Millis) -> Self {
        match millis {
            ..=49 => Self::Good,
            ..=59 => Self::Acceptable,
            ..=74 => Self::Degraded,
            ..=99 => Self::Poor,
            _ => Self::Fail,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn speedtest_teirs() {
        use SpeedtestTier::*;
        // download
        assert_eq!(Good, SpeedtestTier::from_download(&BytesPs::mbps(100)));
        assert_eq!(Acceptable, SpeedtestTier::from_download(&BytesPs::mbps(80)));
        assert_eq!(Degraded, SpeedtestTier::from_download(&BytesPs::mbps(62)));
        assert_eq!(Poor, SpeedtestTier::from_download(&BytesPs::mbps(42)));
        assert_eq!(Fail, SpeedtestTier::from_download(&BytesPs::mbps(20)));

        // upload
        assert_eq!(Good, SpeedtestTier::from_upload(&BytesPs::mbps(10)));
        assert_eq!(Acceptable, SpeedtestTier::from_upload(&BytesPs::mbps(8)));
        assert_eq!(Degraded, SpeedtestTier::from_upload(&BytesPs::mbps(6)));
        assert_eq!(Poor, SpeedtestTier::from_upload(&BytesPs::mbps(4)));
        assert_eq!(Fail, SpeedtestTier::from_upload(&BytesPs::mbps(1)));

        // latency
        assert_eq!(Good, SpeedtestTier::from_latency(&Millis::new(49)));
        assert_eq!(Acceptable, SpeedtestTier::from_latency(&Millis::new(59)));
        assert_eq!(Degraded, SpeedtestTier::from_latency(&Millis::new(74)));
        assert_eq!(Poor, SpeedtestTier::from_latency(&Millis::new(99)));
        assert_eq!(Fail, SpeedtestTier::from_latency(&Millis::new(101)));
    }

    #[test]
    fn restrict_to_maximum_speedtests_allowed() {
        let base = Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency: Millis::new(15),
            timestamp: Utc::now(),
        };
        let speedtests = std::iter::repeat(base).take(10).collect();
        let speedtests = Speedtests::new(speedtests);

        assert_eq!(MAX_ALLOWED_SPEEDTEST_SAMPLES, speedtests.speedtests.len());
    }

    #[test]
    fn speedtests_ordered_newest_to_oldest() {
        let make_speedtest = |timestamp: DateTime<Utc>, latency: Millis| Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency,
            timestamp,
        };

        let speedtests = Speedtests::new(vec![
            make_speedtest(date(2024, 4, 6), Millis::new(15)),
            make_speedtest(date(2024, 4, 5), Millis::new(15)),
            make_speedtest(date(2024, 4, 4), Millis::new(15)),
            make_speedtest(date(2024, 4, 3), Millis::new(15)),
            make_speedtest(date(2024, 4, 2), Millis::new(15)),
            make_speedtest(date(2024, 4, 1), Millis::new(15)),
            //
            make_speedtest(date(2022, 4, 6), Millis::new(999)),
            make_speedtest(date(2022, 4, 5), Millis::new(999)),
            make_speedtest(date(2022, 4, 4), Millis::new(999)),
            make_speedtest(date(2022, 4, 3), Millis::new(999)),
            make_speedtest(date(2022, 4, 2), Millis::new(999)),
            make_speedtest(date(2022, 4, 1), Millis::new(999)),
        ]);
        println!("{speedtests:?}");
        assert_eq!(dec!(1), speedtests.multiplier);
    }

    fn date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    }
}
