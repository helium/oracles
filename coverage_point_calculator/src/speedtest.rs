use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const MIN_REQUIRED_SPEEDTEST_SAMPLES: usize = 2;
const MAX_ALLOWED_SPEEDTEST_SAMPLES: usize = 6;

type Millis = u32;

/// Bytes per second
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct BytesPs(u64);

impl BytesPs {
    const BYTES_PER_MEGABYTE: u64 = 125_000;

    pub fn new(bytes_per_second: u64) -> Self {
        Self(bytes_per_second)
    }

    pub fn mbps(megabytes_per_second: u64) -> Self {
        Self(megabytes_per_second * Self::BYTES_PER_MEGABYTE)
    }

    fn as_mbps(&self) -> u64 {
        self.0 / Self::BYTES_PER_MEGABYTE
    }

    pub fn as_bps(&self) -> u64 {
        self.0
    }
}

pub(crate) fn clean_speedtests(speedtests: Vec<Speedtest>) -> Vec<Speedtest> {
    let mut cleaned = speedtests;
    // sort newest to oldest
    cleaned.sort_by_key(|test| std::cmp::Reverse(test.timestamp));
    cleaned.truncate(MAX_ALLOWED_SPEEDTEST_SAMPLES);
    cleaned
}

pub(crate) fn multiplier(speedtests: &[Speedtest]) -> Decimal {
    if speedtests.len() < MIN_REQUIRED_SPEEDTEST_SAMPLES {
        return dec!(0);
    }

    let avg = Speedtest::avg(speedtests);
    avg.multiplier()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Speedtest {
    pub upload_speed: BytesPs,
    pub download_speed: BytesPs,
    pub latency_millis: u32,
    pub timestamp: DateTime<Utc>,
}

impl Speedtest {
    /// Construct the minimum required speedtests for a given tier
    pub fn mock(tier: SpeedtestTier) -> Vec<Speedtest> {
        // SpeedtestTier is determined solely by upload_speed.
        // Other values are far surpassing ::Good.
        let upload_speed = BytesPs::mbps(match tier {
            SpeedtestTier::Good => 10,
            SpeedtestTier::Acceptable => 8,
            SpeedtestTier::Degraded => 5,
            SpeedtestTier::Poor => 2,
            SpeedtestTier::Fail => 0,
        });

        vec![
            Speedtest {
                upload_speed,
                download_speed: BytesPs::mbps(150),
                latency_millis: 0,
                timestamp: Utc::now(),
            },
            Speedtest {
                upload_speed,
                download_speed: BytesPs::mbps(150),
                latency_millis: 0,
                timestamp: Utc::now(),
            },
        ]
    }

    pub fn multiplier(&self) -> Decimal {
        let upload = SpeedtestTier::from_upload(self.upload_speed);
        let download = SpeedtestTier::from_download(self.download_speed);
        let latency = SpeedtestTier::from_latency(self.latency_millis);

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
            latency += test.latency_millis;
        }

        let count = speedtests.len();
        Self {
            upload_speed: BytesPs::new(upload / count as u64),
            download_speed: BytesPs::new(download / count as u64),
            latency_millis: latency / count as u32,
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
    pub fn multiplier(self) -> Decimal {
        match self {
            SpeedtestTier::Good => dec!(1.00),
            SpeedtestTier::Acceptable => dec!(0.75),
            SpeedtestTier::Degraded => dec!(0.50),
            SpeedtestTier::Poor => dec!(0.25),
            SpeedtestTier::Fail => dec!(0),
        }
    }

    fn from_download(bytes: BytesPs) -> Self {
        match bytes.as_mbps() {
            100.. => Self::Good,
            75.. => Self::Acceptable,
            50.. => Self::Degraded,
            30.. => Self::Poor,
            _ => Self::Fail,
        }
    }

    fn from_upload(bytes: BytesPs) -> Self {
        match bytes.as_mbps() {
            10.. => Self::Good,
            8.. => Self::Acceptable,
            5.. => Self::Degraded,
            2.. => Self::Poor,
            _ => Self::Fail,
        }
    }

    fn from_latency(millis: Millis) -> Self {
        match millis {
            0..=49 => Self::Good,
            50..=59 => Self::Acceptable,
            60..=74 => Self::Degraded,
            75..=99 => Self::Poor,
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
        assert_eq!(Good, SpeedtestTier::from_download(BytesPs::mbps(100)));
        assert_eq!(Acceptable, SpeedtestTier::from_download(BytesPs::mbps(80)));
        assert_eq!(Degraded, SpeedtestTier::from_download(BytesPs::mbps(62)));
        assert_eq!(Poor, SpeedtestTier::from_download(BytesPs::mbps(42)));
        assert_eq!(Fail, SpeedtestTier::from_download(BytesPs::mbps(20)));

        // upload
        assert_eq!(Good, SpeedtestTier::from_upload(BytesPs::mbps(10)));
        assert_eq!(Acceptable, SpeedtestTier::from_upload(BytesPs::mbps(8)));
        assert_eq!(Degraded, SpeedtestTier::from_upload(BytesPs::mbps(6)));
        assert_eq!(Poor, SpeedtestTier::from_upload(BytesPs::mbps(4)));
        assert_eq!(Fail, SpeedtestTier::from_upload(BytesPs::mbps(1)));

        // latency
        assert_eq!(Good, SpeedtestTier::from_latency(49));
        assert_eq!(Acceptable, SpeedtestTier::from_latency(59));
        assert_eq!(Degraded, SpeedtestTier::from_latency(74));
        assert_eq!(Poor, SpeedtestTier::from_latency(99));
        assert_eq!(Fail, SpeedtestTier::from_latency(101));
    }

    #[test]
    fn minimum_required_speedtests_provided_for_multiplier_above_zero() {
        let speedtest = Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency_millis: 15,
            timestamp: Utc::now(),
        };
        let speedtests = |num: usize| std::iter::repeat(speedtest).take(num).collect::<Vec<_>>();

        assert_eq!(
            dec!(0),
            multiplier(&speedtests(MIN_REQUIRED_SPEEDTEST_SAMPLES - 1))
        );
        assert_eq!(
            dec!(1),
            multiplier(&speedtests(MIN_REQUIRED_SPEEDTEST_SAMPLES))
        );
    }

    #[test]
    fn restrict_to_maximum_speedtests_allowed() {
        let base = Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency_millis: 15,
            timestamp: Utc::now(),
        };
        let speedtests = std::iter::repeat(base).take(10).collect();
        let speedtests = clean_speedtests(speedtests);

        assert_eq!(MAX_ALLOWED_SPEEDTEST_SAMPLES, speedtests.len());
    }

    #[test]
    fn speedtests_ordered_newest_to_oldest() {
        let make_speedtest = |timestamp: DateTime<Utc>, latency: Millis| Speedtest {
            upload_speed: BytesPs::mbps(15),
            download_speed: BytesPs::mbps(150),
            latency_millis: latency,
            timestamp,
        };

        // Intersperse new and old speedtests.
        // new speedtests have 1.0 multipliers
        // old speedtests have 0.0 multipliers
        let speedtests = clean_speedtests(vec![
            make_speedtest(date(2024, 4, 6), 15),
            make_speedtest(date(2022, 4, 6), 999),
            // --
            make_speedtest(date(2024, 4, 5), 15),
            make_speedtest(date(2022, 4, 5), 999),
            // --
            make_speedtest(date(2024, 4, 4), 15),
            make_speedtest(date(2022, 4, 4), 999),
            // --
            make_speedtest(date(2022, 4, 3), 999),
            make_speedtest(date(2024, 4, 3), 15),
            // --
            make_speedtest(date(2024, 4, 2), 15),
            make_speedtest(date(2022, 4, 2), 999),
            // --
            make_speedtest(date(2024, 4, 1), 15),
            make_speedtest(date(2022, 4, 1), 999),
        ]);

        // Old speedtests should be unused
        assert_eq!(dec!(1), multiplier(&speedtests));
    }

    #[test]
    fn test_real_bytes_per_second() {
        // Random sampling from database for a download speed that should be
        // "Acceptable". Other situational tests go through the ::mbps()
        // constructor, so will always be consistent with each other.
        assert_eq!(
            SpeedtestTier::Acceptable,
            SpeedtestTier::from_download(BytesPs::new(11_702_687))
        );
    }

    fn date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    }
}
