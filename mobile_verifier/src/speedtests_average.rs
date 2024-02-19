use crate::speedtests::{self, Speedtest};
use chrono::{DateTime, Utc};
use file_store::{
    file_sink::FileSinkClient,
    traits::{MsgTimestamp, TimestampEncode},
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

pub const SPEEDTEST_LAPSE: i64 = 48;
const MIN_DOWNLOAD: u64 = mbps(30);
const MIN_UPLOAD: u64 = mbps(2);
const MAX_LATENCY: u32 = 100;
pub const MIN_REQUIRED_SAMPLES: usize = 2;

pub type EpochAverages = HashMap<PublicKeyBinary, SpeedtestAverage>;

#[derive(Debug, Clone)]
pub struct SpeedtestAverage {
    pub pubkey: PublicKeyBinary,
    pub window_size: usize,
    pub upload_speed_avg_bps: u64,
    pub download_speed_avg_bps: u64,
    pub latency_avg_ms: u32,
    pub validity: proto::SpeedtestAvgValidity,
    pub reward_multiplier: Decimal,
    pub speedtests: Vec<Speedtest>,
}

impl From<Vec<Speedtest>> for SpeedtestAverage {
    fn from(speedtests: Vec<Speedtest>) -> Self {
        let mut id = vec![]; // eww!
        let mut window_size = 0;
        let mut sum_upload = 0;
        let mut sum_download = 0;
        let mut sum_latency = 0;

        for Speedtest { report, .. } in speedtests.iter() {
            id = report.pubkey.as_ref().to_vec(); // eww!
            sum_upload += report.upload_speed;
            sum_download += report.download_speed;
            sum_latency += report.latency;
            window_size += 1;
        }

        if window_size > 0 {
            let upload_speed_avg_bps = sum_upload / window_size as u64;
            let download_speed_avg_bps = sum_download / window_size as u64;
            let latency_avg_ms = sum_latency / window_size as u32;
            let validity = validity(
                window_size as usize,
                upload_speed_avg_bps,
                download_speed_avg_bps,
                latency_avg_ms,
            );
            let tier = SpeedtestTier::new(
                window_size as usize,
                upload_speed_avg_bps,
                download_speed_avg_bps,
                latency_avg_ms,
            );
            let reward_multiplier = tier.into_multiplier();
            SpeedtestAverage {
                pubkey: id.into(),
                window_size: window_size as usize,
                upload_speed_avg_bps,
                download_speed_avg_bps,
                latency_avg_ms,
                validity,
                reward_multiplier,
                speedtests,
            }
        } else {
            SpeedtestAverage {
                pubkey: id.into(),
                window_size: 0,
                upload_speed_avg_bps: sum_upload,
                download_speed_avg_bps: sum_download,
                latency_avg_ms: sum_latency,
                validity: proto::SpeedtestAvgValidity::TooFewSamples,
                reward_multiplier: Decimal::ZERO,
                speedtests,
            }
        }
    }
}

impl SpeedtestAverage {
    pub async fn write(&self, filesink: &FileSinkClient) -> file_store::Result {
        filesink
            .write(
                proto::SpeedtestAvg {
                    pub_key: self.pubkey.clone().into(),
                    upload_speed_avg_bps: self.upload_speed_avg_bps,
                    download_speed_avg_bps: self.download_speed_avg_bps,
                    latency_avg_ms: self.latency_avg_ms,
                    timestamp: Utc::now().encode_timestamp(),
                    speedtests: self
                        .speedtests
                        .iter()
                        .map(|st| proto::Speedtest {
                            timestamp: st.report.timestamp(),
                            upload_speed_bps: st.report.upload_speed,
                            download_speed_bps: st.report.download_speed,
                            latency_ms: st.report.latency,
                        })
                        .collect(),
                    validity: self.validity as i32,
                    reward_multiplier: self.reward_multiplier.try_into().unwrap(),
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    pub fn reward_multiplier(&self) -> Decimal {
        self.reward_multiplier
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpeedtestTier {
    Failed = 0,
    Poor = 1,
    Degraded = 2,
    Acceptable = 3,
    Good = 4,
}

impl SpeedtestTier {
    pub fn new(
        window_size: usize,
        upload_speed_avg_bps: u64,
        download_speed_avg_bps: u64,
        latency_avg_ms: u32,
    ) -> SpeedtestTier {
        calculate_tier(
            window_size,
            upload_speed_avg_bps,
            download_speed_avg_bps,
            latency_avg_ms,
        )
    }

    fn into_multiplier(self) -> Decimal {
        match self {
            Self::Good => dec!(1.0),
            Self::Acceptable => dec!(0.75),
            Self::Degraded => dec!(0.5),
            Self::Poor => dec!(0.25),
            Self::Failed => dec!(0.0),
        }
    }

    fn from_download_speed(download_speed: u64) -> Self {
        if download_speed >= mbps(100) {
            Self::Good
        } else if download_speed >= mbps(75) {
            Self::Acceptable
        } else if download_speed >= mbps(50) {
            Self::Degraded
        } else if download_speed >= mbps(30) {
            Self::Poor
        } else {
            Self::Failed
        }
    }

    fn from_upload_speed(upload_speed: u64) -> Self {
        if upload_speed >= mbps(10) {
            Self::Good
        } else if upload_speed >= mbps(8) {
            Self::Acceptable
        } else if upload_speed >= mbps(5) {
            Self::Degraded
        } else if upload_speed >= mbps(2) {
            Self::Poor
        } else {
            Self::Failed
        }
    }

    fn from_latency(latency: u32) -> Self {
        if latency < 50 {
            Self::Good
        } else if latency < 60 {
            Self::Acceptable
        } else if latency < 75 {
            Self::Degraded
        } else if latency < 100 {
            Self::Poor
        } else {
            Self::Failed
        }
    }
}

#[derive(Clone, Default)]
pub struct SpeedtestAverages {
    pub averages: HashMap<PublicKeyBinary, SpeedtestAverage>,
}

impl SpeedtestAverages {
    pub async fn write_all(&self, sink: &FileSinkClient) -> anyhow::Result<()> {
        for speedtest in self.averages.values() {
            speedtest.write(sink).await?;
        }

        Ok(())
    }

    pub fn get_average(&self, pub_key: &PublicKeyBinary) -> Option<SpeedtestAverage> {
        self.averages.get(pub_key).cloned()
    }

    pub async fn aggregate_epoch_averages(
        epoch_end: DateTime<Utc>,
        pool: &sqlx::Pool<sqlx::Postgres>,
    ) -> Result<SpeedtestAverages, sqlx::Error> {
        let averages: EpochAverages = speedtests::aggregate_epoch_speedtests(epoch_end, pool)
            .await?
            .into_iter()
            .map(|(pub_key, speedtests)| {
                let average = SpeedtestAverage::from(speedtests);
                (pub_key, average)
            })
            .collect();

        Ok(Self { averages })
    }
}

pub fn calculate_tier(
    window_size: usize,
    upload_speed_avg_bps: u64,
    download_speed_avg_bps: u64,
    latency_avg_ms: u32,
) -> SpeedtestTier {
    if window_size < MIN_REQUIRED_SAMPLES {
        SpeedtestTier::Failed
    } else {
        SpeedtestTier::from_download_speed(download_speed_avg_bps)
            .min(SpeedtestTier::from_upload_speed(upload_speed_avg_bps))
            .min(SpeedtestTier::from_latency(latency_avg_ms))
    }
}

pub fn validity(
    window_size: usize,
    upload_speed_avg_bps: u64,
    download_speed_avg_bps: u64,
    latency_avg_ms: u32,
) -> proto::SpeedtestAvgValidity {
    if window_size < MIN_REQUIRED_SAMPLES {
        return proto::SpeedtestAvgValidity::TooFewSamples;
    }
    if download_speed_avg_bps < MIN_DOWNLOAD {
        return proto::SpeedtestAvgValidity::SlowDownloadSpeed;
    }
    if upload_speed_avg_bps < MIN_UPLOAD {
        return proto::SpeedtestAvgValidity::SlowUploadSpeed;
    }
    if latency_avg_ms > MAX_LATENCY {
        return proto::SpeedtestAvgValidity::HighLatency;
    }
    proto::SpeedtestAvgValidity::Valid
}

const fn mbps(mbps: u64) -> u64 {
    mbps * 125000
}

#[cfg(test)]
mod test {
    use super::*;
    use file_store::speedtest::CellSpeedtest;

    impl SpeedtestAverage {
        pub fn tier(&self) -> SpeedtestTier {
            calculate_tier(
                self.window_size,
                self.upload_speed_avg_bps,
                self.download_speed_avg_bps,
                self.latency_avg_ms,
            )
        }
    }

    fn parse_dt(dt: &str) -> DateTime<Utc> {
        DateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S %z")
            .expect("unable_to_parse")
            .with_timezone(&Utc)
    }

    fn bytes_per_s(mbps: u64) -> u64 {
        mbps * 125000
    }

    #[test]
    fn check_tier_cmp() {
        assert_eq!(
            SpeedtestTier::Good.min(SpeedtestTier::Failed),
            SpeedtestTier::Failed,
        );
    }

    #[test]
    fn check_known_valid() {
        let speedtests = known_speedtests();
        dbg!(SpeedtestAverage::from(speedtests[0..5].to_vec()).tier());
        assert_eq!(
            SpeedtestAverage::from(speedtests[0..5].to_vec()).tier(),
            SpeedtestTier::Acceptable,
        );
        assert_eq!(
            SpeedtestAverage::from(speedtests[0..6].to_vec()).tier(),
            SpeedtestTier::Good
        );
    }

    #[test]
    fn check_minimum_known_valid() {
        let speedtests = known_speedtests();
        assert_eq!(
            SpeedtestAverage::from(speedtests[4..4].to_vec()).tier(),
            SpeedtestTier::Failed
        );
        assert_eq!(
            SpeedtestAverage::from(speedtests[4..=5].to_vec()).tier(),
            SpeedtestTier::Good
        );
        assert_eq!(
            SpeedtestAverage::from(speedtests[4..=6].to_vec()).tier(),
            SpeedtestTier::Good
        );
    }

    #[test]
    fn check_minimum_known_invalid() {
        let speedtests = known_speedtests();
        assert_ne!(
            SpeedtestAverage::from(speedtests[5..6].to_vec()).tier(),
            SpeedtestTier::Acceptable
        );
    }

    fn known_speedtests() -> Vec<Speedtest> {
        // This data is taken from the spreadsheet
        // Timestamp	DL	UL	Latency	DL RA	UL RA	Latency RA	Acceptable?
        // 2022-08-02 18:00:00	70	30	40	103.33	19.17	30.00	TRUE
        // 2022-08-02 12:00:00	100	10	30	116.67	17.50	35.00	TRUE
        // 2022-08-02 6:00:00	130	20	10	100.00	15.83	30.00	TRUE
        // 2022-08-02 0:00:00	90	15	10	94.00	15.00	34.00	FALSE
        // 2022-08-01 18:00:00	112	30	40	95.00	15.00	40.00	FALSE
        // 2022-08-01 12:00:00	118	10	50	89.33	10.00	40.00	FALSE
        // 2022-08-01 6:00:00	150	20	70	75.00	10.00	35.00	FALSE
        // 2022-08-01 0:00:00	0	0	0	0.00	0.00	0.00	FALSE*
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");

        vec![
            default_cellspeedtest(gw1.clone(), parse_dt("2022-08-02 18:00:00 +0000"), 0, 0, 0),
            default_cellspeedtest(
                gw1.clone(),
                parse_dt("2022-08-02 12:00:00 +0000"),
                bytes_per_s(20),
                bytes_per_s(150),
                70,
            ),
            default_cellspeedtest(
                gw1.clone(),
                parse_dt("2022-08-02 6:00:00 +0000"),
                bytes_per_s(10),
                bytes_per_s(118),
                50,
            ),
            default_cellspeedtest(
                gw1.clone(),
                parse_dt("2022-08-02 0:00:00 +0000"),
                bytes_per_s(30),
                bytes_per_s(112),
                40,
            ),
            default_cellspeedtest(
                gw1.clone(),
                parse_dt("2022-08-02 0:00:00 +0000"),
                bytes_per_s(15),
                bytes_per_s(90),
                10,
            ),
            default_cellspeedtest(
                gw1.clone(),
                parse_dt("2022-08-01 18:00:00 +0000"),
                bytes_per_s(20),
                bytes_per_s(130),
                10,
            ),
            default_cellspeedtest(
                gw1.clone(),
                parse_dt("2022-08-01 12:00:00 +0000"),
                bytes_per_s(10),
                bytes_per_s(100),
                30,
            ),
            default_cellspeedtest(
                gw1,
                parse_dt("2022-08-01 6:00:00 +0000"),
                bytes_per_s(30),
                bytes_per_s(70),
                40,
            ),
        ]
    }

    fn default_cellspeedtest(
        pubkey: PublicKeyBinary,
        timestamp: DateTime<Utc>,
        upload_speed: u64,
        download_speed: u64,
        latency: u32,
    ) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed,
                download_speed,
                latency,
                serial: "".to_string(),
            },
        }
    }
}
