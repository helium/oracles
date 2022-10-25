use crate::{Error, Result};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use file_store::{
    file_sink,
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    traits::{MsgDecode, TimestampEncode},
    FileStore, FileType,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use helium_crypto::PublicKey;
use helium_proto::services::poc_mobile as proto;
use sqlx::{
    postgres::{types::PgHasArrayType, PgTypeInfo},
    FromRow, Type,
};
use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
};

#[derive(Debug, Clone, Type)]
#[sqlx(type_name = "speedtest")]
pub struct Speedtest {
    pub timestamp: NaiveDateTime,
    pub upload_speed: i64,
    pub download_speed: i64,
    pub latency: i32,
}

impl Speedtest {
    pub fn new(
        timestamp: NaiveDateTime,
        upload_speed: i64,
        download_speed: i64,
        latency: i32,
    ) -> Self {
        Self {
            timestamp,
            upload_speed,
            download_speed,
            latency,
        }
    }
}

impl PgHasArrayType for Speedtest {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_speedtest")
    }
}

#[derive(FromRow)]
pub struct SpeedtestRollingAverage {
    pub id: PublicKey,
    pub speedtests: Vec<Speedtest>,
    pub latest_timestamp: NaiveDateTime,
}

impl SpeedtestRollingAverage {
    pub fn new(id: PublicKey) -> Self {
        Self {
            id,
            speedtests: Vec::new(),
            latest_timestamp: NaiveDateTime::default(),
        }
    }

    pub async fn save(self, exec: impl sqlx::PgExecutor<'_>) -> Result<bool> {
        #[derive(FromRow)]
        struct SaveResult {
            inserted: bool,
        }

        sqlx::query_as::<_, SaveResult>(
            r#"
            insert into speedtests (id, speedtests, latest_timestamp)
            values ($1, $2, $3)
            on conflict (id) do update set
            speedtests = EXCLUDED.speedtests, latest_timestamp = EXCLUDED.latest_timestamp
            returning (xmax = 0) as inserted;
            "#,
        )
        .bind(self.id)
        .bind(self.speedtests)
        .bind(self.latest_timestamp)
        .fetch_one(exec)
        .await
        .map(|result| result.inserted)
        .map_err(Error::from)
    }

    pub async fn write(&self, averages_tx: &file_sink::MessageSender) -> file_store::Result {
        // Write out the speedtests to S3
        let average = Average::from(&self.speedtests);
        let validity = average.validity() as i32;
        let reward_multiplier = average.reward_multiplier();
        let Average {
            upload_speed_avg_bps,
            download_speed_avg_bps,
            latency_avg_ms,
            ..
        } = average;
        file_sink::write(
            averages_tx,
            proto::SpeedtestAvg {
                pub_key: self.id.to_vec(),
                upload_speed_avg_bps,
                download_speed_avg_bps,
                latency_avg_ms,
                timestamp: Utc::now().encode_timestamp(),
                speedtests: self
                    .speedtests
                    .iter()
                    .map(|st| proto::Speedtest {
                        timestamp: st.timestamp.timestamp() as u64,
                        upload_speed_bps: st.upload_speed as u64,
                        download_speed_bps: st.download_speed as u64,
                        latency_ms: st.latency as u32,
                    })
                    .collect(),
                validity,
                reward_multiplier,
            },
        )
        .await?;

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SpeedtestAverages {
    // I'm not sure that VecDeque is actually all that useful here, considering
    // we have to constantly convert between the two.
    // It does make me more confident in the implementation of validate_speedtests
    // though.
    pub speedtests: HashMap<PublicKey, VecDeque<Speedtest>>,
}

impl SpeedtestAverages {
    pub fn into_iter(self) -> impl IntoIterator<Item = SpeedtestRollingAverage> {
        self.speedtests
            .into_iter()
            .map(|(id, window)| SpeedtestRollingAverage {
                id,
                // window is guaranteed to be non-empty. For safety, we set the
                // latest timestamp to epoch.
                latest_timestamp: window
                    .back()
                    .map_or_else(NaiveDateTime::default, |st| st.timestamp),
                speedtests: Vec::from(window),
            })
    }

    pub fn get_average(&self, pub_key: &PublicKey) -> Option<Average> {
        self.speedtests.get(pub_key).map(Average::from)
    }

    pub async fn validated(
        exec: impl sqlx::PgExecutor<'_> + Copy,
        period_end: DateTime<Utc>,
    ) -> std::result::Result<Self, sqlx::Error> {
        let mut speedtests = HashMap::new();

        let mut rows = sqlx::query_as::<_, SpeedtestRollingAverage>(
            "SELECT * FROM speedtests where latest_timestamp >= $1",
        )
        .bind((period_end - Duration::hours(12)).naive_utc())
        .fetch(exec);

        while let Some(SpeedtestRollingAverage {
            id,
            speedtests: window,
            ..
        }) = rows.try_next().await?
        {
            speedtests.insert(id, VecDeque::from(window));
        }

        Ok(Self { speedtests })
    }

    pub async fn validate_speedtests(
        exec: impl SpeedtestStore + Copy,
        file_store: &FileStore,
        epoch: &Range<DateTime<Utc>>,
    ) -> Result<Self> {
        let mut speedtests = HashMap::new();
        let file_list = file_store
            .list_all(FileType::CellSpeedtestIngestReport, epoch.start, epoch.end)
            .await?;
        let mut stream = file_store.source(stream::iter(file_list).map(Ok).boxed());

        while let Some(Ok(msg)) = stream.next().await {
            let CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed,
                download_speed,
                latency,
                ..
            } = match CellSpeedtestIngestReport::decode(msg) {
                Ok(report) => report.report,
                Err(err) => {
                    tracing::error!("Could not decode cell speedtest ingest report: {:?}", err);
                    continue;
                }
            };

            if !speedtests.contains_key(&pubkey) {
                let SpeedtestRollingAverage {
                    id,
                    speedtests: window,
                    ..
                } = exec
                    .fetch(&pubkey)
                    .await
                    .unwrap_or_else(|_| SpeedtestRollingAverage::new(pubkey.clone()));

                speedtests.insert(id, VecDeque::from(window));
            }

            let timestamp = timestamp.naive_utc();
            let upload_speed = upload_speed as i64;
            let download_speed = download_speed as i64;
            let latency = latency as i32;

            // Unwrap here is guaranteed never to panic
            let window = speedtests.get_mut(&pubkey).unwrap();

            // If there are six speedtests in the window, remove the one in the front
            while window.len() >= 6 {
                window.pop_front();
            }

            // If the duration between the last speedtest and the current is greater than
            // 12 hours, then we clear the window.
            if let Some(last_timestamp) = window.back().map(|b| b.timestamp) {
                let duration_since = timestamp - last_timestamp;
                if duration_since >= Duration::hours(12) {
                    window.clear();
                }
            }

            // Add the new speedtest to the back of the window
            window.push_back(Speedtest {
                timestamp,
                upload_speed,
                download_speed,
                latency,
            });
        }

        Ok(Self { speedtests })
    }
}

#[derive(Clone, Debug, Default)]
pub struct Average {
    pub window_size: usize,
    pub upload_speed_avg_bps: u64,
    pub download_speed_avg_bps: u64,
    pub latency_avg_ms: u32,
}

impl<'a, I: ?Sized> From<&'a I> for Average
where
    &'a I: IntoIterator<Item = &'a Speedtest>,
{
    fn from(iter: &'a I) -> Self {
        let mut window_size = 0;
        let mut sum_upload = 0;
        let mut sum_download = 0;
        let mut sum_latency = 0;

        for Speedtest {
            upload_speed,
            download_speed,
            latency,
            ..
        } in iter.into_iter()
        {
            sum_upload += *upload_speed as u64;
            sum_download += *download_speed as u64;
            sum_latency += *latency as u32;
            window_size += 1;
        }

        if window_size > 0 {
            Average {
                window_size,
                upload_speed_avg_bps: sum_upload / window_size as u64,
                download_speed_avg_bps: sum_download / window_size as u64,
                latency_avg_ms: sum_latency / window_size as u32,
            }
        } else {
            Average {
                window_size,
                upload_speed_avg_bps: 0,
                download_speed_avg_bps: 0,
                latency_avg_ms: 0,
            }
        }
    }
}

const MIN_DOWNLOAD: u64 = mbps(30);
const MIN_UPLOAD: u64 = mbps(2);
const MAX_LATENCY: u32 = 100;
pub const MIN_REQUIRED_SAMPLES: usize = 2;

impl Average {
    // TODO: Change this to a multiplier
    pub fn validity(&self) -> proto::SpeedtestAvgValidity {
        if self.window_size < MIN_REQUIRED_SAMPLES {
            return proto::SpeedtestAvgValidity::TooFewSamples;
        }
        if self.download_speed_avg_bps < MIN_DOWNLOAD {
            return proto::SpeedtestAvgValidity::SlowDownloadSpeed;
        }
        if self.upload_speed_avg_bps < MIN_UPLOAD {
            return proto::SpeedtestAvgValidity::SlowUploadSpeed;
        }
        if self.latency_avg_ms > MAX_LATENCY {
            return proto::SpeedtestAvgValidity::HighLatency;
        }
        proto::SpeedtestAvgValidity::Valid
    }

    pub fn tier(&self) -> SpeedtestTier {
        if self.window_size < MIN_REQUIRED_SAMPLES {
            SpeedtestTier::Failed
        } else {
            SpeedtestTier::from_download_speed(self.download_speed_avg_bps)
                .and(SpeedtestTier::from_upload_speed(self.upload_speed_avg_bps))
                .and(SpeedtestTier::from_latency(self.latency_avg_ms))
        }
    }

    pub fn reward_multiplier(&self) -> f32 {
        self.tier().into_multiplier()
    }
}

const fn mbps(mbps: u64) -> u64 {
    mbps * 125000
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SpeedtestTier {
    Acceptable,
    Degraded,
    Poor,
    Failed,
}

impl SpeedtestTier {
    fn and(self, rhs: Self) -> Self {
        match (self, rhs) {
            (_, Self::Failed) | (Self::Failed, _) => Self::Failed,
            (_, Self::Poor) | (Self::Poor, _) => Self::Poor,
            (_, Self::Degraded) | (Self::Degraded, _) => Self::Degraded,
            _ => Self::Acceptable,
        }
    }

    fn into_multiplier(self) -> f32 {
        match self {
            Self::Acceptable => 1.0,
            Self::Degraded => 0.5,
            Self::Poor => 0.25,
            Self::Failed => 0.0,
        }
    }

    fn from_download_speed(download_speed: u64) -> Self {
        if download_speed >= mbps(100) {
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
        if latency <= 50 {
            Self::Acceptable
        } else if latency <= 75 {
            Self::Degraded
        } else if latency <= 100 {
            Self::Poor
        } else {
            Self::Failed
        }
    }
}

// This should probably be abstracted and used elsewhere, but for now all
// we need is fetch from an actual database and mock fetch that returns
// nothing.

#[async_trait::async_trait]
pub trait SpeedtestStore {
    async fn fetch(self, id: &PublicKey) -> Result<SpeedtestRollingAverage>;
}

#[async_trait::async_trait]
impl<E> SpeedtestStore for E
where
    for<'a> E: sqlx::PgExecutor<'a>,
{
    async fn fetch(self, id: &PublicKey) -> Result<SpeedtestRollingAverage> {
        sqlx::query_as::<_, SpeedtestRollingAverage>("SELECT * FROM speedtests WHERE id = $1")
            .bind(id)
            .fetch_one(self)
            .await
            .map_err(Error::from)
    }
}

#[derive(Copy, Clone)]
pub struct EmptyDatabase;

#[async_trait::async_trait]
impl SpeedtestStore for EmptyDatabase {
    async fn fetch(self, _id: &PublicKey) -> Result<SpeedtestRollingAverage> {
        Err(Error::from(sqlx::Error::RowNotFound))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;

    fn parse_dt(dt: &str) -> NaiveDateTime {
        Utc.datetime_from_str(dt, "%Y-%m-%d %H:%M:%S %z")
            .expect("unable_to_parse")
            .naive_utc()
    }

    fn bytes_per_s(mbps: i64) -> i64 {
        mbps * 125000
    }

    fn known_speedtests() -> Vec<Speedtest> {
        // This data is taken from the spreadsheet
        // Timestamp	DL	UL	Latency	DL RA	UL RA	Latency RA	Acceptable?
        // 2022-08-01 0:00:00	0	0	0	0.00	0.00	0.00	FALSE*
        // 2022-08-01 6:00:00	150	20	70	75.00	10.00	35.00	FALSE
        // 2022-08-01 12:00:00	118	10	50	89.33	10.00	40.00	FALSE
        // 2022-08-01 18:00:00	112	30	40	95.00	15.00	40.00	FALSE
        // 2022-08-02 0:00:00	90	15	10	94.00	15.00	34.00	FALSE
        // 2022-08-02 6:00:00	130	20	10	100.00	15.83	30.00	TRUE
        // 2022-08-02 12:00:00	100	10	30	116.67	17.50	35.00	TRUE
        // 2022-08-02 18:00:00	70	30	40	103.33	19.17	30.00	TRUE
        vec![
            Speedtest::new(parse_dt("2022-08-01 0:00:00 +0000"), 0, 0, 0),
            Speedtest::new(
                parse_dt("2022-08-01 6:00:00 +0000"),
                bytes_per_s(20),
                bytes_per_s(150),
                70,
            ),
            Speedtest::new(
                parse_dt("2022-08-01 12:00:00 +0000"),
                bytes_per_s(10),
                bytes_per_s(118),
                50,
            ),
            Speedtest::new(
                parse_dt("2022-08-01 18:00:00 +0000"),
                bytes_per_s(30),
                bytes_per_s(112),
                40,
            ),
            Speedtest::new(
                parse_dt("2022-08-02 0:00:00 +0000"),
                bytes_per_s(15),
                bytes_per_s(90),
                10,
            ),
            Speedtest::new(
                parse_dt("2022-08-02 6:00:00 +0000"),
                bytes_per_s(20),
                bytes_per_s(130),
                10,
            ),
            Speedtest::new(
                parse_dt("2022-08-02 12:00:00 +0000"),
                bytes_per_s(10),
                bytes_per_s(100),
                30,
            ),
            Speedtest::new(
                parse_dt("2022-08-02 18:00:00 +0000"),
                bytes_per_s(30),
                bytes_per_s(70),
                40,
            ),
        ]
    }

    #[test]
    fn check_known_valid() {
        let speedtests = known_speedtests();
        assert_ne!(
            Average::from(&speedtests[0..5]).tier(),
            SpeedtestTier::Acceptable,
        );
        assert_eq!(
            Average::from(&speedtests[0..6]).tier(),
            SpeedtestTier::Acceptable
        );
    }

    #[test]
    fn check_minimum_known_valid() {
        let speedtests = known_speedtests();
        assert_ne!(
            Average::from(&speedtests[4..4]).tier(),
            SpeedtestTier::Acceptable
        );
        assert_eq!(
            Average::from(&speedtests[4..=5]).tier(),
            SpeedtestTier::Acceptable
        );
        assert_eq!(
            Average::from(&speedtests[4..=6]).tier(),
            SpeedtestTier::Acceptable
        );
    }

    #[test]
    fn check_minimum_known_invalid() {
        let speedtests = known_speedtests();
        assert_ne!(
            Average::from(&speedtests[5..6]).tier(),
            SpeedtestTier::Acceptable
        );
    }
}
