use crate::{Error, Result};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use file_store::{speedtest::CellSpeedtest, FileStore};
use futures::stream::TryStreamExt;
use helium_crypto::PublicKey;
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

impl PgHasArrayType for Speedtest {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_speedtest")
    }
}

#[derive(FromRow)]
pub struct SpeedtestRollingAverage {
    pub id: PublicKey,
    pub speedtests: Vec<Speedtest>,
}

impl SpeedtestRollingAverage {
    pub fn new(id: PublicKey, speedtests: Vec<Speedtest>) -> Self {
        Self { id, speedtests }
    }

    pub async fn fetch(exec: impl sqlx::PgExecutor<'_>, id: &PublicKey) -> Result<Self> {
        sqlx::query_as::<_, Self>("SELECT * FROM speedtests WHERE id = $1")
            .bind(id)
            .fetch_one(exec)
            .await
            .map_err(Error::from)
    }

    pub async fn save(self, exec: impl sqlx::PgExecutor<'_>) -> Result<bool> {
        #[derive(FromRow)]
        struct SaveResult {
            inserted: bool,
        }

        sqlx::query_as::<_, SaveResult>(
            r#"
            insert into speedtests (id, speedtests)
            values ($1, $2)
            on conflict (id) do update set
            speedtests = EXCLUDED.speedtests
            returning (xmax = 0) as inserted;
            "#,
        )
        .bind(self.id)
        .bind(self.speedtests)
        .fetch_one(exec)
        .await
        .map(|result| result.inserted)
        .map_err(Error::from)
    }
}

#[derive(Clone)]
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
                speedtests: Vec::from(window),
            })
    }

    pub fn get_average(&self, pub_key: &PublicKey) -> Option<Average> {
        self.speedtests.get(pub_key).map(Average::from)
    }

    pub async fn validated(
        exec: impl sqlx::PgExecutor<'_> + Copy,
        starting: DateTime<Utc>,
    ) -> std::result::Result<Self, sqlx::Error> {
        let mut speedtests = HashMap::new();

        let mut rows = sqlx::query_as::<_, SpeedtestRollingAverage>(
            "SELECT * FROM speedtests where timestamp >= $1",
        )
        .bind(starting.naive_utc())
        .fetch(exec);

        while let Some(SpeedtestRollingAverage {
            id,
            speedtests: window,
        }) = rows.try_next().await?
        {
            speedtests.insert(id, VecDeque::from(window));
        }

        Ok(Self { speedtests })
    }

    pub async fn validate_speedtests(
        exec: impl sqlx::PgExecutor<'_> + Copy,
        file_store: &FileStore,
        epoch: &Range<DateTime<Utc>>,
    ) -> Result<Self> {
        let mut speedtests = HashMap::new();

        for CellSpeedtest {
            pubkey,
            timestamp,
            upload_speed,
            download_speed,
            latency,
            ..
        } in crate::ingest::new_speedtest_reports(file_store, epoch)
            .await?
            .into_iter()
        {
            if !speedtests.contains_key(&pubkey) {
                let SpeedtestRollingAverage {
                    id,
                    speedtests: window,
                } = SpeedtestRollingAverage::fetch(exec, &pubkey)
                    .await
                    .unwrap_or_else(|_| SpeedtestRollingAverage::new(pubkey.clone(), Vec::new()));

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

impl<'a, I> From<&'a I> for Average
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

        Average {
            window_size,
            upload_speed_avg_bps: sum_upload / window_size as u64,
            download_speed_avg_bps: sum_download / window_size as u64,
            latency_avg_ms: sum_latency / window_size as u32,
        }
    }
}

// 12500000 bytes/sec = 100 Mbps
const MIN_DOWNLOAD: u64 = 12500000;
// 1250000 bytes/sec = 10 Mbps
const MIN_UPLOAD: u64 = 1250000;
// 50 ms
const MAX_LATENCY: u32 = 50;
// Minimum samples required to check validity
pub const MIN_REQUIRED_SAMPLES: usize = 2;

impl Average {
    // TODO: Change this to a multiplier
    pub fn is_valid(&self) -> bool {
        self.window_size >= MIN_REQUIRED_SAMPLES
            && self.download_speed_avg_bps >= MIN_DOWNLOAD
            && self.upload_speed_avg_bps >= MIN_UPLOAD
            && self.latency_avg_ms <= MAX_LATENCY
    }
}
