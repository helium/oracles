use chrono::{DateTime, Duration, Utc};
use file_store::{file_sink, speedtest::CellSpeedtest, traits::TimestampEncode};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use mobile_config::{client::ClientError, gateway_info::GatewayInfoResolver, Client};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{
    postgres::{types::PgHasArrayType, PgTypeInfo},
    FromRow, Postgres, Transaction, Type,
};
use std::collections::{HashMap, VecDeque};

const SPEEDTEST_AVG_MAX_DATA_POINTS: usize = 6;
const SPEEDTEST_LAPSE: i64 = 48;

#[derive(Debug, Clone, Type)]
#[sqlx(type_name = "speedtest")]
pub struct Speedtest {
    pub timestamp: DateTime<Utc>,
    pub upload_speed: i64,
    pub download_speed: i64,
    pub latency: i32,
}

impl Speedtest {
    #[cfg(test)]
    pub fn new(
        timestamp: DateTime<Utc>,
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

impl From<CellSpeedtest> for Speedtest {
    fn from(cell_speedtest: CellSpeedtest) -> Self {
        Self {
            timestamp: cell_speedtest.timestamp,
            upload_speed: cell_speedtest.upload_speed as i64,
            download_speed: cell_speedtest.download_speed as i64,
            latency: cell_speedtest.latency as i32,
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
    pub id: PublicKeyBinary,
    pub speedtests: Vec<Speedtest>,
    pub latest_timestamp: DateTime<Utc>,
}

impl SpeedtestRollingAverage {
    pub fn new(id: PublicKeyBinary) -> Self {
        Self {
            id,
            speedtests: Vec::new(),
            latest_timestamp: DateTime::<Utc>::default(),
        }
    }

    pub async fn validate_speedtests<'a>(
        config_client: &'a Client,
        speedtests: impl Stream<Item = CellSpeedtest> + 'a,
        exec: &mut Transaction<'_, Postgres>,
    ) -> Result<impl Stream<Item = Result<Self, ClientError>> + 'a, sqlx::Error> {
        let tests_by_publickey = speedtests
            .fold(
                HashMap::<PublicKeyBinary, Vec<CellSpeedtest>>::new(),
                |mut map, cell_speedtest| async move {
                    map.entry(cell_speedtest.pubkey.clone())
                        .or_default()
                        .push(cell_speedtest);
                    map
                },
            )
            .await;

        let mut speedtests = Vec::new();
        for (pubkey, cell_speedtests) in tests_by_publickey.into_iter() {
            let rolling_average: SpeedtestRollingAverage =
                sqlx::query_as::<_, SpeedtestRollingAverage>(
                    "SELECT * FROM speedtests WHERE id = $1",
                )
                .bind(&pubkey)
                .fetch_optional(&mut *exec)
                .await?
                .unwrap_or_else(|| SpeedtestRollingAverage::new(pubkey.clone()));
            speedtests.push((rolling_average, cell_speedtests));
        }

        Ok(futures::stream::iter(speedtests.into_iter())
            .then(move |(rolling_average, cell_speedtests)| {
                let mut config_client = config_client.clone();
                async move {
                    // If we get back some gateway info for the given address, it's a valid address
                    if config_client
                        .resolve_gateway_info(&rolling_average.id)
                        .await?
                        .is_none()
                    {
                        return Ok(None);
                    }
                    Ok(Some((rolling_average, cell_speedtests)))
                }
            })
            .filter_map(|item| async move { item.transpose() })
            .map_ok(|(rolling_average, cell_speedtests)| {
                let speedtests = cell_speedtests
                    .into_iter()
                    .map(Speedtest::from)
                    .chain(rolling_average.speedtests.into_iter())
                    .take(SPEEDTEST_AVG_MAX_DATA_POINTS)
                    .collect::<Vec<Speedtest>>();

                Self {
                    id: rolling_average.id,
                    latest_timestamp: speedtests[0].timestamp,
                    speedtests,
                }
            }))
    }

    pub async fn save(self, exec: impl sqlx::PgExecutor<'_>) -> Result<bool, sqlx::Error> {
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
            returning (xmax = 0) as inserted
            "#,
        )
        .bind(self.id)
        .bind(self.speedtests)
        .bind(self.latest_timestamp)
        .fetch_one(exec)
        .await
        .map(|result| result.inserted)
    }

    pub async fn write(&self, averages: &file_sink::FileSinkClient) -> file_store::Result {
        // Write out the speedtests to S3
        let average = Average::from(&self.speedtests);
        let validity = average.validity() as i32;
        // this is guaratneed to safely convert and not panic as it can only be one of
        // four possible decimal values based on the speedtest average tier
        let reward_multiplier = average.reward_multiplier().try_into().unwrap();
        let Average {
            upload_speed_avg_bps,
            download_speed_avg_bps,
            latency_avg_ms,
            ..
        } = average;
        averages
            .write(
                proto::SpeedtestAvg {
                    pub_key: self.id.clone().into(),
                    upload_speed_avg_bps,
                    download_speed_avg_bps,
                    latency_avg_ms,
                    timestamp: Utc::now().encode_timestamp(),
                    speedtests: speedtests_without_lapsed(
                        self.speedtests.iter(),
                        Duration::hours(SPEEDTEST_LAPSE),
                    )
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
                [],
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
    pub speedtests: HashMap<PublicKeyBinary, VecDeque<Speedtest>>,
}

impl SpeedtestAverages {
    #[allow(dead_code)]
    pub fn into_iter(self) -> impl IntoIterator<Item = SpeedtestRollingAverage> {
        self.speedtests
            .into_iter()
            .map(|(id, window)| SpeedtestRollingAverage {
                id,
                // window is guaranteed to be non-empty. For safety, we set the
                // latest timestamp to epoch.
                latest_timestamp: window
                    .front()
                    .map_or_else(DateTime::<Utc>::default, |st| st.timestamp),
                speedtests: Vec::from(window),
            })
    }

    pub fn get_average(&self, pub_key: &PublicKeyBinary) -> Option<Average> {
        self.speedtests.get(pub_key).map(Average::from)
    }

    pub async fn validated(
        exec: impl sqlx::PgExecutor<'_> + Copy,
        period_end: DateTime<Utc>,
    ) -> Result<Self, sqlx::Error> {
        let mut speedtests = HashMap::new();

        let mut rows = sqlx::query_as::<_, SpeedtestRollingAverage>(
            "SELECT * FROM speedtests where latest_timestamp >= $1",
        )
        .bind((period_end - Duration::hours(SPEEDTEST_LAPSE)).naive_utc())
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
}

impl Extend<SpeedtestRollingAverage> for SpeedtestAverages {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = SpeedtestRollingAverage>,
    {
        for SpeedtestRollingAverage { id, speedtests, .. } in iter.into_iter() {
            self.speedtests.insert(id, VecDeque::from(speedtests));
        }
    }
}

fn speedtests_without_lapsed<'a>(
    iterable: impl Iterator<Item = &'a Speedtest>,
    lapse_cliff: Duration,
) -> impl Iterator<Item = &'a Speedtest> {
    let mut last_timestamp = None;
    iterable.take_while(move |speedtest| match last_timestamp {
        Some(ts) if ts - speedtest.timestamp > lapse_cliff => false,
        None | Some(_) => {
            last_timestamp = Some(speedtest.timestamp);
            true
        }
    })
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
        } in speedtests_without_lapsed(iter.into_iter(), Duration::hours(SPEEDTEST_LAPSE))
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
            Average::default()
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
                .min(SpeedtestTier::from_upload_speed(self.upload_speed_avg_bps))
                .min(SpeedtestTier::from_latency(self.latency_avg_ms))
        }
    }

    pub fn reward_multiplier(&self) -> Decimal {
        self.tier().into_multiplier()
    }
}

const fn mbps(mbps: u64) -> u64 {
    mbps * 125000
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpeedtestTier {
    Failed = 0,
    Poor = 1,
    Degraded = 2,
    Acceptable = 3,
}

impl SpeedtestTier {
    fn into_multiplier(self) -> Decimal {
        match self {
            Self::Acceptable => dec!(1.0),
            Self::Degraded => dec!(0.5),
            Self::Poor => dec!(0.25),
            Self::Failed => dec!(0.0),
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

/*
#[derive(thiserror::Error, Debug)]
pub enum FetchError {
    #[error("Config client error: {0}")]
    ConfigClientError(#[from] ClientError),
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
}

#[async_trait::async_trait]
pub trait SpeedtestStore {
    async fn fetch(
        &mut self,
        id: &PublicKeyBinary,
    ) -> Result<Option<SpeedtestRollingAverage>, FetchError>;
}

#[async_trait::async_trait]
impl<E> SpeedtestStore for E
where
    for<'a> E: sqlx::PgExecutor<'a>,
{
    async fn fetch(
        &mut self,
        id: &PublicKeyBinary,
    ) -> Result<Option<SpeedtestRollingAverage>, FetchError> {
        Ok(
            sqlx::query_as::<_, SpeedtestRollingAverage>("SELECT * FROM speedtests WHERE id = $1")
                .bind(id)
                .fetch_optional(*self)
                .await?,
        )
    }
}

#[derive(Copy, Clone)]
pub struct EmptyDatabase;

#[async_trait::async_trait]
impl SpeedtestStore for EmptyDatabase {
    async fn fetch(
        &mut self,
        _id: &PublicKeyBinary,
    ) -> Result<Option<SpeedtestRollingAverage>, FetchError> {
        Ok(None)
    }
}
*/

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;

    fn parse_dt(dt: &str) -> DateTime<Utc> {
        Utc.datetime_from_str(dt, "%Y-%m-%d %H:%M:%S %z")
            .expect("unable_to_parse")
    }

    fn bytes_per_s(mbps: i64) -> i64 {
        mbps * 125000
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
        vec![
            Speedtest::new(parse_dt("2022-08-02 18:00:00 +0000"), 0, 0, 0),
            Speedtest::new(
                parse_dt("2022-08-02 12:00:00 +0000"),
                bytes_per_s(20),
                bytes_per_s(150),
                70,
            ),
            Speedtest::new(
                parse_dt("2022-08-02 6:00:00 +0000"),
                bytes_per_s(10),
                bytes_per_s(118),
                50,
            ),
            Speedtest::new(
                parse_dt("2022-08-02 0:00:00 +0000"),
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
                parse_dt("2022-08-01 18:00:00 +0000"),
                bytes_per_s(20),
                bytes_per_s(130),
                10,
            ),
            Speedtest::new(
                parse_dt("2022-08-01 12:00:00 +0000"),
                bytes_per_s(10),
                bytes_per_s(100),
                30,
            ),
            Speedtest::new(
                parse_dt("2022-08-01 6:00:00 +0000"),
                bytes_per_s(30),
                bytes_per_s(70),
                40,
            ),
        ]
    }

    #[test]
    fn check_tier_cmp() {
        assert_eq!(
            SpeedtestTier::Acceptable.min(SpeedtestTier::Failed),
            SpeedtestTier::Failed,
        );
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

    #[test]
    fn check_speedtest_rolling_avg() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let speedtests = VecDeque::from(known_speedtests());
        let avgs = SpeedtestAverages {
            speedtests: HashMap::from([(owner, speedtests)]),
        }
        .into_iter();
        for avg in avgs {
            if let Some(first) = avg.speedtests.first() {
                assert_eq!(avg.latest_timestamp, first.timestamp);
            }
        }
    }

    #[test]
    fn check_speedtest_without_lapsed() {
        let speedtest_cutoff = Duration::hours(10);
        let contiguos_speedtests = known_speedtests();
        let contiguous_speedtests =
            speedtests_without_lapsed(contiguos_speedtests.iter(), speedtest_cutoff);

        let disjoint_speedtests = vec![
            Speedtest::new(
                parse_dt("2022-08-02 6:00:00 +0000"),
                bytes_per_s(20),
                bytes_per_s(150),
                70,
            ),
            Speedtest::new(
                parse_dt("2022-08-01 18:00:00 +0000"),
                bytes_per_s(10),
                bytes_per_s(118),
                50,
            ),
            Speedtest::new(
                parse_dt("2022-08-01 12:00:00 +0000"),
                bytes_per_s(30),
                bytes_per_s(112),
                40,
            ),
        ];
        let disjoint_speedtests =
            speedtests_without_lapsed(disjoint_speedtests.iter(), speedtest_cutoff);

        assert_eq!(contiguous_speedtests.count(), 8);
        assert_eq!(disjoint_speedtests.count(), 1);
    }
}
