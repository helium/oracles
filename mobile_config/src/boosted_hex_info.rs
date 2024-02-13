use crate::client::{hex_boosting_client::HexBoostingInfoResolver, ClientError};
use chrono::{DateTime, Duration, Utc};
use file_store::traits::TimestampDecode;
use futures::stream::{BoxStream, StreamExt};
use helium_proto::BoostedHexInfoV1 as BoostedHexInfoProto;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, convert::TryFrom};

pub type BoostedHexInfoStream = BoxStream<'static, BoostedHexInfo>;

lazy_static::lazy_static! {
    static ref PERIOD_IN_SECONDS: Duration = Duration::seconds(60 * 60 * 24 * 30);
}

#[derive(Clone, Debug)]
pub struct BoostedHexInfo {
    pub location: u64,
    pub start_ts: Option<DateTime<Utc>>,
    pub end_ts: Option<DateTime<Utc>>,
    pub period_length: Duration,
    pub multipliers: Vec<u32>,
    pub boosted_hex_pubkey: Pubkey,
    pub boost_config_pubkey: Pubkey,
    pub version: u32,
}

impl TryFrom<BoostedHexInfoProto> for BoostedHexInfo {
    type Error = anyhow::Error;
    fn try_from(v: BoostedHexInfoProto) -> anyhow::Result<Self> {
        let period_length = Duration::seconds(v.period_length as i64);
        let multipliers = v.multipliers;
        let start_ts = to_start_ts(v.start_ts);
        let end_ts = to_end_ts(start_ts, period_length, multipliers.len());
        let boosted_hex_pubkey: Pubkey = Pubkey::try_from(v.boosted_hex_pubkey.as_slice())?;
        let boost_config_pubkey: Pubkey = Pubkey::try_from(v.boost_config_pubkey.as_slice())?;
        Ok(Self {
            location: v.location,
            start_ts,
            end_ts,
            period_length,
            multipliers,
            boosted_hex_pubkey,
            boost_config_pubkey,
            version: v.version,
        })
    }
}

impl TryFrom<BoostedHexInfo> for BoostedHexInfoProto {
    type Error = anyhow::Error;

    fn try_from(v: BoostedHexInfo) -> anyhow::Result<Self> {
        let start_ts = v.start_ts.map_or(0, |v| v.timestamp() as u64);
        let end_ts = v.end_ts.map_or(0, |v| v.timestamp() as u64);
        Ok(Self {
            location: v.location,
            start_ts,
            end_ts,
            period_length: v.period_length.num_seconds() as u32,
            multipliers: v.multipliers,
            boosted_hex_pubkey: v.boosted_hex_pubkey.to_bytes().into(),
            boost_config_pubkey: v.boost_config_pubkey.to_bytes().into(),
            version: v.version,
        })
    }
}

impl BoostedHexInfo {
    pub fn current_multiplier(&self, ts: DateTime<Utc>) -> anyhow::Result<Option<u32>> {
        if self.end_ts.is_some() && ts >= self.end_ts.unwrap() {
            // end time has been set and the current time is after the end time, so return None
            // to indicate that the hex is no longer boosted
            return Ok(None);
        };
        if self.start_ts.is_some() {
            // start time has previously been set, so we can calculate the current multiplier
            // based on the period length and the current time
            let boost_start_ts = self.start_ts.unwrap();
            let diff = ts - boost_start_ts;
            let index = diff
                .num_seconds()
                .checked_div(self.period_length.num_seconds())
                .unwrap_or(0) as usize;
            Ok(Some(self.multipliers[index]))
        } else {
            // start time has not been previously set, assume this is the first time rewarding this hex
            // and use the first multiplier
            Ok(Some(self.multipliers[0]))
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BoostedHexes {
    pub hexes: HashMap<u64, BoostedHexInfo>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct BoostedHex {
    pub location: u64,
    pub multiplier: u32,
}

impl BoostedHexes {
    pub async fn new(hexes: Vec<BoostedHexInfo>) -> anyhow::Result<Self> {
        let mut map = HashMap::new();
        for info in hexes {
            map.insert(info.location, info);
        }
        Ok(Self { hexes: map })
    }

    pub async fn get_all(
        hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
    ) -> anyhow::Result<Self> {
        tracing::info!("getting boosted hexes");
        let mut map = HashMap::new();
        let mut stream = hex_service_client
            .clone()
            .stream_boosted_hexes_info()
            .await?;
        while let Some(info) = stream.next().await {
            map.insert(info.location, info);
        }
        Ok(Self { hexes: map })
    }

    pub async fn get_modified(
        hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Self> {
        let mut map = HashMap::new();
        let mut stream = hex_service_client
            .clone()
            .stream_modified_boosted_hexes_info(timestamp)
            .await?;
        while let Some(info) = stream.next().await {
            map.insert(info.location, info);
        }
        Ok(Self { hexes: map })
    }

    pub fn get_current_multiplier(&self, location: u64, ts: DateTime<Utc>) -> Option<u32> {
        self.hexes
            .get(&location)
            .and_then(|info| info.current_multiplier(ts).ok()?)
    }
}

pub(crate) mod db {
    use super::{to_end_ts, to_start_ts, BoostedHexInfo};
    use chrono::{DateTime, Duration, Utc};
    use futures::stream::{Stream, StreamExt};
    use solana_sdk::pubkey::Pubkey;
    use sqlx::{PgExecutor, Row};
    use std::str::FromStr;

    const GET_BOOSTED_HEX_INFO_SQL: &str = r#"
            select 
                CAST(hexes.location as bigint), 
                CAST(hexes.start_ts as bigint), 
                config.period_length,
                hexes.boosts_by_period as multipliers,
                hexes.address as boosted_hex_pubkey, 
                config.address as boost_config_pubkey,
                hexes.version
            from boosted_hexes hexes
            join boost_configs config on hexes.boost_config = config.address
        "#;

    // TODO: reuse with string above
    const GET_MODIFIED_BOOSTED_HEX_INFO_SQL: &str = r#"
            select 
                CAST(hexes.location as bigint), 
                CAST(hexes.start_ts as bigint), 
                config.period_length,
                hexes.boosts_by_period as multipliers,
                hexes.address as boosted_hex_pubkey, 
                config.address as boost_config_pubkey,
                hexes.version
            from boosted_hexes hexes
            join boost_configs config on hexes.boost_config = config.address
            where hexes.refreshed_at > $1
        "#;

    pub fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
    ) -> impl Stream<Item = BoostedHexInfo> + 'a {
        sqlx::query_as::<_, BoostedHexInfo>(GET_BOOSTED_HEX_INFO_SQL)
            .fetch(db)
            .filter_map(|info| async move { info.ok() })
            .boxed()
    }

    pub fn modified_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
        ts: DateTime<Utc>,
    ) -> impl Stream<Item = BoostedHexInfo> + 'a {
        sqlx::query_as::<_, BoostedHexInfo>(GET_MODIFIED_BOOSTED_HEX_INFO_SQL)
            .bind(ts)
            .fetch(db)
            .filter_map(|info| async move { info.ok() })
            .boxed()
    }

    impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for BoostedHexInfo {
        fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
            let period_length = Duration::seconds(row.get::<i32, &str>("period_length") as i64);
            let start_ts = to_start_ts(row.get::<i64, &str>("start_ts") as u64);
            let multipliers = row
                .get::<Vec<u8>, &str>("multipliers")
                .into_iter()
                .map(|v| v as u32)
                .collect::<Vec<_>>();
            let end_ts = to_end_ts(start_ts, period_length, multipliers.len());
            let boost_config_pubkey =
                Pubkey::from_str(row.get::<&str, &str>("boost_config_pubkey"))
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let boosted_hex_pubkey = Pubkey::from_str(row.get::<&str, &str>("boosted_hex_pubkey"))
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let version = row.get::<i32, &str>("version") as u32;
            Ok(Self {
                location: row.get::<i64, &str>("location") as u64,
                start_ts,
                end_ts,
                period_length,
                multipliers,
                boosted_hex_pubkey,
                boost_config_pubkey,
                version,
            })
        }
    }
}

fn to_start_ts(timestamp: u64) -> Option<DateTime<Utc>> {
    if timestamp == 0 {
        None
    } else {
        timestamp.to_timestamp().ok()
    }
}

fn to_end_ts(
    start_ts: Option<DateTime<Utc>>,
    period_length: Duration,
    num_multipliers: usize,
) -> Option<DateTime<Utc>> {
    start_ts.map(|ts| ts + period_length * num_multipliers as i32)
}
