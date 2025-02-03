use crate::client::{hex_boosting_client::HexBoostingInfoResolver, ClientError};
use chrono::{DateTime, Duration, Utc};
use file_store::traits::TimestampDecode;
use futures::stream::{BoxStream, StreamExt};
use helium_proto::services::poc_mobile::BoostedHex as BoostedHexProto;
use helium_proto::BoostedHexInfoV1 as BoostedHexInfoProto;
use hextree::Cell;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, convert::TryFrom, num::NonZeroU32};

pub type BoostedHexInfoStream = BoxStream<'static, BoostedHexInfo>;

lazy_static::lazy_static! {
    static ref PERIOD_IN_SECONDS: Duration = Duration::seconds(60 * 60 * 24 * 30);
}

#[derive(Clone, Debug)]
pub struct BoostedHexInfo {
    pub location: Cell,
    pub start_ts: Option<DateTime<Utc>>,
    pub end_ts: Option<DateTime<Utc>>,
    pub period_length: Duration,
    pub multipliers: Vec<NonZeroU32>,
    pub boosted_hex_pubkey: Pubkey,
    pub boost_config_pubkey: Pubkey,
    pub version: u32,
}

impl TryFrom<BoostedHexInfoProto> for BoostedHexInfo {
    type Error = anyhow::Error;
    fn try_from(v: BoostedHexInfoProto) -> anyhow::Result<Self> {
        let period_length = Duration::seconds(v.period_length as i64);
        let multipliers = v
            .multipliers
            .into_iter()
            .map(NonZeroU32::new)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow::anyhow!("multipliers cannot contain values of 0"))?;
        let start_ts = to_start_ts(v.start_ts);
        let end_ts = to_end_ts(start_ts, period_length, multipliers.len());
        let boosted_hex_pubkey: Pubkey = Pubkey::try_from(v.boosted_hex_pubkey.as_slice())?;
        let boost_config_pubkey: Pubkey = Pubkey::try_from(v.boost_config_pubkey.as_slice())?;
        Ok(Self {
            location: v.location.try_into()?,
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
        let multipliers = v
            .multipliers
            .into_iter()
            .map(|v| v.get())
            .collect::<Vec<_>>();
        Ok(Self {
            location: v.location.into_raw(),
            start_ts,
            end_ts,
            period_length: v.period_length.num_seconds() as u32,
            multipliers,
            boosted_hex_pubkey: v.boosted_hex_pubkey.to_bytes().into(),
            boost_config_pubkey: v.boost_config_pubkey.to_bytes().into(),
            version: v.version,
        })
    }
}

impl BoostedHexInfo {
    pub fn current_multiplier(&self, ts: DateTime<Utc>) -> anyhow::Result<Option<NonZeroU32>> {
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
    pub hexes: HashMap<Cell, BoostedHexInfo>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct BoostedHex {
    pub location: Cell,
    pub multiplier: NonZeroU32,
}

impl TryFrom<BoostedHexProto> for BoostedHex {
    type Error = anyhow::Error;

    fn try_from(value: BoostedHexProto) -> Result<Self, Self::Error> {
        let location = Cell::from_raw(value.location)?;
        let multiplier = NonZeroU32::new(value.multiplier)
            .ok_or_else(|| anyhow::anyhow!("multiplier cannot be 0"))?;

        Ok(Self {
            location,
            multiplier,
        })
    }
}

impl BoostedHexes {
    pub fn new(hexes: Vec<BoostedHexInfo>) -> Self {
        let hexes = hexes
            .into_iter()
            .map(|info| (info.location, info))
            .collect();
        Self { hexes }
    }

    pub async fn get_all(
        hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
    ) -> anyhow::Result<Self> {
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

    pub fn is_boosted(&self, location: &Cell) -> bool {
        self.hexes.contains_key(location)
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

    pub fn get_current_multiplier(&self, location: Cell, ts: DateTime<Utc>) -> Option<NonZeroU32> {
        self.hexes
            .get(&location)
            .and_then(|info| info.current_multiplier(ts).ok()?)
    }
}

impl coverage_map::BoostedHexMap for BoostedHexes {
    fn get_current_multiplier(&self, location: Cell, ts: DateTime<Utc>) -> Option<NonZeroU32> {
        self.get_current_multiplier(location, ts)
    }
}

pub(crate) mod db {
    use super::{to_end_ts, to_start_ts, BoostedHexInfo};
    use chrono::{DateTime, Duration, Utc};
    use futures::stream::{Stream, StreamExt};
    use hextree::Cell;
    use solana_sdk::pubkey::Pubkey;
    use sqlx::{PgExecutor, Row};
    use std::num::NonZeroU32;
    use std::str::FromStr;

    const GET_BOOSTED_HEX_INFO_SQL: &str = r#"
            select 
                CAST(hexes.location AS bigint),
                CAST(hexes.start_ts AS bigint),
                CAST(config.period_length AS bigint),
                hexes.boosts_by_period as multipliers,
                hexes.address as boosted_hex_pubkey, 
                config.address as boost_config_pubkey,
                CAST(hexes.version AS integer)
            from boosted_hexes hexes
            join boost_configs config on hexes.boost_config = config.address
        "#;

    // TODO: reuse with string above
    const GET_MODIFIED_BOOSTED_HEX_INFO_SQL: &str = r#"
            select 
                CAST(hexes.location AS bigint),
                CAST(hexes.start_ts AS bigint),
                CAST(config.period_length AS bigint),
                hexes.boosts_by_period as multipliers,
                hexes.address as boosted_hex_pubkey, 
                config.address as boost_config_pubkey,
                CAST(hexes.version AS integer)
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
            let period_length = Duration::seconds(row.get::<i64, &str>("period_length"));
            let start_ts = to_start_ts(row.get::<i64, &str>("start_ts") as u64);
            let multipliers = row
                .get::<Vec<u8>, &str>("multipliers")
                .into_iter()
                .map(|v| NonZeroU32::new(v as u32))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    sqlx::Error::Decode(Box::from("multipliers cannot contain values of 0"))
                })?;
            let end_ts = to_end_ts(start_ts, period_length, multipliers.len());
            let boost_config_pubkey =
                Pubkey::from_str(row.get::<&str, &str>("boost_config_pubkey"))
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let boosted_hex_pubkey = Pubkey::from_str(row.get::<&str, &str>("boosted_hex_pubkey"))
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let version = row.get::<i32, &str>("version") as u32;

            let location = Cell::try_from(row.get::<i64, &str>("location"))
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

            Ok(Self {
                location,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;
    use hextree::Cell;
    use std::str::FromStr;

    const BOOST_HEX_PUBKEY: &str = "J9JiLTpjaShxL8eMvUs8txVw6TZ36E38SiJ89NxnMbLU";
    const BOOST_HEX_CONFIG_PUBKEY: &str = "BZM1QTud72B2cpTW7PhEnFmRX7ZWzvY7DpPpNJJuDrWG";

    #[test]
    fn boosted_hex_from_proto_valid_not_started() -> anyhow::Result<()> {
        let proto = BoostedHexInfoProto {
            location: 631252734740306943,
            start_ts: 0,
            end_ts: 0,
            period_length: 2592000,
            multipliers: vec![2, 10, 15, 35],
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY)
                .unwrap()
                .to_bytes()
                .to_vec(),
            boost_config_pubkey: Pubkey::from_str(BOOST_HEX_CONFIG_PUBKEY)
                .unwrap()
                .to_bytes()
                .to_vec(),
            version: 1,
        };

        let msg = BoostedHexInfo::try_from(proto)?;
        assert_eq!(Cell::from_raw(631252734740306943)?, msg.location);
        assert_eq!(None, msg.start_ts);
        assert_eq!(None, msg.end_ts);
        assert_eq!(2592000, msg.period_length.num_seconds());
        assert_eq!(4, msg.multipliers.len());
        assert_eq!(2, msg.multipliers[0].get());
        assert_eq!(10, msg.multipliers[1].get());
        assert_eq!(15, msg.multipliers[2].get());
        assert_eq!(35, msg.multipliers[3].get());
        assert_eq!(
            Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            msg.boosted_hex_pubkey
        );
        assert_eq!(
            Pubkey::from_str(BOOST_HEX_CONFIG_PUBKEY).unwrap(),
            msg.boost_config_pubkey
        );
        assert_eq!(1, msg.version);
        Ok(())
    }

    #[test]
    fn boosted_hex_from_proto_valid_started() -> anyhow::Result<()> {
        let proto = BoostedHexInfoProto {
            location: 631252734740306943,
            start_ts: 1710378000,
            end_ts: 1720746000,
            period_length: 2592000,
            multipliers: vec![2, 10, 15, 35],
            boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY)
                .unwrap()
                .to_bytes()
                .to_vec(),
            boost_config_pubkey: Pubkey::from_str(BOOST_HEX_CONFIG_PUBKEY)
                .unwrap()
                .to_bytes()
                .to_vec(),
            version: 1,
        };

        let msg = BoostedHexInfo::try_from(proto)?;
        assert_eq!(Cell::from_raw(631252734740306943)?, msg.location);
        assert_eq!(parse_dt("2024-03-14 01:00:00"), msg.start_ts.unwrap());
        assert_eq!(parse_dt("2024-07-12 01:00:00"), msg.end_ts.unwrap());
        assert_eq!(2592000, msg.period_length.num_seconds());
        assert_eq!(4, msg.multipliers.len());
        assert_eq!(2, msg.multipliers[0].get());
        assert_eq!(10, msg.multipliers[1].get());
        assert_eq!(15, msg.multipliers[2].get());
        assert_eq!(35, msg.multipliers[3].get());
        assert_eq!(
            Pubkey::from_str(BOOST_HEX_PUBKEY).unwrap(),
            msg.boosted_hex_pubkey
        );
        assert_eq!(
            Pubkey::from_str(BOOST_HEX_CONFIG_PUBKEY).unwrap(),
            msg.boost_config_pubkey
        );
        assert_eq!(1, msg.version);
        Ok(())
    }

    #[test]
    fn boosted_hex_from_proto_invalid_multiplier() -> anyhow::Result<()> {
        let proto = BoostedHexInfoProto {
            location: 631252734740306943,
            start_ts: 1712624400000,
            end_ts: 0,
            period_length: 2592000,
            multipliers: vec![2, 0, 15, 35],
            boosted_hex_pubkey: BOOST_HEX_PUBKEY.as_bytes().to_vec(),
            boost_config_pubkey: BOOST_HEX_CONFIG_PUBKEY.as_bytes().to_vec(),
            version: 1,
        };
        assert_eq!(
            "multipliers cannot contain values of 0",
            BoostedHexInfo::try_from(proto).err().unwrap().to_string()
        );
        Ok(())
    }

    fn parse_dt(dt: &str) -> DateTime<Utc> {
        NaiveDateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S")
            .expect("unable_to_parse")
            .and_utc()
    }
}
