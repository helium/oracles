use crate::client::{hex_boosting_client::HexBoostingInfoResolver, ClientError};
use chrono::{DateTime, Duration, Utc};
use file_store::traits::TimestampDecode;
use futures::stream::{BoxStream, StreamExt};
use helium_proto::services::poc_mobile::BoostedHex as BoostedHexProto;
use helium_proto::BoostedHexInfoV1 as BoostedHexInfoProto;
use hextree::Cell;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, convert::TryFrom, num::NonZeroU32};

pub use helium_proto::BoostedHexDeviceTypeV1 as BoostedHexDeviceType;
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
    pub device_type: BoostedHexDeviceType,
}

impl TryFrom<BoostedHexInfoProto> for BoostedHexInfo {
    type Error = anyhow::Error;

    fn try_from(v: BoostedHexInfoProto) -> anyhow::Result<Self> {
        let period_length = Duration::seconds(v.period_length as i64);
        let device_type = v.device_type();
        let multipliers = v
            .multipliers
            .into_iter()
            .map(NonZeroU32::new)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow::anyhow!("multipliers cannot contain values of 0"))?;
        let start_ts = to_timestamp(v.start_ts);
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
            device_type,
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
            device_type: v.device_type.into(),
        })
    }
}

impl BoostedHexInfo {
    pub fn current_multiplier(&self, ts: DateTime<Utc>) -> Option<NonZeroU32> {
        if self.end_ts.is_some() && ts >= self.end_ts.unwrap() {
            // end time has been set and the current time is after the end time, so return None
            // to indicate that the hex is no longer boosted
            return None;
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
            Some(self.multipliers[index])
        } else {
            // start time has not been previously set, assume this is the first time rewarding this hex
            // and use the first multiplier
            Some(self.multipliers[0])
        }
    }

    fn matches_device_type(&self, device_type: &BoostedHexDeviceType) -> bool {
        self.device_type == *device_type || self.device_type == BoostedHexDeviceType::All
    }
}

fn device_type_from_str(s: &str) -> anyhow::Result<BoostedHexDeviceType> {
    match s {
        "cbrsIndoor" => Ok(BoostedHexDeviceType::CbrsIndoor),
        "cbrsOutdoor" => Ok(BoostedHexDeviceType::CbrsOutdoor),
        "wifiIndoor" => Ok(BoostedHexDeviceType::WifiIndoor),
        "wifiOutdoor" => Ok(BoostedHexDeviceType::WifiOutdoor),
        unknown => anyhow::bail!("unknown device type value: {unknown}"),
    }
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

#[derive(Debug, Clone, Default)]
pub struct BoostedHexes {
    hexes: HashMap<Cell, Vec<BoostedHexInfo>>,
}

impl BoostedHexes {
    pub fn new(hexes: Vec<BoostedHexInfo>) -> Self {
        let mut me = Self::default();
        for hex in hexes {
            me.insert(hex);
        }
        me
    }

    pub async fn get_all(
        hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
    ) -> anyhow::Result<Self> {
        let mut stream = hex_service_client
            .clone()
            .stream_boosted_hexes_info()
            .await?;

        let mut me = Self::default();
        while let Some(info) = stream.next().await {
            me.insert(info);
        }
        Ok(me)
    }

    pub fn is_boosted(&self, location: &Cell) -> bool {
        self.hexes.contains_key(location)
    }

    pub async fn get_modified(
        hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<Self> {
        let mut stream = hex_service_client
            .clone()
            .stream_modified_boosted_hexes_info(timestamp)
            .await?;

        let mut me = Self::default();
        while let Some(info) = stream.next().await {
            me.insert(info);
        }
        Ok(me)
    }

    pub fn get_current_multiplier(
        &self,
        location: Cell,
        device_type: BoostedHexDeviceType,
        ts: DateTime<Utc>,
    ) -> Option<NonZeroU32> {
        let current_multiplier = self
            .hexes
            .get(&location)?
            .iter()
            .filter(|info| info.matches_device_type(&device_type))
            .flat_map(|info| info.current_multiplier(ts))
            .map(|non_zero| non_zero.get())
            .sum::<u32>();

        NonZeroU32::new(current_multiplier)
    }

    pub fn count(&self) -> usize {
        self.hexes.len()
    }

    pub fn iter_hexes(&self) -> impl Iterator<Item = &BoostedHexInfo> {
        self.hexes.values().flatten()
    }

    pub fn get(&self, location: &Cell) -> Option<&Vec<BoostedHexInfo>> {
        self.hexes.get(location)
    }

    pub fn insert(&mut self, info: BoostedHexInfo) {
        #[cfg(test)]
        if info.end_ts.is_some_and(|end| end < Utc::now()) {
            // mobile-config does not deliver expired boosts from the database.
            // Tests using this struct to mimic mobile-config should uphold the
            // same contract.
            panic!("BoostedHexes should not contain expired boosts");
        }

        self.hexes.entry(info.location).or_default().push(info);
    }
}

pub(crate) mod db {

    use super::{to_timestamp, BoostedHexInfo};
    use chrono::{DateTime, Duration, Utc};
    use futures::stream::{Stream, StreamExt};
    use hextree::Cell;
    use solana_sdk::pubkey::Pubkey;
    use sqlx::{PgExecutor, Row};
    use std::num::NonZeroU32;
    use std::ops::Deref;
    use std::str::FromStr;

    const GET_BOOSTED_HEX_INFO_SQL: &str = r#"
            WITH boosted_hexes_replacement AS (
                SELECT
                    h.*,
                    CASE
                        WHEN start_ts = 0 THEN 0
                        ELSE h.start_ts + (c.period_length * length(h.boosts_by_period))
                    END AS end_ts
                FROM boost_configs c
                INNER JOIN boosted_hexes h ON c.address = h.boost_config
            )
            SELECT
                CAST(hexes.location as bigint),
                CAST(hexes.start_ts as bigint),
                CAST(hexes.end_ts as bigint),
                config.period_length,
                hexes.boosts_by_period as multipliers,
                hexes.address as boosted_hex_pubkey,
                config.address as boost_config_pubkey,
                hexes.version,
                hexes.device_type
            from boosted_hexes_replacement hexes
            join boost_configs config on hexes.boost_config = config.address
            WHERE
                hexes.start_ts = 0
                OR hexes.end_ts > date_part('epoch', $1)
        "#;

    // NOTE(mj): modified hexes should be returned regardless of expiration status
    const GET_MODIFIED_BOOSTED_HEX_INFO_SQL: &str = r#"
            WITH boosted_hexes_replacement AS (
                SELECT
                    h.*,
                    CASE
                        WHEN start_ts = 0 THEN 0
                        ELSE h.start_ts + (c.period_length * length(h.boosts_by_period))
                    END AS end_ts
                FROM boost_configs c
                INNER JOIN boosted_hexes h ON c.address = h.boost_config
            )
            SELECT
                CAST(hexes.location AS bigint),
                CAST(hexes.start_ts AS bigint),
                config.period_length,
                hexes.boosts_by_period AS multipliers,
                hexes.address AS boosted_hex_pubkey,
                config.address AS boost_config_pubkey,
                hexes.version,
                hexes.device_type
            FROM boosted_hexes_replacement hexes
            JOIN boost_configs config ON hexes.boost_config = config.address
            WHERE hexes.refreshed_at > $1
        "#;

    pub fn all_info_stream_with_time_now<'a>(
        db: impl PgExecutor<'a> + 'a,
    ) -> impl Stream<Item = BoostedHexInfo> + 'a {
        all_info_stream(db, Utc::now())
    }

    fn all_info_stream<'a>(
        db: impl PgExecutor<'a> + 'a,
        now: DateTime<Utc>,
    ) -> impl Stream<Item = BoostedHexInfo> + 'a {
        sqlx::query_as::<_, BoostedHexInfo>(GET_BOOSTED_HEX_INFO_SQL)
            .bind(now)
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
            let start_ts = to_timestamp(row.get::<i64, &str>("start_ts") as u64);
            let multipliers = row
                .get::<Vec<u8>, &str>("multipliers")
                .into_iter()
                .map(|v| NonZeroU32::new(v as u32))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    sqlx::Error::Decode(Box::from("multipliers cannot contain values of 0"))
                })?;
            let end_ts = to_timestamp(row.get::<i64, &str>("end_ts") as u64);
            let boost_config_pubkey =
                Pubkey::from_str(row.get::<&str, &str>("boost_config_pubkey"))
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let boosted_hex_pubkey = Pubkey::from_str(row.get::<&str, &str>("boosted_hex_pubkey"))
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            let version = row.get::<i32, &str>("version") as u32;

            let location = Cell::try_from(row.get::<i64, &str>("location"))
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

            type MaybeJsonb = Option<sqlx::types::Json<String>>;
            let device_type_jsonb = row.get::<MaybeJsonb, &str>("device_type");
            let device_type = match device_type_jsonb {
                None => super::BoostedHexDeviceType::All,
                Some(val) => super::device_type_from_str(val.deref())
                    .map_err(|e| sqlx::Error::Decode(e.into()))?,
            };

            Ok(Self {
                location,
                start_ts,
                end_ts,
                period_length,
                multipliers,
                boosted_hex_pubkey,
                boost_config_pubkey,
                version,
                device_type,
            })
        }
    }
}

fn to_timestamp(timestamp: u64) -> Option<DateTime<Utc>> {
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
    use sqlx::PgPool;
    use std::str::FromStr;

    const BOOST_HEX_PUBKEY: &str = "J9JiLTpjaShxL8eMvUs8txVw6TZ36E38SiJ89NxnMbLU";
    const BOOST_HEX_CONFIG_PUBKEY: &str = "BZM1QTud72B2cpTW7PhEnFmRX7ZWzvY7DpPpNJJuDrWG";

    #[test]
    fn boosted_hexes_accumulate_multipliers() -> anyhow::Result<()> {
        let cell = Cell::from_raw(631252734740306943)?;
        let now = Utc::now();

        let hexes = vec![
            BoostedHexInfo {
                location: cell,
                start_ts: None,
                end_ts: None,
                period_length: Duration::seconds(2592000),
                multipliers: vec![NonZeroU32::new(2).unwrap()],
                boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY)?,
                boost_config_pubkey: Pubkey::from_str(BOOST_HEX_CONFIG_PUBKEY)?,
                version: 0,
                device_type: BoostedHexDeviceType::All,
            },
            BoostedHexInfo {
                location: cell,
                start_ts: None,
                end_ts: None,
                period_length: Duration::seconds(2592000),
                multipliers: vec![NonZeroU32::new(3).unwrap()],
                boosted_hex_pubkey: Pubkey::from_str(BOOST_HEX_PUBKEY)?,
                boost_config_pubkey: Pubkey::from_str(BOOST_HEX_CONFIG_PUBKEY)?,
                version: 0,
                device_type: BoostedHexDeviceType::CbrsIndoor,
            },
        ];

        let boosted_hexes = BoostedHexes::new(hexes);
        let boosts = boosted_hexes.get(&cell).expect("boosts for test cell");
        assert_eq!(boosts.len(), 2, "a hex can be boosted multiple times");

        assert_eq!(
            boosted_hexes.get_current_multiplier(cell, BoostedHexDeviceType::CbrsIndoor, now),
            NonZeroU32::new(5),
            "Specific boosts stack with ::ALL"
        );
        assert_eq!(
            boosted_hexes.get_current_multiplier(cell, BoostedHexDeviceType::WifiIndoor, now),
            NonZeroU32::new(2),
            "Missing boosts still return ::ALL"
        );

        Ok(())
    }

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
            device_type: BoostedHexDeviceType::All.into(),
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
            device_type: BoostedHexDeviceType::All.into(),
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
            device_type: BoostedHexDeviceType::All.into(),
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

    mod metadata_db {
        /// Table creation was taken from a dump of the metadata db.
        /// `device_type` was added to boosted_hexes as a jsonb field because the
        /// mobile_hotspot_infos table has a device_type column that maps to an
        /// enum, and it is a nullable jsonb column.
        ///
        /// When the `boosted_hexes` or `boost_configs` tables from the metadata_db
        /// are updated, these tests will not break until the new table formats are
        /// put into `create_tables()`.
        use super::*;

        #[sqlx::test]
        async fn parse_boosted_hex_info_from_database(pool: PgPool) -> anyhow::Result<()> {
            let boost_config_address = Pubkey::new_unique();
            let now = Utc::now();

            create_tables(&pool).await?;
            insert_boost_config(&pool, &boost_config_address, now).await?;

            let device_types = vec![
                None, // legacy boosted hex with NULL device_type
                Some(serde_json::json!("cbrsIndoor")),
                Some(serde_json::json!("cbrsOutdoor")),
                Some(serde_json::json!("wifiIndoor")),
                Some(serde_json::json!("wifiOutdoor")),
                // Some(serde_json::json!(null)) // this is different from None, and will/should break parsing as it's not part of the enum
            ];
            for device_type in device_types {
                insert_boosted_hex(&pool, &boost_config_address, now, device_type, Some(now))
                    .await?;
            }

            assert_eq!(
                5,
                boosted_hexes_count(&pool).await?,
                "there should be 1 of each type of boosted hex"
            );
            assert_eq!(
                5,
                streamed_hexes_count(&pool).await?,
                "not all rows were able to parse"
            );

            Ok(())
        }

        #[sqlx::test]
        async fn filter_expired_boosted_hexes(pool: PgPool) -> anyhow::Result<()> {
            let boost_config_address = Pubkey::new_unique();
            let now = Utc::now();

            create_tables(&pool).await?;
            insert_boost_config(&pool, &boost_config_address, now).await?;

            let times = vec![
                None,                            // unstarted
                Some(now),                       // still boosting
                Some(now - Duration::days(400)), // expired
            ];

            for time in times {
                insert_boosted_hex(
                    &pool,
                    &boost_config_address,
                    now,
                    Some(serde_json::json!("cbrsIndoor")),
                    time,
                )
                .await?;
            }

            assert_eq!(3, boosted_hexes_count(&pool).await?);
            assert_eq!(2, streamed_hexes_count(&pool).await?);

            Ok(())
        }

        async fn create_tables(pool: &PgPool) -> anyhow::Result<()> {
            const CREATE_BOOSTED_HEXES_TABLE: &str = r#"
                CREATE TABLE
                    boosted_hexes (
                        address character varying(255) NOT NULL PRIMARY KEY,
                        boost_config character varying(255) NULL,
                        location numeric NULL,
                        start_ts numeric NULL,
                        reserved numeric[] NULL,
                        bump_seed integer NULL,
                        boosts_by_period bytea NULL,
                        version integer NULL,
                        refreshed_at timestamp with time zone NULL,
                        created_at timestamp with time zone NOT NULL,
                        device_type jsonb
                    )
            "#;

            const CREATE_BOOST_CONFIG_TABLE: &str = r#"
                CREATE TABLE
                    boost_configs (
                        address character varying(255) NOT NULL PRIMARY KEY,
                        price_oracle character varying(255) NULL,
                        payment_mint character varying(255) NULL,
                        sub_dao character varying(255) NULL,
                        rent_reclaim_authority character varying(255) NULL,
                        boost_price numeric NULL,
                        period_length integer NULL,
                        minimum_periods integer NULL,
                        bump_seed integer NULL,
                        start_authority character varying(255) NULL,
                        refreshed_at timestamp with time zone NULL,
                        created_at timestamp with time zone NOT NULL
                    )
            "#;

            sqlx::query(CREATE_BOOSTED_HEXES_TABLE)
                .execute(pool)
                .await?;
            sqlx::query(CREATE_BOOST_CONFIG_TABLE).execute(pool).await?;
            Ok(())
        }

        async fn insert_boost_config(
            pool: &PgPool,
            boost_config_address: &Pubkey,
            created_at: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            const INSERT_BOOST_CONFIG: &str = r#"
                INSERT INTO boost_configs (
                    "boost_price", "bump_seed", "minimum_periods", "period_length", "refreshed_at",

                    -- pubkeys
                    "price_oracle",
                    "payment_mint",
                    "rent_reclaim_authority",
                    "start_authority",
                    "sub_dao",

                    -- our values
                    "address",
                    "created_at"
                )
                VALUES (
                    '5000', 250, 6, 2592000, '2024-03-12 21:13:52.692+00',

                    $1, $2, $3, $4, $5, $6, $7
                )
            "#;

            sqlx::query(INSERT_BOOST_CONFIG)
                .bind(Pubkey::new_unique().to_string()) // price_oracle
                .bind(Pubkey::new_unique().to_string()) // payment_mint
                .bind(Pubkey::new_unique().to_string()) // rent_reclaim_authority
                .bind(Pubkey::new_unique().to_string()) // start_authority
                .bind(Pubkey::new_unique().to_string()) // sub_dao
                // --
                .bind(boost_config_address.to_string()) // address
                .bind(created_at) // created_at
                .execute(pool)
                .await?;

            Ok(())
        }

        async fn insert_boosted_hex(
            pool: &PgPool,
            boost_config_address: &Pubkey,
            created_at: DateTime<Utc>,
            device_type: Option<serde_json::Value>,
            start_ts: Option<DateTime<Utc>>,
        ) -> anyhow::Result<()> {
            const INSERT_BOOSTED_HEX: &str = r#"
                INSERT INTO boosted_hexes (
                    "boosts_by_period", "bump_seed", "location", "refreshed_at", "reserved", "version",

                    -- our values
                    "address",
                    "boost_config",
                    "created_at",
                    "device_type",
                    "start_ts"
                )
                VALUES (
                    'ZGRkZGRk', 1, '631798453297853439', '2024-03-12 21:13:53.773+00', '{0,0,0,0,0,0,0,0}', 1,

                    $1, $2, $3, $4, $5
                )
            "#;

            sqlx::query(INSERT_BOOSTED_HEX)
                .bind(Pubkey::new_unique().to_string()) // address
                .bind(boost_config_address.to_string()) // boost_config
                .bind(created_at) // created_at
                .bind(device_type) // device_type
                .bind(start_ts.map(|t| t.timestamp()).unwrap_or_default()) // start_ts
                .execute(pool)
                .await?;

            Ok(())
        }

        async fn boosted_hexes_count(pool: &PgPool) -> anyhow::Result<i64> {
            let count: i64 = sqlx::query_scalar("select count(*) from boosted_hexes")
                .fetch_one(pool)
                .await?;
            Ok(count)
        }

        async fn streamed_hexes_count(pool: &PgPool) -> anyhow::Result<usize> {
            // If a row cannot be parsed, it is dropped from the stream with no erros.
            let mut infos = super::db::all_info_stream_with_time_now(pool);
            let mut count = 0;
            while let Some(_info) = infos.next().await {
                // println!("info: {_info:?}");
                count += 1;
            }
            Ok(count)
        }
    }
}
