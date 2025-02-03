use crate::EpochInfo;
use chrono::{DateTime, Utc};
use file_store::traits::{TimestampDecode, TimestampEncode};
use helium_proto::services::sub_dao::SubDaoEpochRewardInfo as SubDaoEpochRewardInfoProto;
use rust_decimal::prelude::*;
use std::ops::Range;

#[derive(Clone, Debug)]
pub struct EpochRewardInfo {
    pub epoch_day: u64,
    pub epoch_address: String,
    pub sub_dao_address: String,
    pub epoch_period: Range<DateTime<Utc>>,
    pub epoch_emissions: Decimal,
    pub rewards_issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct RawSubDaoEpochRewardInfo {
    epoch: u64,
    epoch_address: String,
    sub_dao_address: String,
    hnt_rewards_issued: u64,
    delegation_rewards_issued: u64,
    rewards_issued_at: DateTime<Utc>,
}

#[derive(thiserror::Error, Debug)]
pub enum SubDaoRewardInfoParseError {
    #[error("file_store: {0}")]
    FileStore(#[from] file_store::Error),
}

impl From<RawSubDaoEpochRewardInfo> for SubDaoEpochRewardInfoProto {
    fn from(info: RawSubDaoEpochRewardInfo) -> Self {
        Self {
            epoch: info.epoch,
            epoch_address: info.epoch_address,
            sub_dao_address: info.sub_dao_address,
            hnt_rewards_issued: info.hnt_rewards_issued,
            delegation_rewards_issued: info.delegation_rewards_issued,
            rewards_issued_at: info.rewards_issued_at.encode_timestamp(),
        }
    }
}

impl TryFrom<SubDaoEpochRewardInfoProto> for EpochRewardInfo {
    type Error = SubDaoRewardInfoParseError;

    fn try_from(info: SubDaoEpochRewardInfoProto) -> Result<Self, Self::Error> {
        let epoch_period: EpochInfo = info.epoch.into();
        let epoch_rewards = Decimal::from(info.hnt_rewards_issued + info.delegation_rewards_issued);

        Ok(Self {
            epoch_day: info.epoch,
            epoch_address: info.epoch_address,
            sub_dao_address: info.sub_dao_address,
            epoch_period: epoch_period.period,
            epoch_emissions: epoch_rewards,
            rewards_issued_at: info.rewards_issued_at.to_timestamp()?,
        })
    }
}

pub(crate) mod db {
    use crate::sub_dao_epoch_reward_info::RawSubDaoEpochRewardInfo;
    use chrono::{DateTime, Utc};
    use file_store::traits::TimestampDecode;
    use sqlx::postgres::PgRow;
    use sqlx::{FromRow, PgExecutor, Row};

    const GET_EPOCH_REWARD_INFO_SQL: &str = r#"
            SELECT
                address AS epoch_address,
                sub_dao AS sub_dao_address,
                epoch::BIGINT,
                hnt_rewards_issued::BIGINT,
                delegation_rewards_issued::BIGINT,
                rewards_issued_at::BIGINT
            FROM sub_dao_epoch_infos
            WHERE epoch = $1 AND sub_dao = $2
        "#;

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        epoch: u64,
        sub_dao: &str,
    ) -> anyhow::Result<Option<RawSubDaoEpochRewardInfo>> {
        let mut query: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(GET_EPOCH_REWARD_INFO_SQL);
        let res = query
            .build_query_as::<RawSubDaoEpochRewardInfo>()
            .bind(epoch as i64)
            .bind(sub_dao)
            .fetch_optional(db)
            .await;
        tracing::info!("get_info: {:?}", res);
        Ok(res?)
    }

    impl FromRow<'_, PgRow> for RawSubDaoEpochRewardInfo {
        fn from_row(row: &PgRow) -> sqlx::Result<Self> {
            let rewards_issued_at: DateTime<Utc> = (row.try_get::<i64, &str>("rewards_issued_at")?
                as u64)
                .to_timestamp()
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;

            let hnt_rewards_issued = row.get::<i64, &str>("hnt_rewards_issued") as u64;
            if hnt_rewards_issued == 0 {
                return Err(sqlx::Error::Decode(Box::new(sqlx::Error::Decode(
                    Box::from("hnt_rewards_issued is 0"),
                ))));
            };

            let delegation_rewards_issued =
                row.get::<i64, &str>("delegation_rewards_issued") as u64;
            if delegation_rewards_issued == 0 {
                return Err(sqlx::Error::Decode(Box::new(sqlx::Error::Decode(
                    Box::from("delegation_rewards_issued is 0"),
                ))));
            };

            Ok(Self {
                epoch: row.try_get::<i64, &str>("epoch")? as u64,
                epoch_address: row.try_get::<String, &str>("epoch_address")?,
                sub_dao_address: row.try_get::<String, &str>("sub_dao_address")?,
                hnt_rewards_issued,
                delegation_rewards_issued,
                rewards_issued_at,
            })
        }
    }
}
