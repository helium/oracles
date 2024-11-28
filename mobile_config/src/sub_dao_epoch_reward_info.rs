use crate::EpochPeriod;
use chrono::{DateTime, Utc};
use file_store::traits::{TimestampDecode, TimestampEncode};
use helium_proto::services::sub_dao::SubDaoEpochRewardInfo as SubDaoEpochRewardInfoProto;
use rust_decimal::prelude::*;
use std::ops::Range;

#[derive(Clone, Debug)]
pub struct ResolvedSubDaoEpochRewardInfo {
    pub epoch: u64,
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
    rewards_issued: u64,
    delegation_rewards_issued: u64,
    rewards_issued_at: DateTime<Utc>,
}

impl TryFrom<RawSubDaoEpochRewardInfo> for SubDaoEpochRewardInfoProto {
    type Error = anyhow::Error;

    fn try_from(info: RawSubDaoEpochRewardInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: info.epoch,
            epoch_address: info.epoch_address,
            sub_dao_address: info.sub_dao_address,
            rewards_issued: info.rewards_issued,
            delegation_rewards_issued: info.delegation_rewards_issued,
            rewards_issued_at: info.rewards_issued_at.encode_timestamp(),
        })
    }
}

impl TryFrom<SubDaoEpochRewardInfoProto> for ResolvedSubDaoEpochRewardInfo {
    type Error = anyhow::Error;

    fn try_from(info: SubDaoEpochRewardInfoProto) -> Result<Self, Self::Error> {
        let epoch_period: EpochPeriod = info.epoch.try_into()?;
        let epoch_rewards = Decimal::from(info.rewards_issued + info.delegation_rewards_issued);

        Ok(Self {
            epoch: info.epoch,
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
    use sqlx::{postgres::PgRow, FromRow, PgExecutor, Row};

    const GET_EPOCH_REWARD_INFO_SQL: &str = r#"
            SELECT
                address AS epoch_address,
                sub_dao AS sub_dao_address,
                epoch::BIGINT,
                delegation_rewards_issued::BIGINT AS rewards_issued,
                delegation_rewards_issued::BIGINT,
                rewards_issued_at::BIGINT
            FROM sub_dao_epoch_infos
            WHERE epoch = $1 AND sub_dao = $2
        "#;

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        epoch: u64,
        sub_dao_address: &str,
    ) -> anyhow::Result<Option<RawSubDaoEpochRewardInfo>> {
        let mut query: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(GET_EPOCH_REWARD_INFO_SQL);
        let res = query
            .build_query_as::<RawSubDaoEpochRewardInfo>()
            .bind(epoch as i64)
            .bind(sub_dao_address)
            .fetch_optional(db)
            .await?;
        tracing::info!("get_info: {:?}", res);
        Ok(res)
    }

    impl FromRow<'_, PgRow> for RawSubDaoEpochRewardInfo {
        fn from_row(row: &PgRow) -> sqlx::Result<Self> {
            let rewards_issued_at: DateTime<Utc> = (row.try_get::<i64, &str>("rewards_issued_at")?
                as u64)
                .to_timestamp()
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;

            Ok(Self {
                epoch: row.get::<i64, &str>("epoch") as u64,
                epoch_address: row.get::<String, &str>("epoch_address"),
                sub_dao_address: row.get::<String, &str>("sub_dao_address"),
                rewards_issued: row.get::<i64, &str>("rewards_issued") as u64,
                delegation_rewards_issued: row.get::<i64, &str>("delegation_rewards_issued") as u64,
                rewards_issued_at,
            })
        }
    }
}
