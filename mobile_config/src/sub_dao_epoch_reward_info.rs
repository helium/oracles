use crate::EpochInfo;
use chrono::{DateTime, Utc};
use file_store::traits::{TimestampDecode, TimestampDecodeError, TimestampEncode};
use helium_proto::services::sub_dao::SubDaoEpochRewardInfo as SubDaoEpochRewardInfoProto;
use rust_decimal::prelude::*;
use std::ops::Range;

#[derive(Clone, Debug)]
pub struct EpochRewardInfo {
    pub epoch_day: u64,
    pub epoch_address: String,
    pub sub_dao_address: String,
    pub epoch_period: Range<DateTime<Utc>>,
    /// Total mobile sub-DAO emissions for the epoch (100%): the HNT this rewarder
    /// distributes (`hnt_rewards_issued`) plus the delegation rewards already paid
    /// to veHNT holders on-chain.
    pub epoch_emissions: Decimal,
    /// The slice of `epoch_emissions` this rewarder is responsible for
    /// distributing (service providers + data transfer). Under the HIP-149 3×
    /// cap / backstop the chain shifts HNT between this and the delegation
    /// rewards, so it is no longer a fixed 94% of `epoch_emissions`; the rewarder
    /// must distribute exactly this amount — no more, no less.
    pub hnt_rewards_issued: Decimal,
    /// The portion of `epoch_emissions` paid to veHNT delegators on-chain
    /// (`epoch_emissions - hnt_rewards_issued`). The rewarder does not distribute
    /// this; it is carried so the full on-chain split is available without another
    /// mobile-config change.
    pub delegation_rewards_issued: Decimal,
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
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),
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
        let hnt_rewards_issued = Decimal::from(info.hnt_rewards_issued);
        let delegation_rewards_issued = Decimal::from(info.delegation_rewards_issued);

        Ok(Self {
            epoch_day: info.epoch,
            epoch_address: info.epoch_address,
            sub_dao_address: info.sub_dao_address,
            epoch_period: epoch_period.period,
            epoch_emissions: hnt_rewards_issued + delegation_rewards_issued,
            hnt_rewards_issued,
            delegation_rewards_issued,
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
                hnt_rewards_issued::BIGINT,
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
        tracing::debug!("get_info: {:?}", res);
        Ok(res)
    }

    impl FromRow<'_, PgRow> for RawSubDaoEpochRewardInfo {
        fn from_row(row: &PgRow) -> sqlx::Result<Self> {
            let rewards_issued_at: DateTime<Utc> = (row.try_get::<i64, &str>("rewards_issued_at")?
                as u64)
                .to_timestamp()
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;

            let hnt_rewards_issued = row.get::<i64, &str>("hnt_rewards_issued") as u64;
            let delegation_rewards_issued =
                row.get::<i64, &str>("delegation_rewards_issued") as u64;

            if hnt_rewards_issued == 0 && delegation_rewards_issued == 0 {
                return Err(sqlx::Error::Decode(Box::new(sqlx::Error::Decode(
                    Box::from("hnt_rewards_issued and delegation_rewards_issued are 0, epoch is not closed"),
                ))));
            }

            Ok(Self {
                epoch: row.get::<i64, &str>("epoch") as u64,
                epoch_address: row.get::<String, &str>("epoch_address"),
                sub_dao_address: row.get::<String, &str>("sub_dao_address"),
                hnt_rewards_issued,
                delegation_rewards_issued,
                rewards_issued_at,
            })
        }
    }
}
