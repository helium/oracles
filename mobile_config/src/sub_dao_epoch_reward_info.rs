use crate::EpochPeriod;
use chrono::{DateTime, Utc};
use file_store::traits::{TimestampDecode, TimestampEncode};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::SubDaoEpochRewardInfo as SubDaoEpochRewardInfoProto;
use rust_decimal::prelude::*;
use sqlx::FromRow;
use std::ops::Range;

#[derive(Clone, Debug)]
pub struct ResolvedSubDaoEpochRewardInfo {
    pub epoch: u64,
    pub epoch_address: PublicKeyBinary,
    pub sub_dao_address: PublicKeyBinary,
    pub epoch_period: Range<DateTime<Utc>>,
    pub epoch_emissions: Decimal,
    pub rewards_issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, FromRow)]
pub struct RawSubDaoEpochRewardInfo {
    #[sqlx(try_from = "i64")]
    pub epoch: u64,
    pub epoch_address: PublicKeyBinary,
    pub sub_dao_address: PublicKeyBinary,
    #[sqlx(try_from = "i64")]
    pub sub_dao_utility_score: u64,
    #[sqlx(try_from = "i64")]
    pub total_utility_score: u64,
    #[sqlx(try_from = "i64")]
    pub total_emissions: u64,
    pub rewards_issued_at: DateTime<Utc>,
}

// server goes from raw to proto, client goes from proto to resolved
impl TryFrom<RawSubDaoEpochRewardInfo> for SubDaoEpochRewardInfoProto {
    type Error = anyhow::Error;

    fn try_from(info: RawSubDaoEpochRewardInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            epoch: info.epoch,
            address: info.epoch_address.into(),
            sub_dao: info.sub_dao_address.into(),
            sub_dao_utility_score: info.sub_dao_utility_score,
            total_utility_score: info.total_utility_score,
            total_emissions: info.total_emissions,
            rewards_issued_at: info.rewards_issued_at.encode_timestamp(),
        })
    }
}

// server returns the proto struct to client, client resolves to the resolved struct
impl TryFrom<SubDaoEpochRewardInfoProto> for ResolvedSubDaoEpochRewardInfo {
    type Error = anyhow::Error;

    fn try_from(info: SubDaoEpochRewardInfoProto) -> Result<Self, Self::Error> {
        let epoch_period: EpochPeriod = info.epoch.try_into()?;
        // todo: confirm rounding requirements here
        let epoch_rewards = Decimal::from(
            info.total_emissions * (info.sub_dao_utility_score / info.total_utility_score),
        );

        Ok(Self {
            epoch: info.epoch,
            epoch_address: info.address.into(),
            sub_dao_address: info.sub_dao.into(),
            epoch_period: epoch_period.period,
            epoch_emissions: epoch_rewards,
            rewards_issued_at: info.rewards_issued_at.to_timestamp()?,
        })
    }
}

pub(crate) mod db {

    use crate::sub_dao_epoch_reward_info::RawSubDaoEpochRewardInfo;
    use helium_crypto::PublicKeyBinary;
    use sqlx::PgExecutor;

    const GET_EPOCH_REWARD_INFO_SQL: &str = r#"
            SELECT
                t_subdao.address AS epoch_address,
                t_subdao.sub_dao AS sub_dao_address,
                t_subdao.epoch,
                t_subdao.utility_score,
                t_dao.total_utility_score,
                t_dao.total_rewards,
                t_subdao.rewards_issued_at
            FROM sub_dao_epoch_infos t_subdao
            JOIN dao_epoch_infos t_dao ON t_subdao.epoch = t_dao.epoch
            WHERE t_subdao.epoch = $1 AND t_subdao.sub_dao = $2
        "#;

    pub async fn get_info(
        db: impl PgExecutor<'_>,
        epoch: u64,
        sub_dao: PublicKeyBinary,
    ) -> anyhow::Result<Option<RawSubDaoEpochRewardInfo>> {
        let mut query: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(GET_EPOCH_REWARD_INFO_SQL);
        Ok(query
            .build_query_as::<RawSubDaoEpochRewardInfo>()
            .bind(epoch as i64)
            .bind(sub_dao.to_string())
            .fetch_optional(db)
            .await?)
    }
}
