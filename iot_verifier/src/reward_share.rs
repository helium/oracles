use crate::poc_report::ReportType;
use chrono::{DateTime, Duration, Utc};
use file_store::{iot_valid_poc::IotValidPoc, traits::TimestampEncode};
use futures::stream::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora as proto;
use lazy_static::lazy_static;
use rust_decimal::prelude::*;
use sqlx::{Postgres, Transaction};
use std::{collections::HashMap, ops::Range};

const REWARDS_PER_SHARE_PREC: u32 = 9;

// rewards in IoT Bones per 24 hours based on emission curve year 1
// TODO: expand to cover the full multi-year emission curve
lazy_static! {
    static ref REWARDS_PER_DAY: Decimal = (Decimal::from(60_000_000_000_u64) / Decimal::from(365)) * Decimal::from(100_000_000); // 16_438_356_164_383_560
    // TODO: year 1 emissions allocate 30% of total to PoC with 6% to beacons and 24% to witnesses but subsequent years back
    // total PoC percentage off 1.5% each year; determine how beacons and witnesses will split the subsequent years' allocations
    static ref BEACON_REWARDS_PER_DAY: Decimal = *REWARDS_PER_DAY * Decimal::new(600, 4);
    static ref WITNESS_REWARDS_PER_DAY: Decimal = *REWARDS_PER_DAY * Decimal::new(2400, 4);
}

fn get_tokens_by_duration(tokens: Decimal, duration: Duration) -> Decimal {
    (tokens / Decimal::from(Duration::hours(24).num_seconds()))
        * Decimal::from(duration.num_seconds())
}

fn get_scheduled_tokens(duration: Duration) -> (Decimal, Decimal) {
    (
        get_tokens_by_duration(*BEACON_REWARDS_PER_DAY, duration),
        get_tokens_by_duration(*WITNESS_REWARDS_PER_DAY, duration),
    )
}

#[derive(sqlx::FromRow)]
pub struct GatewayShare {
    pub hotspot_key: PublicKeyBinary,
    pub reward_type: ReportType,
    pub reward_timestamp: DateTime<Utc>,
    pub hex_scale: Decimal,
    pub reward_unit: Decimal,
    pub poc_id: Vec<u8>,
}

#[derive(sqlx::FromRow)]
struct GatewayShareSaveResult {
    inserted: bool,
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct SaveGatewayShareError(#[from] sqlx::Error);

impl GatewayShare {
    pub async fn save(
        self,
        db: &mut Transaction<'_, Postgres>,
    ) -> Result<bool, SaveGatewayShareError> {
        Ok(sqlx::query_as::<_, GatewayShareSaveResult>(
            r#"
            insert into gateway_shares (hotspot_key, reward_type, reward_timestamp, hex_scale, reward_unit, poc_id)
            values ($1, $2, $3, $4, $5, $6)
            on conflict (hotspot_key, poc_id) do update set
            reward_type = EXCLUDED.reward_type, reward_timestamp = EXCLUDED.reward_timestamp, hex_scale = EXCLUDED.hex_scale, reward_unit = EXCLUDED.reward_unit
            returning (xmax = 0) as inserted;
            "#,
        )
        .bind(self.hotspot_key)
        .bind(self.reward_type)
        .bind(self.reward_timestamp)
        .bind(self.hex_scale)
        .bind(self.reward_unit)
        .bind(self.poc_id)
        .fetch_one(&mut *db)
        .await?
        .inserted)
    }

    pub fn shares_from_poc(report: &IotValidPoc) -> impl Iterator<Item = Self> {
        let mut shares: Vec<Self> = Vec::new();
        let poc_id = report.poc_id.clone();
        let beacon_scaling_factor = report.beacon_report.hex_scale;
        let beacon_reward_unit = report.beacon_report.reward_unit;
        if beacon_scaling_factor > Decimal::ZERO && beacon_reward_unit > Decimal::ZERO {
            shares.push(Self {
                hotspot_key: report.beacon_report.report.pub_key.clone(),
                reward_type: ReportType::Beacon,
                reward_timestamp: report.beacon_report.received_timestamp,
                hex_scale: beacon_scaling_factor,
                reward_unit: beacon_reward_unit,
                poc_id: poc_id.clone(),
            })
        };
        for witness in &report.witness_reports {
            let witness_hex_scale = witness.hex_scale;
            let witness_reward_unit = witness.reward_unit;
            if witness_hex_scale > Decimal::ZERO && witness_reward_unit > Decimal::ZERO {
                shares.push(Self {
                    hotspot_key: witness.report.pub_key.clone(),
                    reward_type: ReportType::Witness,
                    reward_timestamp: witness.received_timestamp,
                    hex_scale: witness_hex_scale,
                    reward_unit: witness_reward_unit,
                    poc_id: poc_id.clone(),
                })
            }
        }
        shares.into_iter()
    }
}

#[derive(Default)]
pub struct RewardShares {
    pub beacon_shares: Decimal,
    pub witness_shares: Decimal,
}

impl RewardShares {
    pub fn add_reward(&mut self, share: &GatewayShare) {
        let rewards = share.hex_scale * share.reward_unit;
        match share.reward_type {
            ReportType::Beacon => self.beacon_shares += rewards,
            ReportType::Witness => self.witness_shares += rewards,
        }
    }
}

#[derive(Default)]
pub struct GatewayShares {
    pub shares: HashMap<PublicKeyBinary, RewardShares>,
}

impl GatewayShares {
    pub async fn aggregate(
        db: impl sqlx::PgExecutor<'_> + Copy,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Result<Self, sqlx::Error> {
        let mut shares = Self::default();

        let mut rows = sqlx::query_as::<_, GatewayShare>(
            "select * from gateway_shares where reward_timestamp > $1 and reward_timestamp <= $2",
        )
        .bind(reward_period.start)
        .bind(reward_period.end)
        .fetch(db);

        while let Some(gateway_share) = rows.try_next().await? {
            shares
                .shares
                .entry(gateway_share.hotspot_key.clone())
                .or_default()
                .add_reward(&gateway_share)
        }

        Ok(shares)
    }

    pub async fn clear_rewarded_shares(
        db: impl sqlx::PgExecutor<'_>,
        period_end: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("delete from gateway_shares where reward_timestamp <= $1")
            .bind(period_end)
            .execute(db)
            .await
            .map(|_| ())
    }

    pub fn total_shares(&self) -> (Decimal, Decimal) {
        self.shares.iter().fold(
            (Decimal::ZERO, Decimal::ZERO),
            |(beacon_sum, witness_sum), (_, reward_shares)| {
                (
                    beacon_sum + reward_shares.beacon_shares,
                    witness_sum + reward_shares.witness_shares,
                )
            },
        )
    }

    pub fn into_gateway_reward_shares(
        self,
        reward_period: &'_ Range<DateTime<Utc>>,
    ) -> impl Iterator<Item = proto::GatewayRewardShare> + '_ {
        let (total_beacon_shares, total_witness_shares) = self.total_shares();
        let (total_beacon_rewards, total_witness_rewards) =
            get_scheduled_tokens(reward_period.end - reward_period.start);
        let beacon_rewards_per_share = rewards_per_share(total_beacon_rewards, total_beacon_shares);
        let witness_rewards_per_share =
            rewards_per_share(total_witness_rewards, total_witness_shares);
        self.shares
            .into_iter()
            .map(
                move |(hotspot_key, reward_shares)| proto::GatewayRewardShare {
                    hotspot_key: hotspot_key.into(),
                    beacon_amount: compute_rewards(
                        beacon_rewards_per_share,
                        reward_shares.beacon_shares,
                    ),
                    witness_amount: compute_rewards(
                        witness_rewards_per_share,
                        reward_shares.witness_shares,
                    ),
                    start_period: reward_period.start.encode_timestamp(),
                    end_period: reward_period.end.encode_timestamp(),
                },
            )
            .filter(|reward_share| {
                reward_share.beacon_amount > 0 || reward_share.witness_amount > 0
            })
    }
}

fn rewards_per_share(total_rewards: Decimal, total_shares: Decimal) -> Decimal {
    if total_shares > Decimal::ZERO {
        (total_rewards / total_shares)
            .round_dp_with_strategy(REWARDS_PER_SHARE_PREC, RoundingStrategy::ToPositiveInfinity)
    } else {
        Decimal::ZERO
    }
}

fn compute_rewards(rewards_per_share: Decimal, shares: Decimal) -> u64 {
    (rewards_per_share * shares)
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_u64()
        .unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::*;

    fn reward_shares_in_dec(beacon_shares: i64, witness_shares: i64) -> RewardShares {
        RewardShares {
            beacon_shares: Decimal::new(beacon_shares, 4),
            witness_shares: Decimal::new(witness_shares, 4),
        }
    }

    #[test]
    fn test_reward_share_calculation() {
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        let gw3: PublicKeyBinary = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed gw3 parse");
        let gw4: PublicKeyBinary = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed gw4 parse");
        let gw5: PublicKeyBinary = "112j1iw1sV2B2Tz2DxPSeum9Cmc5kMKNdDTDg1zDRsdwuvZueq3B"
            .parse()
            .expect("failed gw5 parse");
        let gw6: PublicKeyBinary = "11fCasUk9XvU15ktsMMH64J9E7XuqQ2L5FJPv8HZMCDG6kdZ3SC"
            .parse()
            .expect("failed gw6 parse");

        let mut shares = HashMap::new();
        shares.insert(gw1.clone(), reward_shares_in_dec(10, 300)); // 0.0010, 0.0300
        shares.insert(gw2.clone(), reward_shares_in_dec(200, 550)); // 0.0200, 0.0550
        shares.insert(gw3.clone(), reward_shares_in_dec(75, 400)); // 0.0075, 0.0400
        shares.insert(gw4.clone(), reward_shares_in_dec(0, 0)); // 0.0000, 0.0000
        shares.insert(gw5.clone(), reward_shares_in_dec(20, 700)); // 0.0020, 0.0700
        shares.insert(gw6.clone(), reward_shares_in_dec(150, 350)); // 0.0150, 0.0350
        let gw_shares = GatewayShares { shares };

        let now = Utc::now();
        let reward_period = (now - Duration::minutes(10))..now;

        let rewards: HashMap<PublicKeyBinary, proto::GatewayRewardShare> = gw_shares
            .into_gateway_reward_shares(&reward_period)
            .map(|reward| {
                (
                    reward
                        .hotspot_key
                        .clone()
                        .try_into()
                        .expect("failed to decode hotspot_key"),
                    reward,
                )
            })
            .collect();

        let gw1_rewards = rewards
            .get(&gw1)
            .expect("failed to fetch gw1 rewards")
            .clone();
        let gw2_rewards = rewards
            .get(&gw2)
            .expect("failed to fetch gw2 rewards")
            .clone();
        let gw3_rewards = rewards
            .get(&gw3)
            .expect("failed to fetch gw3 rewards")
            .clone();
        let gw5_rewards = rewards
            .get(&gw5)
            .expect("failed to fetch gw4 rewards")
            .clone();
        let gw6_rewards = rewards
            .get(&gw6)
            .expect("failed to fetch gw5 rewards")
            .clone();

        assert_eq!(rewards.get(&gw4), None); // Validate zero-amount entry filtered out

        assert_eq!(gw1_rewards.beacon_amount, 150_534_397_110);
        assert_eq!(gw1_rewards.witness_amount, 3_573_555_687_909);
        assert_eq!(gw2_rewards.beacon_amount, 3_010_687_942_195);
        assert_eq!(gw2_rewards.witness_amount, 6_551_518_761_167);
        assert_eq!(gw3_rewards.beacon_amount, 1_129_007_978_323);
        assert_eq!(gw3_rewards.witness_amount, 4_764_740_917_213);
        assert_eq!(gw5_rewards.beacon_amount, 301_068_794_219);
        assert_eq!(gw5_rewards.witness_amount, 8_338_296_605_122);
        assert_eq!(gw6_rewards.beacon_amount, 2_258_015_956_646);
        assert_eq!(gw6_rewards.witness_amount, 4_169_148_302_561);
    }
}
