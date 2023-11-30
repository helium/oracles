use crate::poc_report::ReportType as PocReportType;
use chrono::{DateTime, Duration, Utc};
use file_store::{iot_packet::IotValidPacket, iot_valid_poc::IotPoc, traits::TimestampEncode};
use futures::stream::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora as proto;
use helium_proto::services::poc_lora::iot_reward_share::Reward as ProtoReward;
use lazy_static::lazy_static;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{Postgres, Transaction};
use std::{collections::HashMap, ops::Range};

const DEFAULT_PREC: u32 = 15;

// rewards in IoT Bones ( iot @ 10^6 ) per 24 hours based on emission curve year 1
// TODO: expand to cover the full multi-year emission curve
lazy_static! {
    // TODO: year 1 emissions allocate 30% of total to PoC with 6% to beacons and 24% to witnesses but subsequent years back
    // total PoC percentage off 1.5% each year; determine how beacons and witnesses will split the subsequent years' allocations
    static ref REWARDS_PER_DAY: Decimal = (Decimal::from(32_500_000_000_u64) / Decimal::from(366)) * Decimal::from(1_000_000); //  88_797_814_207_650.273224043715847
    static ref BEACON_REWARDS_PER_DAY_PERCENT: Decimal = dec!(0.06);
    static ref WITNESS_REWARDS_PER_DAY_PERCENT: Decimal = dec!(0.24);
    // Data transfer is allocated 50% of daily rewards
    static ref DATA_TRANSFER_REWARDS_PER_DAY_PERCENT: Decimal = dec!(0.50);
    // Operations fund is allocated 7% of daily rewards
    static ref OPERATIONS_REWARDS_PER_DAY_PERCENT: Decimal = dec!(0.07);
    // dc remainer distributed at ration of 4:1 in favour of witnesses
    // ie WITNESS_REWARDS_PER_DAY_PERCENT:BEACON_REWARDS_PER_DAY_PERCENT
    static ref WITNESS_DC_REMAINER_PERCENT: Decimal = dec!(0.80);
    static ref BEACON_DC_REMAINER_PERCENT: Decimal = dec!(0.20);
    static ref DC_USD_PRICE: Decimal =  dec!(0.00001);
}

fn get_tokens_by_duration(tokens: Decimal, duration: Duration) -> Decimal {
    ((tokens / Decimal::from(Duration::hours(24).num_seconds()))
        * Decimal::from(duration.num_seconds()))
    .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::MidpointNearestEven)
}

pub fn get_scheduled_poc_tokens(
    duration: Duration,
    dc_transfer_remainder: Decimal,
) -> (Decimal, Decimal) {
    (
        get_tokens_by_duration(*REWARDS_PER_DAY * *BEACON_REWARDS_PER_DAY_PERCENT, duration)
            + (dc_transfer_remainder * *BEACON_DC_REMAINER_PERCENT),
        get_tokens_by_duration(
            *REWARDS_PER_DAY * *WITNESS_REWARDS_PER_DAY_PERCENT,
            duration,
        ) + (dc_transfer_remainder * *WITNESS_DC_REMAINER_PERCENT),
    )
}

pub fn get_scheduled_dc_tokens(duration: Duration) -> Decimal {
    get_tokens_by_duration(
        *REWARDS_PER_DAY * *DATA_TRANSFER_REWARDS_PER_DAY_PERCENT,
        duration,
    )
}

pub fn get_scheduled_ops_fund_tokens(duration: Duration) -> Decimal {
    get_tokens_by_duration(
        *REWARDS_PER_DAY * *OPERATIONS_REWARDS_PER_DAY_PERCENT,
        duration,
    )
}

#[derive(sqlx::FromRow)]
pub struct GatewayPocShare {
    pub hotspot_key: PublicKeyBinary,
    pub reward_type: PocReportType,
    pub reward_timestamp: DateTime<Utc>,
    pub hex_scale: Decimal,
    pub reward_unit: Decimal,
    pub poc_id: Vec<u8>,
}

#[derive(sqlx::FromRow)]
pub struct GatewayDCShare {
    pub hotspot_key: PublicKeyBinary,
    pub reward_timestamp: DateTime<Utc>,
    pub num_dcs: Decimal,
    pub id: Vec<u8>,
}

#[derive(sqlx::FromRow)]
struct GatewayShareSaveResult {
    inserted: bool,
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct SaveGatewayShareError(#[from] sqlx::Error);

impl GatewayPocShare {
    pub async fn save(
        self,
        db: &mut Transaction<'_, Postgres>,
    ) -> Result<bool, SaveGatewayShareError> {
        Ok(sqlx::query_as::<_, GatewayShareSaveResult>(
            r#"
            insert into gateway_shares (hotspot_key, reward_type, reward_timestamp, hex_scale, reward_unit, poc_id)
            values ($1, $2, $3, $4, $5, $6)
            on conflict (hotspot_key, poc_id) do update set
                reward_type = EXCLUDED.reward_type,
                reward_timestamp = EXCLUDED.reward_timestamp,
                hex_scale = EXCLUDED.hex_scale,
                reward_unit = EXCLUDED.reward_unit
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

    pub fn shares_from_poc(report: &IotPoc) -> impl Iterator<Item = Self> {
        let mut shares: Vec<Self> = Vec::new();
        let beacon_scaling_factor = report.beacon_report.hex_scale;
        let beacon_reward_unit = report.beacon_report.reward_unit;
        if beacon_scaling_factor > Decimal::ZERO && beacon_reward_unit > Decimal::ZERO {
            shares.push(Self {
                hotspot_key: report.beacon_report.report.pub_key.clone(),
                reward_type: PocReportType::Beacon,
                reward_timestamp: report.beacon_report.received_timestamp,
                hex_scale: beacon_scaling_factor,
                reward_unit: beacon_reward_unit,
                poc_id: report.poc_id.clone(),
            })
        };
        for witness in &report.selected_witnesses {
            let witness_hex_scale = witness.hex_scale;
            let witness_reward_unit = witness.reward_unit;
            if witness.status == proto::VerificationStatus::Valid
                && witness_hex_scale > Decimal::ZERO
                && witness_reward_unit > Decimal::ZERO
            {
                shares.push(Self {
                    hotspot_key: witness.report.pub_key.clone(),
                    reward_type: PocReportType::Witness,
                    reward_timestamp: witness.received_timestamp,
                    hex_scale: witness_hex_scale,
                    reward_unit: witness_reward_unit,
                    poc_id: report.poc_id.clone(),
                })
            }
        }
        shares.into_iter()
    }
}

impl GatewayDCShare {
    pub async fn save(
        self,
        db: &mut Transaction<'_, Postgres>,
    ) -> Result<bool, SaveGatewayShareError> {
        Ok(sqlx::query_as::<_, GatewayShareSaveResult>(
            r#"
            insert into gateway_dc_shares (hotspot_key, reward_timestamp, num_dcs, id)
            values ($1, $2, $3, $4)
            on conflict (id) do update set
                reward_timestamp = EXCLUDED.reward_timestamp,
                num_dcs = EXCLUDED.num_dcs
            returning (xmax = 0) as inserted;
            "#,
        )
        .bind(self.hotspot_key)
        .bind(self.reward_timestamp)
        .bind(self.num_dcs)
        .bind(self.id)
        .fetch_one(&mut *db)
        .await?
        .inserted)
    }

    pub fn share_from_packet(packet: &IotValidPacket) -> Self {
        Self {
            hotspot_key: packet.gateway.clone(),
            reward_timestamp: packet.packet_timestamp,
            num_dcs: Decimal::new(packet.num_dcs as i64, 0),
            id: packet.packet_id(),
        }
    }
}

#[derive(Default)]
pub struct RewardShares {
    pub beacon_shares: Decimal,
    pub witness_shares: Decimal,
    pub dc_shares: Decimal,
}

impl RewardShares {
    pub fn add_poc_reward(&mut self, share: &GatewayPocShare) {
        let rewards = share.hex_scale * share.reward_unit;
        match share.reward_type {
            PocReportType::Beacon => self.beacon_shares += rewards,
            PocReportType::Witness => self.witness_shares += rewards,
        }
    }
    pub fn add_dc_reward(&mut self, share: &GatewayDCShare) {
        self.dc_shares += share.num_dcs
    }
}

#[derive(Default)]
pub struct GatewayShares {
    pub shares: HashMap<PublicKeyBinary, RewardShares>,
    pub beacon_rewards_per_share: Decimal,
    pub witness_rewards_per_share: Decimal,
    pub dc_transfer_rewards_per_share: Decimal,
    pub total_rewards_for_poc_and_dc: Decimal,
}

impl GatewayShares {
    pub async fn aggregate(
        db: impl sqlx::PgExecutor<'_> + Copy,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Result<Self, sqlx::Error> {
        let mut shares = Self::default();
        // get all the shares, poc and dc
        shares.aggregate_poc_shares(db, reward_period).await?;
        shares.aggregate_dc_shares(db, reward_period).await?;
        Ok(shares)
    }

    pub async fn clear_rewarded_shares(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        period_end: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("delete from gateway_shares where reward_timestamp <= $1")
            .bind(period_end)
            .execute(&mut *tx)
            .await
            .map(|_| ())?;

        sqlx::query("delete from gateway_dc_shares where reward_timestamp <= $1")
            .bind(period_end)
            .execute(&mut *tx)
            .await
            .map(|_| ())
    }

    pub fn total_shares(&self) -> (Decimal, Decimal, Decimal) {
        self.shares.iter().fold(
            (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            |(beacon_sum, witness_sum, dc_sum), (_, reward_shares)| {
                (
                    beacon_sum + reward_shares.beacon_shares,
                    witness_sum + reward_shares.witness_shares,
                    dc_sum + reward_shares.dc_shares,
                )
            },
        )
    }

    async fn aggregate_poc_shares(
        &mut self,
        db: impl sqlx::PgExecutor<'_> + Copy,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let mut rows = sqlx::query_as::<_, GatewayPocShare>(
            "select * from gateway_shares where reward_timestamp > $1 and reward_timestamp <= $2",
        )
        .bind(reward_period.start)
        .bind(reward_period.end)
        .fetch(db);
        while let Some(gateway_share) = rows.try_next().await? {
            self.shares
                .entry(gateway_share.hotspot_key.clone())
                .or_default()
                .add_poc_reward(&gateway_share)
        }
        Ok(())
    }

    async fn aggregate_dc_shares(
        &mut self,
        db: impl sqlx::PgExecutor<'_> + Copy,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Result<(), sqlx::Error> {
        let mut rows = sqlx::query_as::<_, GatewayDCShare>(
            "select hotspot_key, reward_timestamp, num_dcs::numeric, id from gateway_dc_shares where reward_timestamp > $1 and reward_timestamp <= $2",
        )
        .bind(reward_period.start)
        .bind(reward_period.end)
        .fetch(db);
        while let Some(gateway_share) = rows.try_next().await? {
            self.shares
                .entry(gateway_share.hotspot_key.clone())
                .or_default()
                .add_dc_reward(&gateway_share)
        }
        Ok(())
    }

    pub fn calculate_rewards_per_share_and_total_usage(
        &mut self,
        reward_period: &'_ Range<DateTime<Utc>>,
        iot_price: Decimal,
    ) {
        // the total number of shares for beacons, witnesses and data transfer
        // dc shares here is the sum of all spent data transfer DC this epoch
        let (total_beacon_shares, total_witness_shares, total_dc_shares) = self.total_shares();

        // the total number of iot rewards for dc transfer this epoch
        let total_dc_transfer_rewards =
            get_scheduled_dc_tokens(reward_period.end - reward_period.start);

        // convert the total spent data transfer DC to it equiv iot bone value
        // the rewards distributed to gateways will be equal to this
        // up to a max cap of total_dc_transfer_rewards
        // if the dc transfer rewards is less than total_dc_transfer_rewards
        // then the remainer will be added to the POC rewards allocation
        let total_dc_transfer_rewards_used = dc_to_iot_bones(total_dc_shares, iot_price);
        let (dc_transfer_rewards_unused, total_dc_transfer_rewards_capped) =
            normalize_dc_transfer_rewards(
                total_dc_transfer_rewards_used,
                total_dc_transfer_rewards,
            );
        // the total amounts of iot rewards this epoch for beacons, witnesses
        // taking into account any remaining dc transfer rewards
        let (total_beacon_rewards, total_witness_rewards) = get_scheduled_poc_tokens(
            reward_period.end - reward_period.start,
            dc_transfer_rewards_unused,
        );
        // work out the rewards per share for beacons, witnesses and dc transfer
        let beacon_rewards_per_share = rewards_per_share(total_beacon_rewards, total_beacon_shares);
        let witness_rewards_per_share =
            rewards_per_share(total_witness_rewards, total_witness_shares);
        let dc_transfer_rewards_per_share =
            rewards_per_share(total_dc_transfer_rewards_capped, total_dc_shares);

        tracing::info!(
            %total_dc_shares,
            %total_dc_transfer_rewards_used,
            %dc_transfer_rewards_unused,
            %dc_transfer_rewards_per_share,
            "data transfer rewards"
        );
        self.beacon_rewards_per_share = beacon_rewards_per_share;
        self.witness_rewards_per_share = witness_rewards_per_share;
        self.dc_transfer_rewards_per_share = dc_transfer_rewards_per_share;
        self.total_rewards_for_poc_and_dc =
            total_beacon_rewards + total_witness_rewards + total_dc_transfer_rewards_capped;
    }

    pub fn into_iot_reward_shares(
        self,
        reward_period: &'_ Range<DateTime<Utc>>,
    ) -> impl Iterator<Item = (u64, proto::IotRewardShare)> + '_ {
        self.shares
            .into_iter()
            .map(move |(hotspot_key, reward_shares)| {
                let beacon_amount =
                    compute_rewards(self.beacon_rewards_per_share, reward_shares.beacon_shares);
                let witness_amount =
                    compute_rewards(self.witness_rewards_per_share, reward_shares.witness_shares);
                let dc_transfer_amount =
                    compute_rewards(self.dc_transfer_rewards_per_share, reward_shares.dc_shares);
                proto::GatewayReward {
                    hotspot_key: hotspot_key.into(),
                    beacon_amount,
                    witness_amount,
                    dc_transfer_amount,
                }
            })
            .filter(|reward_share| {
                reward_share.beacon_amount > 0
                    || reward_share.witness_amount > 0
                    || reward_share.dc_transfer_amount > 0
            })
            .map(|gateway_reward| {
                let total_gateway_reward = gateway_reward.dc_transfer_amount
                    + gateway_reward.beacon_amount
                    + gateway_reward.witness_amount;
                (
                    total_gateway_reward,
                    proto::IotRewardShare {
                        start_period: reward_period.start.encode_timestamp(),
                        end_period: reward_period.end.encode_timestamp(),
                        reward: Some(ProtoReward::GatewayReward(gateway_reward)),
                    },
                )
            })
    }
}

/// returns the equiv iot bones value for a specified dc amount
pub fn dc_to_iot_bones(dc_amount: Decimal, iot_price: Decimal) -> Decimal {
    // iot prices are supplied in 10^6 *per iot token*
    // we need the price at this point per iot bones
    let iot_price = iot_price_to_bones(iot_price);
    // use the price per bones to get the num of bones for our
    // dc USD value
    let dc_in_usd = dc_amount * (*DC_USD_PRICE);
    (dc_in_usd / iot_price)
        .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::ToPositiveInfinity)
}

/// returns the equiv dc value for a specified iot bones amount
pub fn iot_bones_to_dc(iot_amount: Decimal, iot_price: Decimal) -> Decimal {
    // iot prices are supplied in 10^6 *per iot token*
    // we need the price at this point per iot bones
    let iot_price = iot_price_to_bones(iot_price);
    // use the price per bones to get the value of our bones in DC
    let iot_value = iot_amount * iot_price;
    (iot_value / (*DC_USD_PRICE)).round_dp_with_strategy(0, RoundingStrategy::ToNegativeInfinity)
}

pub fn iot_price_to_bones(iot_price: Decimal) -> Decimal {
    iot_price
        / dec!(1_000_000) // Per Iot token
        / dec!(1_000_000) // Per Bone
}

pub fn normalize_dc_transfer_rewards(
    total_dc_transfer_rewards_used: Decimal,
    total_dc_transfer_rewards: Decimal,
) -> (Decimal, Decimal) {
    match total_dc_transfer_rewards_used <= total_dc_transfer_rewards {
        true => (
            total_dc_transfer_rewards - total_dc_transfer_rewards_used,
            total_dc_transfer_rewards_used,
        ),
        false => (Decimal::ZERO, total_dc_transfer_rewards),
    }
}

fn rewards_per_share(total_rewards: Decimal, total_shares: Decimal) -> Decimal {
    if total_shares > Decimal::ZERO {
        (total_rewards / total_shares)
            .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::MidpointNearestEven)
    } else {
        Decimal::ZERO
    }
}

fn compute_rewards(rewards_per_share: Decimal, shares: Decimal) -> u64 {
    (rewards_per_share * shares)
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::*;

    fn reward_shares_in_dec(
        beacon_shares: Decimal,
        witness_shares: Decimal,
        dc_shares: Decimal,
    ) -> RewardShares {
        RewardShares {
            beacon_shares,
            witness_shares,
            // the test allocate shares to gateways via percentages, and so we end up with fractional values
            // in order to ensure precision we keep those
            // in the real world dc shares will be whole numbers, as we never spend fractional DC
            dc_shares: dc_shares
                .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::MidpointNearestEven),
        }
    }

    #[test]
    fn test_non_gateway_reward_shares() {
        let epoch_duration = Duration::hours(1);
        let total_tokens_for_period = *REWARDS_PER_DAY / dec!(24);
        println!("total_tokens_for_period: {total_tokens_for_period}");

        let operation_tokens_for_period = get_scheduled_ops_fund_tokens(epoch_duration);
        assert_eq!(
            dec!(258_993_624_772.313296903460838),
            operation_tokens_for_period
        );
    }

    #[test]
    // test reward distribution where there is a fixed dc spend per gateway
    // with the total dc spend across all gateways being significantly lower than the
    // total epoch dc rewards amount
    // this results in a significant redistribution of dc rewards to POC
    fn test_reward_share_calculation_fixed_dc_spend_with_transfer_distribution() {
        let iot_price = dec!(359);
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

        let now = Utc::now();
        let reward_period = (now - Duration::minutes(10))..now;
        let total_data_transfer_tokens_for_period = get_scheduled_dc_tokens(Duration::minutes(10));
        println!("total data transfer scheduled tokens: {total_data_transfer_tokens_for_period}");

        let gw1_dc_spend = dec!(502);
        let gw2_dc_spend = dec!(5000);
        let gw3_dc_spend = dec!(5000);
        // gw4 gets zero dc transfer shares and zero poc shares
        // will get no rewards
        let gw4_dc_spend = dec!(0);
        // gw5 gets zero dc transfer shares but does get poc shares below
        // will get poc rewards only
        let gw5_dc_spend = dec!(0);
        let gw6_dc_spend = dec!(50000);
        let total_dc_spend =
            gw1_dc_spend + gw2_dc_spend + gw3_dc_spend + gw4_dc_spend + gw5_dc_spend + gw6_dc_spend;
        println!("total dc spend: {total_dc_spend}");
        let total_used_data_transfer_tokens = dc_to_iot_bones(total_dc_spend, iot_price);
        println!("total data transfer rewards for dc spent: {total_used_data_transfer_tokens}");
        let total_unused_data_transfer_tokens =
            total_data_transfer_tokens_for_period - total_used_data_transfer_tokens;

        // generate the rewards map
        let mut shares = HashMap::new();
        shares.insert(
            gw1.clone(),
            reward_shares_in_dec(dec!(10), dec!(300), gw1_dc_spend),
        ); // 0.0010, 0.0300
        shares.insert(
            gw2.clone(),
            reward_shares_in_dec(dec!(200), dec!(550), gw2_dc_spend),
        ); // 0.0200, 0.0550
        shares.insert(
            gw3.clone(),
            reward_shares_in_dec(dec!(75), dec!(400), gw3_dc_spend),
        ); // 0.0075, 0.0400
        shares.insert(
            gw4.clone(),
            reward_shares_in_dec(dec!(0), dec!(0), gw4_dc_spend),
        ); // 0.0000, 0.0000
        shares.insert(
            gw5.clone(),
            reward_shares_in_dec(dec!(20), dec!(700), gw5_dc_spend),
        ); // 0.0020, 0.0700
        shares.insert(
            gw6.clone(),
            reward_shares_in_dec(dec!(150), dec!(350), gw6_dc_spend),
        ); // 0.0150, 0.0350

        let mut gw_shares = GatewayShares {
            shares,
            ..Default::default()
        };
        let mut rewards: HashMap<PublicKeyBinary, proto::GatewayReward> = HashMap::new();
        gw_shares.calculate_rewards_per_share_and_total_usage(&reward_period, iot_price);
        let total_rewards_for_poc_and_dc = gw_shares.total_rewards_for_poc_and_dc;
        let mut allocated_gateway_rewards = 0_u64;
        for (reward_amount, reward) in gw_shares.into_iot_reward_shares(&reward_period) {
            if let Some(ProtoReward::GatewayReward(gateway_reward)) = reward.reward {
                let gateway_reward_total = gateway_reward.beacon_amount
                    + gateway_reward.witness_amount
                    + gateway_reward.dc_transfer_amount;
                rewards.insert(
                    gateway_reward.hotspot_key.clone().into(),
                    gateway_reward,
                );
                assert_eq!(reward_amount, gateway_reward_total);
                allocated_gateway_rewards += reward_amount;
            }
        }

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

        // confirm we did not distribute more than the daily max dc rewards allocation
        let sum_dc_amounts = gw1_rewards.dc_transfer_amount
            + gw2_rewards.dc_transfer_amount
            + gw3_rewards.dc_transfer_amount
            + gw5_rewards.dc_transfer_amount
            + gw6_rewards.dc_transfer_amount;
        println!("max data transfer rewards for spent dc: {total_used_data_transfer_tokens}");
        println!("total actual data transfer rewards distributed: {sum_dc_amounts}");
        let data_transfer_diff =
            total_used_data_transfer_tokens.to_i64().unwrap() - sum_dc_amounts as i64;
        // the sum of rewards distributed should not exceed total allocation
        // but due to rounding whilst going to u64 in compute_rewards,
        // is permitted to be a few bones less
        assert_eq!(data_transfer_diff, 1);

        // assert the expected data transfer rewards amounts per gateway
        // using the dc_to_iot_bones helper function
        let gw1_expected_dc_rewards = dc_to_iot_bones(gw1_dc_spend, iot_price).to_u64().unwrap();
        assert_eq!(gw1_expected_dc_rewards.to_u64().unwrap(), 13_983_286);
        let gw2_expected_dc_rewards = dc_to_iot_bones(gw2_dc_spend, iot_price).to_u64().unwrap();
        assert_eq!(gw2_expected_dc_rewards.to_u64().unwrap(), 139_275_766);
        let gw3_expected_dc_rewards = dc_to_iot_bones(gw3_dc_spend, iot_price).to_u64().unwrap();
        assert_eq!(gw3_expected_dc_rewards.to_u64().unwrap(), 139_275_766);
        let gw5_expected_dc_rewards = dc_to_iot_bones(gw5_dc_spend, iot_price).to_u64().unwrap();
        assert_eq!(gw5_expected_dc_rewards.to_u64().unwrap(), 0);
        let gw6_expected_dc_rewards = dc_to_iot_bones(gw6_dc_spend, iot_price).to_u64().unwrap();
        assert_eq!(gw6_expected_dc_rewards.to_u64().unwrap(), 1_392_757_660);
        assert_eq!(gw1_rewards.dc_transfer_amount, gw1_expected_dc_rewards);
        assert_eq!(gw2_rewards.dc_transfer_amount, gw2_expected_dc_rewards);
        assert_eq!(gw3_rewards.dc_transfer_amount, gw3_expected_dc_rewards);
        assert_eq!(gw5_rewards.dc_transfer_amount, gw5_expected_dc_rewards);
        assert_eq!(gw6_rewards.dc_transfer_amount, gw6_expected_dc_rewards);

        // assert the beacon and witness amount, these will now have an allocation
        // of any unused data transfer rewards
        assert_eq!(rewards.get(&gw4), None); // Validate zero-amount entry filtered out
        assert_eq!(gw1_rewards.beacon_amount, 2_161_036_912);
        assert_eq!(gw1_rewards.witness_amount, 51_301_137_137);
        assert_eq!(gw2_rewards.beacon_amount, 43_220_738_247);
        assert_eq!(gw2_rewards.witness_amount, 94_052_084_751);
        assert_eq!(gw3_rewards.beacon_amount, 16_207_776_842);
        assert_eq!(gw3_rewards.witness_amount, 68_401_516_182);
        assert_eq!(gw5_rewards.beacon_amount, 4_322_073_824);
        assert_eq!(gw5_rewards.witness_amount, 119_702_653_319);
        assert_eq!(gw6_rewards.beacon_amount, 32_415_553_685);
        assert_eq!(gw6_rewards.witness_amount, 59_851_326_659);

        // assert the total POC rewards allocated equals TOTAL_POC_REWARDS_FOR_PERIOD
        // plus the remainder of the total dc transfer rewards for the period
        let sum_poc_amounts = gw1_rewards.beacon_amount
            + gw1_rewards.witness_amount
            + gw2_rewards.beacon_amount
            + gw2_rewards.witness_amount
            + gw3_rewards.beacon_amount
            + gw3_rewards.witness_amount
            + gw5_rewards.beacon_amount
            + gw5_rewards.witness_amount
            + gw6_rewards.beacon_amount
            + gw6_rewards.witness_amount;

        let (exp_total_beacon_tokens, exp_total_witness_tokens) =
            get_scheduled_poc_tokens(Duration::minutes(10), total_unused_data_transfer_tokens);
        let exp_sum_poc_tokens = exp_total_beacon_tokens + exp_total_witness_tokens;
        println!("max poc rewards: {exp_sum_poc_tokens}");
        println!("total actual poc rewards distributed: {sum_poc_amounts}");

        // confirm the unallocated poc reward/dc amounts
        // we can loose up to 1 bone per gateway for each of beacon_amount, witness_amount and dc_amount
        // due to going from decimal to u64
        let unallocated_poc_reward_amount =
            total_rewards_for_poc_and_dc - Decimal::from(allocated_gateway_rewards);
        assert_eq!(unallocated_poc_reward_amount.to_u64().unwrap(), 6);
    }

    #[test]
    // test reward distribution where there is zero transfer of dc rewards to poc
    fn test_reward_share_calculation_without_data_transfer_distribution() {
        let iot_price = dec!(359);
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

        let now = Utc::now();
        let reward_period = (now - Duration::minutes(10))..now;
        let total_data_transfer_tokens_for_period = get_scheduled_dc_tokens(Duration::minutes(10));
        println!("total data transfer scheduled tokens: {total_data_transfer_tokens_for_period}");

        // get the expected total amount of dc we need to spend
        // in order to use up all the dc rewards for this period
        // distribute this amount of dc across the gateways
        // this results in zero unallocated dc rewards being
        // available to distributed to POC
        let total_dc_to_spend = iot_bones_to_dc(total_data_transfer_tokens_for_period, iot_price);
        println!("total dc value of scheduled data transfer tokens: {total_dc_to_spend}");

        // generate the rewards map
        // distribute *ALL* the dc shares across gateways
        // gateways 1,2,3 & 5 get 10% each of the total shares
        // gateway 6 gets 80% of the total shares
        // this means we spent DC with a value > than the max DC rewards value
        // in this scenario the DC rewards will be capped at daily max
        // each gateway should get a share of this cap proportational to
        // their relative spend value
        let gw1_dc_spend = total_dc_to_spend * dec!(0.1);
        let gw2_dc_spend = total_dc_to_spend * dec!(0.1);
        let gw3_dc_spend = total_dc_to_spend * dec!(0.1);
        let gw4_dc_spend = dec!(0);
        let gw5_dc_spend = total_dc_to_spend * dec!(0.1);
        let gw6_dc_spend = total_dc_to_spend * dec!(0.8);

        let mut shares = HashMap::new();
        shares.insert(
            gw1.clone(),
            reward_shares_in_dec(dec!(10), dec!(300), gw1_dc_spend),
        ); // 0.0010, 0.0300
        shares.insert(
            gw2.clone(),
            reward_shares_in_dec(dec!(200), dec!(550), gw2_dc_spend),
        ); // 0.0200, 0.0550
        shares.insert(
            gw3.clone(),
            reward_shares_in_dec(dec!(75), dec!(400), gw3_dc_spend),
        ); // 0.0075, 0.0400
        shares.insert(
            gw4.clone(),
            reward_shares_in_dec(dec!(0), dec!(0), gw4_dc_spend),
        ); // 0.0000, 0.0000, 0.7000
        shares.insert(
            gw5.clone(),
            reward_shares_in_dec(dec!(20), dec!(700), gw5_dc_spend),
        ); // 0.0020, 0.0700
        shares.insert(
            gw6.clone(),
            reward_shares_in_dec(dec!(150), dec!(350), gw6_dc_spend),
        ); // 0.0150, 0.0350

        let mut gw_shares = GatewayShares {
            shares,
            ..Default::default()
        };
        let mut rewards: HashMap<PublicKeyBinary, proto::GatewayReward> = HashMap::new();
        gw_shares.calculate_rewards_per_share_and_total_usage(&reward_period, iot_price);
        let total_rewards_for_poc_and_dc = gw_shares.total_rewards_for_poc_and_dc;
        let mut allocated_gateway_rewards = 0_u64;
        for (reward_amount, reward) in gw_shares.into_iot_reward_shares(&reward_period) {
            if let Some(ProtoReward::GatewayReward(gateway_reward)) = reward.reward {
                let gateway_reward_total = gateway_reward.beacon_amount
                    + gateway_reward.witness_amount
                    + gateway_reward.dc_transfer_amount;
                rewards.insert(
                    gateway_reward.hotspot_key.clone().into(),
                    gateway_reward,
                );
                assert_eq!(reward_amount, gateway_reward_total);
                allocated_gateway_rewards += reward_amount;
            }
        }

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

        // confirm we did not distribute more than the daily max dc rewards allocation
        // even tho the value of the dc shares distributed across the gateways
        // amounted to > 100% max rewards value
        // confirm the max allocation was capped at the reward period value
        let sum_data_transfer_amounts: u64 = gw1_rewards.dc_transfer_amount
            + gw2_rewards.dc_transfer_amount
            + gw3_rewards.dc_transfer_amount
            + gw5_rewards.dc_transfer_amount
            + gw6_rewards.dc_transfer_amount;
        println!("max data transfer rewards: {total_data_transfer_tokens_for_period}");
        println!("total actual data transfer rewards distributed: {sum_data_transfer_amounts}");
        let data_transfer_diff = total_data_transfer_tokens_for_period.to_i64().unwrap()
            - sum_data_transfer_amounts as i64;
        // the sum of rewards distributed should not exceed the epoch amount
        // but due to rounding whilst going to u64 in compute_rewards,
        // is permitted to be a few bones less
        assert_eq!(data_transfer_diff, 1);

        // assert the expected data transfer rewards amounts per gateway
        assert_eq!(gw1_rewards.dc_transfer_amount, 25_693_811_981); // ~8.33% of total rewards
        assert_eq!(gw2_rewards.dc_transfer_amount, 25_693_811_981); // ~8.33% of total rewards
        assert_eq!(gw3_rewards.dc_transfer_amount, 25_693_811_981); // ~8.33% of total rewards
        assert_eq!(gw5_rewards.dc_transfer_amount, 25_693_811_981); // ~8.33% of total rewards
        assert_eq!(gw6_rewards.dc_transfer_amount, 205_550_495_851); // ~66.64% of total rewards, or 8x each of the other gateways

        // assert the beacon and witness amount
        // these will be rewards solely from POC as there are zero unallocated
        // dc transfer rewards
        assert_eq!(rewards.get(&gw4), None); // Validate zero-amount entry filtered out
        assert_eq!(gw1_rewards.beacon_amount, 813_166_796);
        assert_eq!(gw1_rewards.witness_amount, 19_303_872_653);
        assert_eq!(gw2_rewards.beacon_amount, 16_263_335_935);
        assert_eq!(gw2_rewards.witness_amount, 35_390_433_198);
        assert_eq!(gw3_rewards.beacon_amount, 6_098_750_975);
        assert_eq!(gw3_rewards.witness_amount, 25_738_496_871);
        assert_eq!(gw5_rewards.beacon_amount, 1_626_333_593);
        assert_eq!(gw5_rewards.witness_amount, 45_042_369_525);
        assert_eq!(gw6_rewards.beacon_amount, 12_197_501_951);
        assert_eq!(gw6_rewards.witness_amount, 22_521_184_762);

        // assert the total rewards allocated equals TOTAL_POC_REWARDS_FOR_PERIOD
        // plus 0% of the total dc transfer rewards for the period
        let sum_poc_amounts = gw1_rewards.beacon_amount
            + gw1_rewards.witness_amount
            + gw2_rewards.beacon_amount
            + gw2_rewards.witness_amount
            + gw3_rewards.beacon_amount
            + gw3_rewards.witness_amount
            + gw5_rewards.beacon_amount
            + gw5_rewards.witness_amount
            + gw6_rewards.beacon_amount
            + gw6_rewards.witness_amount;
        let (exp_total_beacon_tokens, exp_total_witness_tokens) =
            get_scheduled_poc_tokens(Duration::minutes(10), Decimal::ZERO);
        let exp_sum_poc_tokens = exp_total_beacon_tokens + exp_total_witness_tokens;
        println!("max poc rewards: {exp_sum_poc_tokens}");
        println!("total actual poc rewards distributed: {sum_poc_amounts}");

        // confirm the unallocated poc reward/dc amounts
        // we can loose up to 1 bone per gateway for each of beacon_amount, witness_amount and dc_amount
        // due to going from decimal to u64
        let unallocated_poc_reward_amount =
            total_rewards_for_poc_and_dc - Decimal::from(allocated_gateway_rewards);
        assert_eq!(unallocated_poc_reward_amount.to_u64().unwrap(), 8);
    }

    #[test]
    // test reward distribution where there is transfer of dc rewards to poc
    fn test_reward_share_calculation_with_data_transfer_distribution() {
        let iot_price = dec!(359);
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

        let now = Utc::now();
        let reward_period = (now - Duration::minutes(10))..now;
        let total_data_transfer_tokens_for_period = get_scheduled_dc_tokens(Duration::minutes(10));
        println!("total_data_transfer_tokens_for_period: {total_data_transfer_tokens_for_period}");

        // get the expected total amount of dc we need to spend
        // spread *some* of this across the gateways and then confirm
        // the unallocated rewards go to poc
        let total_dc_to_spend = iot_bones_to_dc(total_data_transfer_tokens_for_period, iot_price);
        println!("total_dc_to_spend: {total_dc_to_spend}");

        // generate the rewards map
        // distribute 55% of the dc shares across gateways
        let gw1_dc_spend = total_dc_to_spend * dec!(0.1);
        let gw2_dc_spend = dec!(0);
        let gw3_dc_spend = total_dc_to_spend * dec!(0.2);
        let gw4_dc_spend = dec!(0);
        let gw5_dc_spend = total_dc_to_spend * dec!(0.05);
        let gw6_dc_spend = total_dc_to_spend * dec!(0.2);

        let mut shares = HashMap::new();
        shares.insert(
            gw1.clone(),
            reward_shares_in_dec(dec!(10), dec!(300), gw1_dc_spend),
        ); // 0.0010, 0.0300
        shares.insert(
            gw2.clone(),
            reward_shares_in_dec(dec!(200), dec!(550), gw2_dc_spend),
        ); // 0.0200, 0.0550
        shares.insert(
            gw3.clone(),
            reward_shares_in_dec(dec!(75), dec!(400), gw3_dc_spend),
        ); // 0.0075, 0.0400
        shares.insert(
            gw4.clone(),
            reward_shares_in_dec(dec!(0), dec!(0), gw4_dc_spend),
        ); // 0.0000, 0.0000, 0.7000
        shares.insert(
            gw5.clone(),
            reward_shares_in_dec(dec!(20), dec!(700), gw5_dc_spend),
        ); // 0.0020, 0.0700
        shares.insert(
            gw6.clone(),
            reward_shares_in_dec(dec!(150), dec!(350), gw6_dc_spend),
        ); // 0.0150, 0.0350

        let mut gw_shares = GatewayShares {
            shares,
            ..Default::default()
        };
        let mut rewards: HashMap<PublicKeyBinary, proto::GatewayReward> = HashMap::new();
        gw_shares.calculate_rewards_per_share_and_total_usage(&reward_period, iot_price);
        let total_rewards_for_poc_and_dc = gw_shares.total_rewards_for_poc_and_dc;
        let mut allocated_gateway_rewards = 0_u64;
        for (reward_amount, reward) in gw_shares.into_iot_reward_shares(&reward_period) {
            if let Some(ProtoReward::GatewayReward(gateway_reward)) = reward.reward {
                let gateway_reward_total = gateway_reward.beacon_amount
                    + gateway_reward.witness_amount
                    + gateway_reward.dc_transfer_amount;
                rewards.insert(
                    gateway_reward.hotspot_key.clone().into(),
                    gateway_reward,
                );
                assert_eq!(reward_amount, gateway_reward_total);
                allocated_gateway_rewards += reward_amount;
            }
        }

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

        // assert the sum of data transfer amounts matches what we expect
        // which is 55% of the total available data transfer rewards
        let sum_data_transfer_amounts = gw1_rewards.dc_transfer_amount
            + gw2_rewards.dc_transfer_amount
            + gw3_rewards.dc_transfer_amount
            + gw5_rewards.dc_transfer_amount
            + gw6_rewards.dc_transfer_amount;
        println!("max data transfer rewards: {total_data_transfer_tokens_for_period}");
        println!("total actual data transfer rewards distributed: {sum_data_transfer_amounts}");
        let expected_data_transfer_percent = Decimal::from_u64(sum_data_transfer_amounts).unwrap()
            * dec!(100)
            / total_data_transfer_tokens_for_period;
        assert_eq!(expected_data_transfer_percent.round(), dec!(55));

        // assert the expected dc amounts per gateway
        assert_eq!(gw1_rewards.dc_transfer_amount, 30_832_573_816); // 10% of total
        assert_eq!(gw2_rewards.dc_transfer_amount, 0); // 0% of total
        assert_eq!(gw3_rewards.dc_transfer_amount, 61_665_147_632); // 20% of total
        assert_eq!(gw5_rewards.dc_transfer_amount, 15_416_286_908); // 5% of total
        assert_eq!(gw6_rewards.dc_transfer_amount, 61_665_147_632); // 20% of total

        // assert the beacon and witness amount, these will now have an allocation
        // of any unused data transfer rewards
        assert_eq!(rewards.get(&gw4), None); // Validate zero-amount entry filtered out
        assert_eq!(gw1_rewards.beacon_amount, 1_423_041_907);
        assert_eq!(gw1_rewards.witness_amount, 33_781_777_466);
        assert_eq!(gw2_rewards.beacon_amount, 28_460_838_158);
        assert_eq!(gw2_rewards.witness_amount, 61_933_258_688);
        assert_eq!(gw3_rewards.beacon_amount, 10_672_814_309);
        assert_eq!(gw3_rewards.witness_amount, 45_042_369_955);
        assert_eq!(gw5_rewards.beacon_amount, 2_846_083_815);
        assert_eq!(gw5_rewards.witness_amount, 78_824_147_421);
        assert_eq!(gw6_rewards.beacon_amount, 21_345_628_618);
        assert_eq!(gw6_rewards.witness_amount, 39_412_073_710);

        // assert the total POC rewards allocated equal TOTAL_POC_REWARDS_FOR_PERIOD
        // plus 45% of the total dc transfer rewards for the period
        let sum_poc_amounts = gw1_rewards.beacon_amount
            + gw1_rewards.witness_amount
            + gw2_rewards.beacon_amount
            + gw2_rewards.witness_amount
            + gw3_rewards.beacon_amount
            + gw3_rewards.witness_amount
            + gw5_rewards.beacon_amount
            + gw5_rewards.witness_amount
            + gw6_rewards.beacon_amount
            + gw6_rewards.witness_amount;
        let expected_data_transfer_tokens_for_poc = total_data_transfer_tokens_for_period
            - Decimal::from_u64(sum_data_transfer_amounts).unwrap();
        println!("expected_data_transfer_tokens_for_poc: {expected_data_transfer_tokens_for_poc}");
        let (exp_total_beacon_tokens, exp_total_witness_tokens) =
            get_scheduled_poc_tokens(Duration::minutes(10), expected_data_transfer_tokens_for_poc);
        let exp_sum_poc_tokens = exp_total_beacon_tokens + exp_total_witness_tokens;
        println!("max poc rewards: {exp_sum_poc_tokens}");
        println!("total actual poc rewards distributed: {sum_poc_amounts}");

        // confirm the unallocated poc reward/dc amounts
        // we can loose up to 1 bone per gateway for each of beacon_amount, witness_amount and dc_amount
        // due to going from decimal to u64
        let unallocated_poc_reward_amount =
            total_rewards_for_poc_and_dc - Decimal::from(allocated_gateway_rewards);
        assert_eq!(unallocated_poc_reward_amount.to_u64().unwrap(), 7);
    }

    #[test]
    fn test_dc_iot_conversion() {
        let iot_price = dec!(359); //iot per token price @ 0.000359 @ 10^6 = 359
        let dc_amount = dec!(1000000);
        // convert the dc amount to iot and assert
        let dc_iot_amt = dc_to_iot_bones(dc_amount, iot_price);
        println!("dc_iot_amt: {dc_iot_amt}");
        assert_eq!(dc_iot_amt, dec!(27855153203.342618384401115));

        // convert the returned iot amount back to dc and assert
        // it matches our original dc amount
        let iot_dc_amt = iot_bones_to_dc(dc_iot_amt, iot_price);
        println!("iot_dc_amt: {iot_dc_amt}");
        assert_eq!(iot_dc_amt, dc_amount);
    }
}
