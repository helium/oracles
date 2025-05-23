use std::collections::HashMap;
use std::string::ToString;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use helium_proto::{
    service_provider_promotions::Promotion, services::poc_mobile::UnallocatedRewardType,
    ServiceProvider, ServiceProviderPromotions,
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};

use crate::common::{self, reward_info_24_hours, AsStringKeyedMap};
use mobile_config::client::{carrier_service_client::CarrierServiceVerifier, ClientError};
use mobile_verifier::{data_session, reward_shares, rewarder, service_provider};

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";
const PAYER_1: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const PAYER_2: &str = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp";
const SP_1: &str = "Helium Mobile";

pub type ValidSpMap = HashMap<String, String>;

#[derive(Debug, Clone)]
pub struct MockCarrierServiceClient {
    pub valid_sps: ValidSpMap,
    pub promotions: Vec<ServiceProviderPromotions>,
}

impl MockCarrierServiceClient {
    fn new(valid_sps: ValidSpMap) -> Self {
        Self {
            valid_sps,
            promotions: vec![],
        }
    }

    fn with_promotions(self, promotions: Vec<ServiceProviderPromotions>) -> Self {
        Self { promotions, ..self }
    }
}

#[async_trait]
impl CarrierServiceVerifier for MockCarrierServiceClient {
    async fn payer_key_to_service_provider(
        &self,
        pubkey: &str,
    ) -> Result<ServiceProvider, ClientError> {
        match self.valid_sps.get(pubkey) {
            Some(v) => Ok(ServiceProvider::from_str(v)
                .map_err(|_| ClientError::UnknownServiceProvider(pubkey.to_string()))?),
            None => Err(ClientError::UnknownServiceProvider(pubkey.to_string())),
        }
    }

    async fn list_incentive_promotions(
        &self,
        _epoch_start: &DateTime<Utc>,
    ) -> Result<Vec<ServiceProviderPromotions>, ClientError> {
        Ok(self.promotions.clone())
    }
}

#[sqlx::test]
async fn test_service_provider_rewards(pool: PgPool) -> anyhow::Result<()> {
    let mut valid_sps = HashMap::<String, String>::new();
    valid_sps.insert(PAYER_1.to_string(), SP_1.to_string());
    let carrier_client = MockCarrierServiceClient::new(valid_sps);
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    // seed db with test specific data
    let mut txn = pool.clone().begin().await?;
    seed_hotspot_data(reward_info.epoch_period.end, &mut txn).await?;
    txn.commit().await?;

    let dc_sessions =
        service_provider::get_dc_sessions(&pool, &carrier_client, &reward_info.epoch_period)
            .await?;
    let sp_promotions = carrier_client
        .list_incentive_promotions(&reward_info.epoch_period.start)
        .await?;

    rewarder::reward_service_providers(
        dc_sessions,
        sp_promotions.into(),
        mobile_rewards_client,
        &reward_info,
        dec!(0.0001),
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;

    let sp_reward = rewards.sp_rewards.first().expect("sp 1 reward");
    assert_eq!(5_999, sp_reward.amount);

    let unallocated_reward = rewards.unallocated.first().expect("unallocated");
    assert_eq!(
        UnallocatedRewardType::ServiceProvider as i32,
        unallocated_reward.reward_type
    );
    assert_eq!(8_219_178_076_192, unallocated_reward.amount);

    // confirm the total rewards allocated matches expectations
    let expected_sum =
        reward_shares::get_scheduled_tokens_for_service_providers(reward_info.epoch_emissions)
            .to_u64()
            .unwrap();
    assert_eq!(expected_sum, sp_reward.amount + unallocated_reward.amount);

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(unallocated_reward.amount) / reward_info.epoch_emissions)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.1));

    Ok(())
}

#[sqlx::test]
async fn test_service_provider_rewards_halt_on_invalid_sp(pool: PgPool) -> anyhow::Result<()> {
    // only payer 1 has a corresponding SP key
    // data sessions from payer 2 will result in an error, halting rewards
    let mut valid_sps = HashMap::<String, String>::new();
    valid_sps.insert(PAYER_1.to_string(), SP_1.to_string());
    let carrier_client = MockCarrierServiceClient::new(valid_sps);

    let reward_info = reward_info_24_hours();

    let mut txn = pool.clone().begin().await?;
    seed_hotspot_data_invalid_sp(reward_info.epoch_period.end, &mut txn).await?;
    txn.commit().await.expect("db txn failed");

    let dc_sessions =
        service_provider::get_dc_sessions(&pool, &carrier_client, &reward_info.epoch_period).await;
    assert_eq!(
        dc_sessions.unwrap_err().to_string(),
        format!("unknown service provider {PAYER_2}")
    );
    // This is where rewarding would happen if we could properly fetch dc_sessions

    Ok(())
}

#[sqlx::test]
async fn test_service_provider_promotion_rewards(pool: PgPool) -> anyhow::Result<()> {
    // Single SP has allocated shares for a few of their subscribers.
    // Rewards are matched by the unallocated SP rewards for the subscribers

    let valid_sps = HashMap::from_iter([(PAYER_1.to_string(), SP_1.to_string())]);
    // promotions allocated 15.00%
    let carrier_client =
        MockCarrierServiceClient::new(valid_sps).with_promotions(vec![ServiceProviderPromotions {
            service_provider: 0,
            incentive_escrow_fund_bps: 1500,
            promotions: vec![
                Promotion {
                    entity: "one".to_string(),
                    shares: 1,
                    ..Default::default()
                },
                Promotion {
                    entity: "two".to_string(),
                    shares: 2,
                    ..Default::default()
                },
                Promotion {
                    entity: "three".to_string(),
                    shares: 3,
                    ..Default::default()
                },
            ],
        }]);

    let reward_info = reward_info_24_hours();

    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let mut txn = pool.begin().await?;
    seed_hotspot_data(reward_info.epoch_period.end, &mut txn).await?; // DC transferred == 6,000 reward amount

    txn.commit().await?;

    let dc_sessions =
        service_provider::get_dc_sessions(&pool, &carrier_client, &reward_info.epoch_period)
            .await?;
    let sp_promotions = carrier_client
        .list_incentive_promotions(&reward_info.epoch_period.start)
        .await?;

    rewarder::reward_service_providers(
        dc_sessions,
        sp_promotions.into(),
        mobile_rewards_client,
        &reward_info,
        dec!(0.00001),
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;
    let promo_rewards = rewards.promotion_rewards.as_keyed_map();

    // 1 share
    let promo_reward_1 = promo_rewards.get("one").expect("promo 1");
    assert_eq!(promo_reward_1.service_provider_amount, 1_499);
    assert_eq!(promo_reward_1.matched_amount, 1_499);

    // 2 shares
    let promo_reward_2 = promo_rewards.get("two").expect("promo 2");
    assert_eq!(promo_reward_2.service_provider_amount, 2999);
    assert_eq!(promo_reward_2.matched_amount, 2999);

    // 3 shares
    let promo_reward_3 = promo_rewards.get("three").expect("promo 3");
    assert_eq!(promo_reward_3.service_provider_amount, 4_499);
    assert_eq!(promo_reward_3.matched_amount, 4_499);

    // dc_percentage * total_sp_allocation rounded down
    let sp_reward = rewards.sp_rewards.first().expect("sp 1 reward");
    assert_eq!(sp_reward.amount, 50_999);

    let unallocated_sp_rewards = get_unallocated_sp_rewards(reward_info.epoch_emissions);
    let expected_unallocated = unallocated_sp_rewards
        - 50_999 // 85% service provider rewards rounded down
        - 8_998 // 15% service provider promotions
        - 8_998 // matched promotion
        + 2; // rounding

    let unallocated = rewards.unallocated.first().expect("unallocated");
    assert_eq!(unallocated.amount, expected_unallocated);

    Ok(())
}

async fn seed_hotspot_data(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let data_session_1 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_1.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 10000,
        rewardable_bytes: 1024 * 1000 + 1024 * 10000,
        num_dcs: 10_000,
        received_timestamp: ts - Duration::hours(1),
        burn_timestamp: ts - Duration::hours(1),
    };

    let data_session_2 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_1.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 50000,
        rewardable_bytes: 1024 * 1000 + 1024 * 50000,
        num_dcs: 50_000,
        received_timestamp: ts - Duration::hours(2),
        burn_timestamp: ts - Duration::hours(2),
    };

    data_session_1.save(txn).await?;
    data_session_2.save(txn).await?;
    Ok(())
}

async fn seed_hotspot_data_invalid_sp(
    ts: DateTime<Utc>,
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let data_session_1 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_1.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 10000,
        rewardable_bytes: 1024 * 1000 + 1024 * 10000,
        num_dcs: 10_000,
        received_timestamp: ts - Duration::hours(2),
        burn_timestamp: ts - Duration::hours(2),
    };

    let data_session_2 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_2.parse().unwrap(),
        payer: PAYER_2.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 50000,
        rewardable_bytes: 1024 * 1000 + 1024 * 50000,
        num_dcs: 50_000,
        received_timestamp: ts - Duration::hours(2),
        burn_timestamp: ts - Duration::hours(2),
    };

    data_session_1.save(txn).await?;
    data_session_2.save(txn).await?;
    Ok(())
}

// Helper for turning Decimal -> u64 to compare against output rewards
fn get_unallocated_sp_rewards(total_emissions: Decimal) -> u64 {
    reward_shares::get_scheduled_tokens_for_service_providers(total_emissions)
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
}
