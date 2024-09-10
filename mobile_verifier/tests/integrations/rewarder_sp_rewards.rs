use std::collections::HashMap;
use std::string::ToString;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use file_store::promotion_reward::{self, PromotionReward};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_mobile::{
        MobileRewardShare, ServiceProviderReward, UnallocatedReward, UnallocatedRewardType,
    },
    ServiceProvider,
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use solana::{carrier::SolanaNetwork, SolanaRpcError};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::common::{self, MockFileSinkReceiver};
use mobile_config::client::{carrier_service_client::CarrierServiceVerifier, ClientError};
use mobile_verifier::{
    data_session, promotion_reward::save_promotion_reward, reward_shares, rewarder,
};

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";
const PAYER_1: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const PAYER_2: &str = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp";
const SP_1: &str = "Helium Mobile";

pub type ValidSpMap = HashMap<String, String>;

#[derive(Debug, Clone)]
pub struct MockCarrierServiceClient {
    pub valid_sps: ValidSpMap,
}

impl MockCarrierServiceClient {
    fn new(valid_sps: ValidSpMap) -> Self {
        Self { valid_sps }
    }
}

#[async_trait]
impl CarrierServiceVerifier for MockCarrierServiceClient {
    type Error = ClientError;

    async fn payer_key_to_service_provider_name(
        &self,
        pubkey: &str,
    ) -> Result<String, ClientError> {
        match self.valid_sps.get(pubkey) {
            Some(v) => Ok(v.clone()),
            None => Err(ClientError::UnknownServiceProvider(pubkey.to_string())),
        }
    }

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
}

#[sqlx::test]
async fn test_service_provider_rewards(pool: PgPool) -> anyhow::Result<()> {
    let mut valid_sps = HashMap::<String, String>::new();
    valid_sps.insert(PAYER_1.to_string(), SP_1.to_string());
    let carrier_client = MockCarrierServiceClient::new(valid_sps);
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;

    // seed db with test specific data
    let mut txn = pool.clone().begin().await?;
    seed_hotspot_data(epoch.end, &mut txn).await?;
    txn.commit().await?;

    let (_, rewards) = tokio::join!(
        rewarder::reward_service_providers(
            &pool,
            &None,
            &carrier_client,
            &mobile_rewards_client,
            &epoch,
            dec!(0.0001),
        ),
        receive_expected_rewards(&mut mobile_rewards)
    );
    if let Ok((sp_reward, unallocated_reward)) = rewards {
        assert_eq!(
            SP_1.to_string(),
            ServiceProvider::try_from(sp_reward.service_provider_id)
                .unwrap()
                .to_string()
        );
        assert_eq!(6000, sp_reward.amount);

        assert_eq!(
            UnallocatedRewardType::ServiceProvider as i32,
            unallocated_reward.reward_type
        );
        assert_eq!(8_219_178_076_191, unallocated_reward.amount);
        // confirm the total rewards allocated matches expectations
        let expected_sum =
            reward_shares::get_scheduled_tokens_for_service_providers(epoch.end - epoch.start)
                .to_u64()
                .unwrap();
        assert_eq!(expected_sum, sp_reward.amount + unallocated_reward.amount);

        // confirm the rewarded percentage amount matches expectations
        let daily_total = reward_shares::get_total_scheduled_tokens(epoch.end - epoch.start);
        let percent = (Decimal::from(unallocated_reward.amount) / daily_total)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(percent, dec!(0.1));
    } else {
        panic!("no rewards received");
    }

    Ok(())
}

#[sqlx::test]
async fn test_service_provider_rewards_invalid_sp(pool: PgPool) -> anyhow::Result<()> {
    // only payer 1 has a corresponding SP key
    // data sessions from payer 2 will result in an error, halting rewards
    let mut valid_sps = HashMap::<String, String>::new();
    valid_sps.insert(PAYER_1.to_string(), SP_1.to_string());
    let carrier_client = MockCarrierServiceClient::new(valid_sps);

    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;

    let mut txn = pool.clone().begin().await?;
    seed_hotspot_data_invalid_sp(epoch.end, &mut txn).await?;
    txn.commit().await.expect("db txn failed");

    let resp = rewarder::reward_service_providers(
        &pool.clone(),
        &None,
        &carrier_client.clone(),
        &mobile_rewards_client,
        &epoch,
        dec!(0.0001),
    )
    .await;
    assert_eq!(
        resp.unwrap_err().to_string(),
        "unknown service provider ".to_string() + PAYER_2
    );

    // confirm we get no msgs as rewards halted
    mobile_rewards.assert_no_messages();
    Ok(())
}

#[sqlx::test]
async fn test_service_provider_promotion_rewards(pool: PgPool) -> anyhow::Result<()> {
    // Single SP has allocated shares for a few of their subscribers.
    // Rewards are matched by the unallocated SP rewards for the subscribers

    let valid_sps = HashMap::from_iter([(PAYER_1.to_string(), SP_1.to_string())]);
    let carrier_client = MockCarrierServiceClient::new(valid_sps);

    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;
    let (mobile_rewards_client, mut mobile_rewards) = common::create_file_sink();

    let mut txn = pool.begin().await?;
    seed_hotspot_data(epoch.end, &mut txn).await?; // DC transferred == 6,000 reward amount
    seed_sp_promotion_rewards_with_random_subscribers(
        PAYER_1.to_string().parse().unwrap(),
        &[1, 2, 3],
        &mut txn,
    )
    .await?;
    txn.commit().await?;

    // Standin for Solana when grabbing Service Provider incentive percentage
    struct SPPromotionAllocation(Decimal);

    #[async_trait::async_trait]
    impl SolanaNetwork for SPPromotionAllocation {
        async fn fetch_incentive_escrow_fund_percent(
            &self,
            _network_name: &str,
        ) -> Result<Decimal, SolanaRpcError> {
            Ok(self.0)
        }
    }

    let sp_promo_allocation = SPPromotionAllocation(dec!(0.05));

    let (_, rewards) = tokio::join!(
        rewarder::reward_service_providers(
            &pool,
            &sp_promo_allocation,
            &carrier_client,
            &mobile_rewards_client,
            &epoch,
            dec!(0.0001)
        ),
        async move {
            let mut promos = vec![
                mobile_rewards.receive_promotion_reward().await,
                mobile_rewards.receive_promotion_reward().await,
                mobile_rewards.receive_promotion_reward().await,
            ];
            // sort by awarded amount least -> most
            promos.sort_by_key(|a| a.service_provider_amount);

            let sp_reward = mobile_rewards.receive_service_provider_reward().await;
            let unallocated = mobile_rewards.receive_unallocated_reward().await;

            mobile_rewards.assert_no_messages();

            (promos, sp_reward, unallocated)
        }
    );

    let (promos, sp_reward, unallocated) = rewards;
    let promo_reward_1 = promos[0].clone();
    let promo_reward_2 = promos[1].clone();
    let promo_reward_3 = promos[2].clone();

    // 1 share
    assert_eq!(promo_reward_1.service_provider_amount, 50);
    assert_eq!(promo_reward_1.matched_amount, 50);

    // 2 shares
    assert_eq!(promo_reward_2.service_provider_amount, 100);
    assert_eq!(promo_reward_2.matched_amount, 100);

    // 3 shares
    assert_eq!(promo_reward_3.service_provider_amount, 150);
    assert_eq!(promo_reward_3.matched_amount, 150);

    assert_eq!(sp_reward.amount, 5700);

    let unallocated_sp_rewards = get_unallocated_sp_rewards(&epoch);
    let expected_unallocated = unallocated_sp_rewards
        - 5700 // 95% service provider rewards
        - 300 // 5% service provider promotions
        - 300; // matched promotion

    assert_eq!(unallocated.amount, expected_unallocated);

    Ok(())
}

async fn receive_expected_rewards(
    mobile_rewards: &mut MockFileSinkReceiver<MobileRewardShare>,
) -> anyhow::Result<(ServiceProviderReward, UnallocatedReward)> {
    // get the filestore outputs from rewards run
    // we will have 3 radio rewards, 1 wifi radio and 2 cbrs radios
    let sp_reward1 = mobile_rewards.receive_service_provider_reward().await;
    // let sp_reward2 = mobile_rewards.receive_service_provider_reward().await;
    // dump the sp rewards into a vec and sort to get a deteminstic order

    // expect one unallocated reward
    let unallocated_reward = mobile_rewards.receive_unallocated_reward().await;

    // should be no further msgs
    mobile_rewards.assert_no_messages();

    Ok((sp_reward1, unallocated_reward))
}

// Service Provider promotion rewards are verified during ingest. When you write
// directly to the database, the assumption is the entity and the payer are
// valid.
async fn seed_sp_promotion_rewards_with_random_subscribers(
    payer: PublicKeyBinary,
    sub_shares: &[u64],
    txn: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    for &shares in sub_shares {
        save_promotion_reward(
            txn,
            &PromotionReward {
                entity: promotion_reward::Entity::SubscriberId(Uuid::new_v4().into()),
                shares,
                timestamp: Utc::now() - chrono::Duration::hours(2),
                received_timestamp: Utc::now(),
                carrier_pub_key: payer.clone(),
                signature: vec![],
            },
        )
        .await?;
    }

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
        num_dcs: 10000,
        received_timestamp: ts - ChronoDuration::hours(1),
    };

    let data_session_2 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_1.parse().unwrap(),
        payer: PAYER_1.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 50000,
        num_dcs: 50000,
        received_timestamp: ts - ChronoDuration::hours(2),
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
        num_dcs: 10000,
        received_timestamp: ts - ChronoDuration::hours(2),
    };

    let data_session_2 = data_session::HotspotDataSession {
        pub_key: HOTSPOT_2.parse().unwrap(),
        payer: PAYER_2.parse().unwrap(),
        upload_bytes: 1024 * 1000,
        download_bytes: 1024 * 50000,
        num_dcs: 50000,
        received_timestamp: ts - ChronoDuration::hours(2),
    };

    data_session_1.save(txn).await?;
    data_session_2.save(txn).await?;
    Ok(())
}

// Helper for turning Decimal -> u64 to compare against output rewards
fn get_unallocated_sp_rewards(epoch: &std::ops::Range<DateTime<Utc>>) -> u64 {
    reward_shares::get_scheduled_tokens_for_service_providers(epoch.end - epoch.start)
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
}
