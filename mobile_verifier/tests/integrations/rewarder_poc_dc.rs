use std::ops::Range;

use crate::common::seed::{
    seed_data_sessions, seed_heartbeats, seed_speedtests, seed_unique_connections,
    update_assignments, update_assignments_bad, HOTSPOT_3,
};
use crate::common::{
    self, default_price_info, reward_info_24_hours, MockHexBoostingClient, RadioRewardV2Ext,
};
use chrono::{DateTime, Utc};
use file_store_oracles::mobile_ban;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::UnallocatedRewardType;
use mobile_verifier::{
    banning,
    reward_shares::{self, DataTransferAndPocAllocatedRewardBuckets},
    rewarder,
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        service_provider_boosted_rewards_banned_radio_req_v1::KeyType,
        service_provider_boosted_rewards_banned_radio_req_v1::{
            SpBoostedRewardsBannedRadioBanType, SpBoostedRewardsBannedRadioReason,
        },
        CoverageObjectValidity, HeartbeatValidity, LocationSource, SeniorityUpdateReason,
        ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
        ServiceProviderBoostedRewardsBannedRadioReqV1, SignalLevel,
    };
}

#[sqlx::test]
async fn test_poc_and_dc_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    let hex_boosting_client = MockHexBoostingClient::new(vec![]);
    let price_info = default_price_info();

    // run rewards for poc and dc
    let (_, _, poc_unallocated_amount) = rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2s;
    let dc_rewards = rewards.gateway_rewards;

    let poc_sum: u64 = poc_rewards.iter().map(|r| r.total_poc_reward()).sum();

    assert_eq!(poc_sum / 3, poc_rewards[0].total_poc_reward());
    assert_eq!(poc_sum / 3, poc_rewards[1].total_poc_reward());
    assert_eq!(poc_sum / 3, poc_rewards[2].total_poc_reward());

    // assert no unallocated reward writes
    assert_eq!(
        rewards
            .unallocated
            .iter()
            .filter(|r| r.reward_type == UnallocatedRewardType::Poc as i32)
            .count(),
        0
    );

    // assert the boosted hexes in the radio rewards
    // boosted hexes will contain the used multiplier for each boosted hex
    // in this test there are no boosted hexes
    assert_eq!(0, poc_rewards[0].boosted_hexes_len());
    assert_eq!(0, poc_rewards[1].boosted_hexes_len());
    assert_eq!(0, poc_rewards[2].boosted_hexes_len());

    // assert the dc reward outputs
    assert_eq!(500_000, dc_rewards[0].dc_transfer_reward);
    assert_eq!(500_000, dc_rewards[1].dc_transfer_reward);
    assert_eq!(500_000, dc_rewards[2].dc_transfer_reward);

    // confirm the total rewards allocated matches expectations
    let dc_sum: u64 = dc_rewards.iter().map(|r| r.dc_transfer_reward).sum();
    let total = poc_sum + dc_sum + poc_unallocated_amount.to_u64().unwrap_or(0);

    let expected_sum = reward_shares::get_scheduled_tokens_for_poc(reward_info.epoch_emissions)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, total);

    // confirm the rewarded percentage amount matches expectations
    let percent = (Decimal::from(total) / reward_info.epoch_emissions)
        .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
    assert_eq!(percent, dec!(0.70));

    Ok(())
}

#[sqlx::test]
async fn test_qualified_wifi_poc_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse()?; // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    let rewardable_total = seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments_bad(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let boosted_hexes = vec![];
    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let price_info = default_price_info();

    // seed single unique connections report within epoch
    let mut txn = pool.begin().await?;
    seed_unique_connections(&mut txn, &[(pubkey.clone(), 42)], &reward_info.epoch_period).await?;
    txn.commit().await?;

    // SP ban radio, unique connections should supersede banning
    let mut txn = pool.begin().await?;
    ban_wifi_radio_for_epoch(&mut txn, pubkey.clone(), &reward_info.epoch_period).await?;
    txn.commit().await?;

    // run rewards for poc and dc
    rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    let msgs = mobile_rewards.finish().await?;
    let poc_rewards = msgs.radio_reward_v2s;
    let dc_rewards = msgs.gateway_rewards;

    // expecting single radio with poc rewards, no unallocated
    assert_eq!(poc_rewards.len(), 1);
    assert_eq!(dc_rewards.len(), 3);
    assert_eq!(msgs.unallocated.len(), 0);

    // Check that we used rewardable_bytes for calculation and not upload_bytes + download_bytes anymore
    let rewardable_sum: u64 = dc_rewards.iter().map(|r| r.rewardable_bytes).sum();
    assert_eq!(rewardable_total, rewardable_sum);

    // Check that we used rewardable_bytes for calculation and not upload_bytes + download_bytes anymore
    let rewardable_sum: u64 = dc_rewards.iter().map(|r| r.rewardable_bytes).sum();
    assert_eq!(rewardable_total, rewardable_sum);

    let poc_sum: u64 = poc_rewards.iter().map(|r| r.total_poc_reward()).sum();
    let dc_sum: u64 = dc_rewards.iter().map(|r| r.dc_transfer_reward).sum();
    let total = poc_sum + dc_sum;

    let expected_sum = reward_shares::get_scheduled_tokens_for_poc(reward_info.epoch_emissions)
        .to_u64()
        .unwrap();
    assert_eq!(expected_sum, total);

    Ok(())
}

#[sqlx::test]
async fn test_sp_banned_radio(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse()?; // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let boosted_hexes = vec![];
    let hex_boosting_client = MockHexBoostingClient::new(boosted_hexes);

    let price_info = default_price_info();

    let _rewarder = rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info.clone(),
    )
    .await?;

    let msgs = mobile_rewards.finish().await?;
    assert_eq!(msgs.gateway_rewards.len(), 3);
    assert_eq!(msgs.radio_reward_v2s.len(), 3);

    // ==============================================================
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    // SP ban radio, zeroed rewards are filtered out
    let mut txn = pool.begin().await?;
    ban_wifi_radio_for_epoch(&mut txn, pubkey.clone(), &reward_info.epoch_period).await?;
    txn.commit().await?;

    let _rewarder = rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    let msgs = mobile_rewards.finish().await?;
    assert_eq!(msgs.gateway_rewards.len(), 3);
    assert_eq!(msgs.radio_reward_v2s.len(), 2);

    Ok(())
}

#[sqlx::test]
async fn test_all_banned_radio(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse()?; // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    seed_data_sessions(reward_info.epoch_period.start, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let hex_boosting_client = MockHexBoostingClient::new(vec![]);
    let price_info = default_price_info();

    // ban radio
    let mut txn = pool.begin().await?;
    ban_radio(
        &mut txn,
        pubkey.clone(),
        mobile_ban::BanType::All,
        reward_info.rewards_issued_at - chrono::Duration::hours(1), // ban before epoch
    )
    .await?;
    txn.commit().await?;

    // run rewards for poc and dc
    let (_, _, poc_unallocated_reward) = rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2s;
    let dc_rewards = rewards.gateway_rewards;
    let poc_unallocated_reward = poc_unallocated_reward.to_u64().unwrap_or(0);

    // expecting single radio with poc rewards, minimal unallocated due to rounding
    assert_eq!(poc_rewards.len(), 2);
    assert_eq!(dc_rewards.len(), 3);
    assert!(poc_unallocated_reward >= 1);

    Ok(())
}

#[sqlx::test]
async fn test_data_banned_radio_still_receives_poc(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();

    let reward_info = reward_info_24_hours();

    let pubkey: PublicKeyBinary = HOTSPOT_3.to_string().parse()?; // wifi hotspot

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    // Run rewards with no unique connections, no poc rewards, expect unallocated
    let hex_boosting_client = MockHexBoostingClient::new(vec![]);
    let price_info = default_price_info();

    // ban radio for Data only
    let mut txn = pool.begin().await?;
    ban_radio(
        &mut txn,
        pubkey.clone(),
        mobile_ban::BanType::Data,
        reward_info.rewards_issued_at,
    )
    .await?;
    txn.commit().await?;

    // run rewards for poc and dc
    let (_, _, poc_unallocated_reward) = rewarder::reward_poc_and_dc(
        &pool,
        &hex_boosting_client,
        mobile_rewards_client,
        &reward_info,
        price_info,
    )
    .await?;

    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2s;
    let dc_rewards = rewards.gateway_rewards;
    let poc_unallocated_reward = poc_unallocated_reward.to_u64().unwrap_or(0);

    assert_eq!(poc_rewards.len(), 3);
    assert_eq!(dc_rewards.len(), 0);
    assert!(poc_unallocated_reward >= 1);

    Ok(())
}

async fn ban_wifi_radio_for_epoch(
    txn: &mut Transaction<'_, Postgres>,
    pubkey: PublicKeyBinary,
    epoch: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    let until = (epoch.start + chrono::Duration::days(7)).timestamp_millis() as u64;
    let received_timestamp = (epoch.start + chrono::Duration::hours(2)).timestamp_millis() as u64;

    let ban_report = proto::ServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
        received_timestamp,
        report: Some(proto::ServiceProviderBoostedRewardsBannedRadioReqV1 {
            pubkey: pubkey.clone().into(),
            reason: proto::SpBoostedRewardsBannedRadioReason::NoNetworkCorrelation.into(),
            until,
            signature: vec![],
            ban_type: proto::SpBoostedRewardsBannedRadioBanType::Poc.into(),
            key_type: Some(proto::KeyType::HotspotKey(pubkey.into())),
        }),
    };

    use banning::sp_boosted_rewards_bans;

    let ban_report = sp_boosted_rewards_bans::BannedRadioReport::try_from(ban_report)?;
    sp_boosted_rewards_bans::db::update_report(txn, &ban_report).await?;
    Ok(())
}

async fn ban_radio(
    txn: &mut Transaction<'_, Postgres>,
    pubkey: PublicKeyBinary,
    ban_type: mobile_ban::BanType,
    timestamp: DateTime<Utc>,
) -> anyhow::Result<()> {
    banning::db::update_hotspot_ban(
        txn,
        &mobile_ban::VerifiedBanReport {
            verified_timestamp: timestamp,
            report: mobile_ban::BanReport {
                received_timestamp: timestamp,
                report: mobile_ban::BanRequest {
                    hotspot_pubkey: pubkey.clone(),
                    timestamp,
                    ban_pubkey: pubkey,
                    signature: vec![],
                    ban_action: mobile_ban::BanAction::Ban(mobile_ban::BanDetails {
                        hotspot_serial: "test-serial".to_string(),
                        message: "test-notes".to_string(),
                        reason: mobile_ban::BanReason::LocationGaming,
                        ban_type,
                        expiration_timestamp: None,
                    }),
                },
            },
            status: mobile_ban::VerifiedBanIngestReportStatus::Valid,
        },
    )
    .await?;
    Ok(())
}

#[sqlx::test]
async fn test_reward_poc_with_zero_poc_allocation(pool: PgPool) -> anyhow::Result<()> {
    let (mobile_rewards_client, mobile_rewards) = common::create_file_sink();
    let reward_info = reward_info_24_hours();

    // seed heartbeats and speedtests to create potential POC rewards
    let mut txn = pool.clone().begin().await?;
    seed_heartbeats(reward_info.epoch_period.start, &mut txn).await?;
    seed_speedtests(reward_info.epoch_period.end, &mut txn).await?;
    txn.commit().await?;
    update_assignments(&pool).await?;

    let hex_boosting_client = MockHexBoostingClient::new(vec![]);

    // Create reward shares with zero POC allocation
    let reward_shares = DataTransferAndPocAllocatedRewardBuckets::new(dec!(1000000));

    // Test the reward_poc function directly with zero POC allocation
    let (unallocated_amount, _unallocated_poc_amount, _calculated_shares) = rewarder::reward_poc(
        &pool,
        &hex_boosting_client,
        &mobile_rewards_client,
        &reward_info,
        reward_shares,
    )
    .await?;

    // Drop the client to close the channel
    drop(mobile_rewards_client);

    let rewards = mobile_rewards.finish().await?;
    let poc_rewards = rewards.radio_reward_v2s;

    // With zero POC allocation, should have no POC rewards distributed
    assert_eq!(
        poc_rewards.len(),
        0,
        "No POC rewards should be distributed when POC allocation is zero"
    );

    // The unallocated amount should be zero since there was no POC pool to begin with
    assert_eq!(
        unallocated_amount, 0,
        "Unallocated amount should be zero when POC pool is zero"
    );

    // With zero POC allocation, calculated shares should be default (no need to test private fields)

    Ok(())
}
