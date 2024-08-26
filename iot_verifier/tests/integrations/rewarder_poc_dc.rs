use crate::common::{self, MockFileSinkReceiver};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_lora::{
    GatewayReward, IotRewardShare, UnallocatedReward, UnallocatedRewardType,
};
use iot_verifier::{
    poc_report::ReportType,
    reward_share::{self, GatewayDCShare, GatewayPocShare},
    rewarder,
};
use prost::Message;
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use std::{self, str::FromStr};

const HOTSPOT_1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
const HOTSPOT_2: &str = "11uJHS2YaEWJqgqC7yza9uvSmpv5FWoMQXiP8WbxBGgNUmifUJf";
const HOTSPOT_3: &str = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp";
const HOTSPOT_4: &str = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL";

#[sqlx::test]
async fn test_poc_and_dc_rewards(pool: PgPool) -> anyhow::Result<()> {
    let (iot_rewards_client, mut iot_rewards) = common::create_file_sink();
    let now = Utc::now();
    let epoch = (now - ChronoDuration::hours(24))..now;

    // seed all the things
    let mut txn = pool.clone().begin().await?;
    seed_pocs(epoch.start, &mut txn).await?;
    seed_dc(epoch.start, &mut txn).await?;
    txn.commit().await?;

    // run rewards for poc and dc
    let (_, rewards) = tokio::join!(
        rewarder::reward_poc_and_dc(&pool, &iot_rewards_client, &epoch, dec!(0.0001)),
        receive_expected_rewards(&mut iot_rewards)
    );
    if let Ok((gateway_rewards, unallocated_poc_reward)) = rewards {
        // assert the gateway rewards
        assert_eq!(
            gateway_rewards[0].hotspot_key,
            PublicKeyBinary::from_str(HOTSPOT_1).unwrap().as_ref()
        );
        assert_eq!(gateway_rewards[0].beacon_amount, 1_780_821_917_808);
        assert_eq!(gateway_rewards[0].witness_amount, 0);
        assert_eq!(gateway_rewards[0].dc_transfer_amount, 14_840_182_648_401);

        assert_eq!(
            gateway_rewards[1].hotspot_key,
            PublicKeyBinary::from_str(HOTSPOT_2).unwrap().as_ref()
        );
        assert_eq!(gateway_rewards[1].beacon_amount, 0);
        assert_eq!(gateway_rewards[1].witness_amount, 8_547_945_205_479);
        assert_eq!(gateway_rewards[1].dc_transfer_amount, 29_680_365_296_803);
        // hotspot 2 should have double the dc rewards of hotspot 1
        assert_eq!(
            gateway_rewards[1].dc_transfer_amount,
            gateway_rewards[0].dc_transfer_amount * 2 + 1
        );

        assert_eq!(
            gateway_rewards[2].hotspot_key,
            PublicKeyBinary::from_str(HOTSPOT_3).unwrap().as_ref()
        );
        // hotspot 2 has double reward scale of hotspot 1 and thus double the beacon  amount
        assert_eq!(gateway_rewards[2].beacon_amount, 3_561_643_835_616);
        assert_eq!(
            gateway_rewards[2].beacon_amount,
            gateway_rewards[0].beacon_amount * 2
        );
        assert_eq!(gateway_rewards[2].witness_amount, 0);
        assert_eq!(gateway_rewards[2].dc_transfer_amount, 0);

        assert_eq!(
            gateway_rewards[3].hotspot_key,
            PublicKeyBinary::from_str(HOTSPOT_4).unwrap().as_ref()
        );
        assert_eq!(gateway_rewards[3].beacon_amount, 0);
        assert_eq!(gateway_rewards[3].witness_amount, 12_821_917_808_219);
        assert_eq!(gateway_rewards[3].dc_transfer_amount, 0);

        // assert our unallocated reward
        assert_eq!(
            UnallocatedRewardType::Poc as i32,
            unallocated_poc_reward.reward_type
        );
        assert_eq!(2, unallocated_poc_reward.amount);

        // confirm the total rewards allocated matches expectations
        let poc_sum: u64 = gateway_rewards
            .iter()
            .map(|r| r.beacon_amount + r.witness_amount)
            .sum();
        let dc_sum: u64 = gateway_rewards.iter().map(|r| r.dc_transfer_amount).sum();
        let unallocated_sum: u64 = unallocated_poc_reward.amount;

        let expected_dc = reward_share::get_scheduled_dc_tokens(epoch.end - epoch.start);
        let (expected_beacon_sum, expected_witness_sum) =
            reward_share::get_scheduled_poc_tokens(epoch.end - epoch.start, expected_dc);
        let expected_total =
            expected_beacon_sum.to_u64().unwrap() + expected_witness_sum.to_u64().unwrap();
        assert_eq!(expected_total, poc_sum + dc_sum + unallocated_sum);

        // confirm the poc & dc percentage amount matches expectations
        let daily_total = *reward_share::REWARDS_PER_DAY;
        let poc_dc_percent = (Decimal::from(poc_sum + dc_sum + unallocated_sum) / daily_total)
            .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven);
        assert_eq!(poc_dc_percent, dec!(0.8));
    } else {
        panic!("no rewards received");
    };
    Ok(())
}

async fn receive_expected_rewards(
    iot_rewards: &mut MockFileSinkReceiver<IotRewardShare>,
) -> anyhow::Result<(Vec<GatewayReward>, UnallocatedReward)> {
    // get the filestore outputs from rewards run
    // we will have 3 gateway rewards and one unallocated reward
    let gateway_reward1 = iot_rewards.receive_gateway_reward().await;
    let gateway_reward2 = iot_rewards.receive_gateway_reward().await;
    let gateway_reward3 = iot_rewards.receive_gateway_reward().await;
    let gateway_reward4 = iot_rewards.receive_gateway_reward().await;
    let unallocated_poc_reward = iot_rewards.receive_unallocated_reward().await;

    // should be no further msgs
    iot_rewards.assert_no_messages();

    // ordering is not guaranteed, so stick the rewards into a vec and sort
    let mut gateway_rewards = vec![
        gateway_reward1,
        gateway_reward2,
        gateway_reward3,
        gateway_reward4,
    ];
    gateway_rewards.sort_by(|a, b| b.hotspot_key.cmp(&a.hotspot_key));
    Ok((gateway_rewards, unallocated_poc_reward))
}
async fn seed_pocs(ts: DateTime<Utc>, txn: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
    let poc_beacon_1 = GatewayPocShare {
        hotspot_key: HOTSPOT_1.to_string().parse().unwrap(),
        reward_type: ReportType::Beacon,
        reward_timestamp: ts + ChronoDuration::hours(1),
        hex_scale: dec!(1.0),
        reward_unit: dec!(1.0),
        poc_id: "poc_id_1".to_string().encode_to_vec(),
    };

    let poc_witness_1 = GatewayPocShare {
        hotspot_key: HOTSPOT_2.to_string().parse().unwrap(),
        reward_type: ReportType::Witness,
        reward_timestamp: ts + ChronoDuration::hours(1),
        hex_scale: dec!(1.0),
        reward_unit: dec!(1.0),
        poc_id: "poc_id_1".to_string().encode_to_vec(),
    };

    let poc_beacon_2 = GatewayPocShare {
        hotspot_key: HOTSPOT_3.to_string().parse().unwrap(),
        reward_type: ReportType::Beacon,
        reward_timestamp: ts + ChronoDuration::hours(1),
        hex_scale: dec!(1.0),
        reward_unit: dec!(2.0),
        poc_id: "poc_id_2".to_string().encode_to_vec(),
    };

    let poc_witness_2 = GatewayPocShare {
        hotspot_key: HOTSPOT_4.to_string().parse().unwrap(),
        reward_type: ReportType::Witness,
        reward_timestamp: ts + ChronoDuration::hours(1),
        hex_scale: dec!(1.0),
        reward_unit: dec!(1.5),
        poc_id: "poc_id_2".to_string().encode_to_vec(),
    };
    poc_beacon_1.save(txn).await?;
    poc_witness_1.save(txn).await?;

    poc_beacon_2.save(txn).await?;
    poc_witness_2.save(txn).await?;
    Ok(())
}

async fn seed_dc(ts: DateTime<Utc>, txn: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
    let dc_share_1 = GatewayDCShare {
        hotspot_key: HOTSPOT_1.to_string().parse().unwrap(),
        reward_timestamp: ts + ChronoDuration::hours(1),
        num_dcs: dec!(1000),
        id: "dc_id_1".to_string().encode_to_vec(),
    };

    let dc_share_2 = GatewayDCShare {
        hotspot_key: HOTSPOT_2.to_string().parse().unwrap(),
        reward_timestamp: ts + ChronoDuration::hours(1),
        num_dcs: dec!(2000),
        id: "dc_id_2".to_string().encode_to_vec(),
    };

    dc_share_1.save(txn).await?;
    dc_share_2.save(txn).await?;
    Ok(())
}
