use chrono::{DateTime, Duration, TimeZone, Utc};
use helium_iceberg::IcebergTestHarness;
use helium_iceberg_oracles::rewards::{
    self, IcebergGatewayReward, IcebergServiceProviderReward, IcebergUnallocatedReward,
};
use reward_index::{
    iceberg::{self, handle_rewards},
    reward::RewardType,
    settings::IcebergSettings,
};
use sqlx::PgPool;

use crate::common;

const UNALLOCATED_KEY: &str = "unallocated-key";

fn epoch(day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, day, 0, 0, 0).unwrap()
}

fn gateway(
    hotspot_key: &str,
    dc_transfer_reward: u64,
    start: DateTime<Utc>,
) -> IcebergGatewayReward {
    IcebergGatewayReward {
        hotspot_key: hotspot_key.to_string(),
        dc_transfer_reward,
        rewardable_bytes: 0,
        price: 0,
        start_period: start.into(),
        end_period: (start + Duration::days(1)).into(),
    }
}

fn service_provider(amount: u64, start: DateTime<Utc>) -> IcebergServiceProviderReward {
    IcebergServiceProviderReward {
        service_provider_id: 0,
        amount,
        rewardable_entity_key: "network".to_string(),
        start_period: start.into(),
        end_period: (start + Duration::days(1)).into(),
    }
}

fn unallocated(amount: u64, start: DateTime<Utc>) -> IcebergUnallocatedReward {
    IcebergUnallocatedReward {
        reward_type: "data".to_string(),
        amount,
        start_period: start.into(),
        end_period: (start + Duration::days(1)).into(),
    }
}

async fn handle<T: iceberg::IcebergReward>(
    pool: &PgPool,
    rows: Vec<T>,
    start_epoch_period: DateTime<Utc>,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let mut txn = pool.begin().await?;
    let indexed_through =
        handle_rewards(&mut txn, rows, UNALLOCATED_KEY, start_epoch_period).await?;
    txn.commit().await?;
    Ok(indexed_through)
}

#[sqlx::test]
async fn accumulates_rewards_per_key(pool: PgPool) -> anyhow::Result<()> {
    let indexed_through = handle(
        &pool,
        vec![
            gateway("one", 1, epoch(1)),
            gateway("one", 2, epoch(1)),
            gateway("two", 4, epoch(1)),
        ],
        epoch(1),
    )
    .await?;
    assert_eq!(indexed_through, Some(epoch(2)));

    let one = common::get_reward(&pool, "one", RewardType::MobileGateway).await?;
    assert_eq!(one.rewards, 3);
    assert_eq!(one.last_reward, epoch(2));
    let two = common::get_reward(&pool, "two", RewardType::MobileGateway).await?;
    assert_eq!(two.rewards, 4);

    Ok(())
}

#[sqlx::test]
async fn replayed_epoch_is_not_counted_twice(pool: PgPool) -> anyhow::Result<()> {
    handle(&pool, vec![gateway("one", 5, epoch(1))], epoch(1)).await?;
    let indexed_through = handle(&pool, vec![gateway("one", 5, epoch(1))], epoch(1)).await?;
    assert_eq!(indexed_through, None);

    let one = common::get_reward(&pool, "one", RewardType::MobileGateway).await?;
    assert_eq!(one.rewards, 5);

    Ok(())
}

#[sqlx::test]
async fn same_epoch_in_different_tables_is_counted_once_each(pool: PgPool) -> anyhow::Result<()> {
    handle(&pool, vec![gateway("one", 5, epoch(1))], epoch(1)).await?;
    handle(&pool, vec![service_provider(7, epoch(1))], epoch(1)).await?;
    handle(&pool, vec![unallocated(9, epoch(1))], epoch(1)).await?;

    let gateway = common::get_reward(&pool, "one", RewardType::MobileGateway).await?;
    assert_eq!(gateway.rewards, 5);
    let sp = common::get_reward(&pool, "network", RewardType::MobileServiceProvider).await?;
    assert_eq!(sp.rewards, 7);
    let unallocated =
        common::get_reward(&pool, UNALLOCATED_KEY, RewardType::MobileUnallocated).await?;
    assert_eq!(unallocated.rewards, 9);

    Ok(())
}

#[sqlx::test]
async fn epochs_before_start_are_skipped(pool: PgPool) -> anyhow::Result<()> {
    let indexed_through = handle(
        &pool,
        vec![gateway("one", 1, epoch(1)), gateway("one", 2, epoch(2))],
        epoch(2),
    )
    .await?;
    assert_eq!(indexed_through, Some(epoch(3)));

    let one = common::get_reward(&pool, "one", RewardType::MobileGateway).await?;
    assert_eq!(one.rewards, 2);

    Ok(())
}

#[sqlx::test]
async fn last_reward_does_not_move_backwards(pool: PgPool) -> anyhow::Result<()> {
    handle(&pool, vec![gateway("one", 1, epoch(2))], epoch(1)).await?;
    handle(&pool, vec![gateway("one", 1, epoch(1))], epoch(1)).await?;

    let one = common::get_reward(&pool, "one", RewardType::MobileGateway).await?;
    assert_eq!(one.rewards, 2);
    assert_eq!(one.last_reward, epoch(3));

    Ok(())
}

#[sqlx::test]
async fn zero_rewards_are_not_indexed(pool: PgPool) -> anyhow::Result<()> {
    handle(&pool, vec![gateway("one", 1, epoch(1))], epoch(1)).await?;
    handle(&pool, vec![gateway("one", 0, epoch(2))], epoch(1)).await?;

    let one = common::get_reward(&pool, "one", RewardType::MobileGateway).await?;
    assert_eq!(one.rewards, 1);
    assert_eq!(
        one.last_reward,
        epoch(2),
        "zero reward must not move last_reward"
    );

    Ok(())
}

#[sqlx::test]
async fn streams_snapshots_into_the_index(pool: PgPool) -> anyhow::Result<()> {
    let harness =
        IcebergTestHarness::new_with_tables([rewards::data_transfer::table_definition()?]).await?;
    let writer = harness
        .get_table_writer_in::<IcebergGatewayReward>(
            rewards::NAMESPACE,
            rewards::data_transfer::TABLE_NAME,
        )
        .await?;
    // Epoch 1 predates the cutover and must be skipped.
    writer
        .write_idempotent("rewards-epoch-1", vec![gateway("one", 1, epoch(1))])
        .await?;
    writer
        .write_idempotent(
            "rewards-epoch-2",
            vec![gateway("one", 2, epoch(2)), gateway("two", 3, epoch(2))],
        )
        .await?;
    writer
        .write_idempotent("rewards-epoch-3", vec![gateway("one", 4, epoch(3))])
        .await?;

    let db_dir = tempfile::tempdir()?;
    let settings = IcebergSettings {
        catalog: harness.as_settings(),
        start_epoch_period: epoch(2),
        snapshot_start_after: DateTime::UNIX_EPOCH,
        db_dir: Some(db_dir.path().to_path_buf()),
        poll_duration: std::time::Duration::from_millis(100),
    };
    let (server, indexer) = iceberg::create::<IcebergGatewayReward>(
        harness.iceberg_catalog().clone(),
        &settings,
        pool.clone(),
        UNALLOCATED_KEY.to_string(),
    )
    .await?;

    let (trigger, shutdown) = triggered::trigger();
    let server = tokio::spawn(server.run(shutdown.clone()));
    let indexer = tokio::spawn(indexer.run(shutdown));

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let indexed: i64 = sqlx::query_scalar("select count(*) from processed_writes")
            .fetch_one(&pool)
            .await?;
        if indexed == 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for snapshots to be indexed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    trigger.trigger();
    server.await??;
    indexer.await??;

    let one = common::get_reward(&pool, "one", RewardType::MobileGateway).await?;
    assert_eq!(one.rewards, 6);
    assert_eq!(one.last_reward, epoch(4));
    let two = common::get_reward(&pool, "two", RewardType::MobileGateway).await?;
    assert_eq!(two.rewards, 3);

    Ok(())
}
