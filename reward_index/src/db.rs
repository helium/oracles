use std::time::Duration;

use crate::indexer::RewardType;
use chrono::{DateTime, Utc};
use sqlx::Postgres;

pub async fn insert(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    address: String,
    amount: u64,
    reward_type: RewardType,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    // Safeguard against 0 amount shares updating the last rewarded timestamp
    if amount == 0 {
        return Ok(());
    }

    sqlx::query(
        r#"
        insert into reward_index (
                address,
                rewards,
                last_reward,
                reward_type
            ) values ($1, $2, $3, $4)
            on conflict(address) do update set
                rewards = reward_index.rewards + EXCLUDED.rewards,
                last_reward = EXCLUDED.last_reward
        "#,
    )
    .bind(address)
    .bind(amount as i64)
    .bind(timestamp)
    .bind(reward_type)
    .execute(executor)
    .await?;

    Ok(())
}

pub async fn insert_escrowed_reward(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    address: String,
    amount: u64,
    reward_type: RewardType,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    // Safeguard against 0 amount shares updating the last rewarded timestamp
    if amount == 0 {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO escrow_rewards
            (address, amount, reward_type, inserted_at)
        VALUES
            ($1, $2, $3, $4)
        "#,
    )
    .bind(address)
    .bind(amount as i64)
    .bind(reward_type)
    .bind(timestamp)
    .execute(executor)
    .await?;

    Ok(())
}

pub async fn unlock_escrowed_rewards(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
    default_escrow_duration: u32,
) -> anyhow::Result<usize> {
    let res = sqlx::query(
        r#"
            WITH unlocked_rewards as (
              UPDATE
                escrow_rewards up
              SET
                unlocked = true
              -- requerying from escrow_rewards is needed to LEFT JOIN on escrow_durations
              FROM
                escrow_rewards rew
                LEFT JOIN escrow_durations dur ON rew.address = dur.address
              WHERE
                rew.address = up.address
                AND up.inserted_at <= $1 - INTERVAL '1 day' * COALESCE(dur.duration_days, $2)
                AND up.unlocked = false
              RETURNING
                up.*
            )
            INSERT INTO
              reward_index (address, rewards, last_reward, reward_type)
            SELECT
              address,
              sum(amount) as rewards,
              max(inserted_at) as last_reward,
              reward_type
            FROM
              unlocked_rewards
            GROUP BY
              address,
              reward_type
            ON CONFLICT (address)
            DO UPDATE SET
              rewards = reward_index.rewards + EXCLUDED.rewards,
              last_reward = EXCLUDED.last_reward;
        "#,
    )
    .bind(timestamp)
    .bind(default_escrow_duration as i32)
    .execute(executor)
    .await?;

    Ok(res.rows_affected() as usize)
}

pub async fn get_escrow_duration(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    address: &str,
) -> Result<Option<Duration>, sqlx::Error> {
    let seconds: Option<i64> =
        sqlx::query_scalar("SELECT duration_secs from escrow_durations where address = $1")
            .bind(address)
            .fetch_optional(executor)
            .await?;

    let dur = seconds.map(|s| Duration::from_secs(s as u64));
    Ok(dur)
}

pub async fn insert_escrow_duration(
    executor: impl sqlx::Executor<'_, Database = Postgres>,
    address: &str,
    duration_days: u32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO escrow_durations
            (address, duration_days)
        VALUES 
            ($1, $2)
        "#,
    )
    .bind(address)
    .bind(duration_days as i64)
    .execute(executor)
    .await?;

    Ok(())
}

pub async fn delete_escrow_duration(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    address: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM escrow_durations WHERE address = $1")
        .bind(address)
        .execute(executor)
        .await?;

    Ok(())
}
