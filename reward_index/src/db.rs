use std::{collections::HashMap, time::Duration};

use crate::indexer::RewardKey;
use chrono::{DateTime, Utc};

pub use escrow_duration::purge_expired_escrow_duration;

pub async fn insert_rewards(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    mut rewards: HashMap<RewardKey, u64>,
    timestamp: &DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    // Remove rewards with 0 amount
    rewards.retain(|_key, amount| *amount > 0);

    if rewards.is_empty() {
        return Ok(0);
    }

    let rewards_count = rewards.len();

    // Pre-allocate vectors for efficient insertion
    let mut addresses = Vec::with_capacity(rewards_count);
    let mut amounts = Vec::with_capacity(rewards_count);
    let mut reward_types = Vec::with_capacity(rewards_count);

    for (key, amount) in rewards.iter() {
        addresses.push(key.key.as_str());
        amounts.push(*amount as i64);
        reward_types.push(key.reward_type);
    }

    let res = sqlx::query(
        r#"
        INSERT INTO reward_index (
            address,
            rewards,
            claimable,
            reward_type,
            last_reward
        )
        SELECT
            unnest($1::text[]),
            unnest($2::bigint[]),
            unnest($2::bigint[]),
            unnest($3::reward_type[]) ,
            $4  -- Single timestamp for all rows
        ON CONFLICT (address) DO UPDATE SET
            rewards = reward_index.rewards + EXCLUDED.rewards,
            claimable = reward_index.claimable + EXCLUDED.claimable,
            last_reward = EXCLUDED.last_reward
        "#,
    )
    .bind(&addresses)
    .bind(&amounts)
    .bind(&reward_types)
    .bind(timestamp) // Single timestamp for all records
    .execute(executor)
    .await?;

    Ok(res.rows_affected() as usize)
}

pub async fn insert_escrowed_rewards(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    mut rewards: HashMap<RewardKey, u64>,
    timestamp: &DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    // Remove rewards with 0 amount
    rewards.retain(|_key, amount| *amount > 0);

    if rewards.is_empty() {
        return Ok(0);
    }

    let rewards_count = rewards.len();

    // Pre-allocate vectors for efficient insertion
    let mut addresses = Vec::with_capacity(rewards_count);
    let mut amounts = Vec::with_capacity(rewards_count);
    let mut reward_types = Vec::with_capacity(rewards_count);

    for (key, amount) in rewards.iter() {
        addresses.push(key.key.as_str());
        amounts.push(*amount as i64);
        reward_types.push(key.reward_type);
    }

    let res = sqlx::query(
        r#"
        INSERT INTO escrow_rewards (
            address,
            amount,
            reward_type,
            inserted_at
        )
        SELECT
            unnest($1::text[]),
            unnest($2::bigint[]),
            unnest($3::reward_type[]),
            $4  -- Single timestamp for all rows
        "#,
    )
    .bind(&addresses)
    .bind(&amounts)
    .bind(&reward_types)
    .bind(timestamp) // Single timestamp for all records
    .execute(executor)
    .await?;

    Ok(res.rows_affected() as usize)
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
                unlocked_at = $1
              -- requerying from escrow_rewards is needed to LEFT JOIN on escrow_durations
              FROM
                escrow_rewards rew
                LEFT JOIN escrow_durations dur ON rew.address = dur.address
              WHERE
                rew.address = up.address
                AND up.inserted_at <= $1 - INTERVAL '1 day' * COALESCE(dur.duration_days, $2)
                AND up.unlocked_at IS NULL
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

pub async fn purge_historical_escrowed_rewards(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    today: DateTime<Utc>,
    keep_duration: Duration,
) -> anyhow::Result<usize> {
    let res = sqlx::query(
        r#"
            DELETE FROM escrow_rewards
            WHERE unlocked_at <= $1
        "#,
    )
    .bind(today - keep_duration)
    .execute(executor)
    .await?;

    Ok(res.rows_affected() as usize)
}

pub mod escrow_duration {
    use chrono::NaiveDate;
    use sqlx::{Executor, PgPool, Postgres};

    pub async fn purge_expired_escrow_duration(
        executor: impl Executor<'_, Database = Postgres>,
        today: NaiveDate,
    ) -> anyhow::Result<usize> {
        let res = sqlx::query("DELETE FROM escrow_durations where expires_on <= $1")
            .bind(today)
            .execute(executor)
            .await?;

        Ok(res.rows_affected() as usize)
    }

    pub async fn get(
        executor: impl Executor<'_, Database = Postgres>,
        address: &str,
    ) -> Result<Option<(u32, Option<chrono::NaiveDate>)>, sqlx::Error> {
        #[derive(sqlx::FromRow)]
        struct Escrow {
            #[sqlx(try_from = "i64")]
            duration_days: u32,
            expires_on: Option<chrono::NaiveDate>,
        }

        let escrow: Option<Escrow> = sqlx::query_as(
            "SELECT duration_days, expires_on from escrow_durations where address = $1",
        )
        .bind(address)
        .fetch_optional(executor)
        .await?;

        Ok(escrow.map(|e| (e.duration_days, e.expires_on)))
    }

    pub async fn insert(
        executor: impl Executor<'_, Database = Postgres>,
        address: &str,
        duration_days: u32,
        expiration_date: Option<chrono::NaiveDate>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
        INSERT INTO escrow_durations
            (address, duration_days, expires_on)
        VALUES 
            ($1, $2, $3)
        ON CONFLICT (address) DO UPDATE SET
            duration_days = EXCLUDED.duration_days,
            expires_on = EXCLUDED.expires_on
        "#,
        )
        .bind(address)
        .bind(duration_days as i64)
        .bind(expiration_date)
        .execute(executor)
        .await?;

        Ok(())
    }

    pub async fn delete(
        executor: impl Executor<'_, Database = Postgres>,
        address: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM escrow_durations WHERE address = $1")
            .bind(address)
            .execute(executor)
            .await?;

        Ok(())
    }

    pub async fn migrate_known_radios(
        pool: &PgPool,
        expires_on: chrono::NaiveDate,
    ) -> anyhow::Result<usize> {
        let res = sqlx::query(
            r#"
            INSERT INTO 
                escrow_durations (address, duration_days, inserted_at, expires_on)
            SELECT 
                DISTINCT address, 
                0 as duration_days, 
                NOW() as inserted_at, 
                $1 as expires_on
            FROM reward_index
            ON CONFLICT (address) DO NOTHING
        "#,
        )
        .bind(expires_on)
        .execute(pool)
        .await?;

        Ok(res.rows_affected() as usize)
    }
}
