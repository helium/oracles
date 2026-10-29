use crate::reward::RewardType;
use chrono::{DateTime, Utc};

pub async fn insert<'c, E>(
    executor: E,
    address: String,
    amount: u64,
    reward_type: RewardType,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
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
                last_reward = greatest(reward_index.last_reward, EXCLUDED.last_reward)
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

/// Record that the rewards identified by `write_id` have been indexed.
///
/// Returns `false` if `write_id` was already recorded, in which case the caller
/// must not index those rewards again. Run it in the same transaction as the
/// reward inserts so the two commit (or roll back) together.
pub async fn insert_processed_write<'c, E>(executor: E, write_id: &str) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let result = sqlx::query(
        r#"
        insert into processed_writes (write_id) values ($1)
            on conflict(write_id) do nothing
        "#,
    )
    .bind(write_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected() == 1)
}
