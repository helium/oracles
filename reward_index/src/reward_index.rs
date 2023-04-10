use crate::indexer::RewardType;
use chrono::{DateTime, Utc};

pub async fn insert<'c, E>(
    executor: E,
    address: &Vec<u8>,
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
