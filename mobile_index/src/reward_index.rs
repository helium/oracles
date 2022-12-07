use chrono::{DateTime, Utc};
use helium_crypto::PublicKey;

pub async fn insert<'c, E>(
    executor: E,
    address: &PublicKey,
    amount: u64,
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
                last_reward
            ) values ($1, $2, $3)
            on conflict(address) do update set
                rewards = reward_index.rewards + EXCLUDED.rewards,
                last_reward = EXCLUDED.last_reward
        "#,
    )
    .bind(address)
    .bind(amount as i64)
    .bind(timestamp)
    .execute(executor)
    .await?;

    Ok(())
}
