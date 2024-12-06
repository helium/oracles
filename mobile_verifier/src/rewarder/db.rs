use std::ops::Range;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

pub async fn no_cbrs_heartbeats(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM cbrs_heartbeats WHERE latest_timestamp >= $1",
    )
    .bind(reward_period.end)
    .fetch_one(pool)
    .await?;

    Ok(count == 0)
}

pub async fn no_wifi_heartbeats(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM wifi_heartbeats WHERE latest_timestamp >= $1",
    )
    .bind(reward_period.end)
    .fetch_one(pool)
    .await?;

    Ok(count == 0)
}

pub async fn no_speedtests(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<bool, anyhow::Error> {
    let count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM speedtests WHERE timestamp >= $1")
            .bind(reward_period.end)
            .fetch_one(pool)
            .await?;

    Ok(count == 0)
}

pub async fn no_unique_connections(
    pool: &PgPool,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<bool> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) from unique_connections WHERE received_timestamp >= $1",
    )
    .bind(reward_period.end)
    .fetch_one(pool)
    .await?;

    Ok(count == 0)
}
