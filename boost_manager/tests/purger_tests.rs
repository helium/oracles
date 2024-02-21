mod common;
use boost_manager::purger;
use boost_manager::OnChainStatus;
use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Postgres, Transaction};

const BOOST_HEX_PUBKEY: &str = "J9JiLTpjaShxL8eMvUs8txVw6TZ36E38SiJ89NxnMbLU";
const BOOST_CONFIG_PUBKEY: &str = "BZM1QTud72B2cpTW7PhEnFmRX7ZWzvY7DpPpNJJuDrWG";

#[sqlx::test]
async fn test_purge(pool: PgPool) -> anyhow::Result<()> {
    // seed test data to db
    seed_data(&pool).await?;

    // assert the db contains the expected number of records pre purge
    let count: i64 = sqlx::query_scalar("select count(*) from activated_hexes")
        .fetch_one(&pool)
        .await?;
    assert_eq!(7, count);

    // do da purge
    purger::purge(&pool, Duration::days(7)).await?;

    // assert the db contains the expected number of records post purge
    let count: i64 = sqlx::query_scalar("select count(*) from activated_hexes")
        .fetch_one(&pool)
        .await?;
    assert_eq!(4, count);

    Ok(())
}

async fn seed_data(db: &PgPool) -> anyhow::Result<()> {
    let now = Utc::now();
    let mut txn = db.begin().await?;

    insert_data(
        &mut txn,
        0x8c2681a306601ff_u64,
        OnChainStatus::Queued,
        now - Duration::days(1),
    )
    .await?;
    insert_data(
        &mut txn,
        0x8c2681a306602ff_u64,
        OnChainStatus::Queued,
        now - Duration::days(8),
    )
    .await?;
    insert_data(
        &mut txn,
        0x8c2681a306603ff_u64,
        OnChainStatus::Success,
        now - Duration::days(2),
    )
    .await?;
    insert_data(
        &mut txn,
        0x8c2681a306604ff_u64,
        OnChainStatus::Success,
        now - Duration::days(6),
    )
    .await?;
    insert_data(
        &mut txn,
        0x8c2681a306605ff_u64,
        OnChainStatus::Success,
        now - Duration::days(8),
    )
    .await?;
    insert_data(
        &mut txn,
        0x8c2681a306606ff_u64,
        OnChainStatus::Success,
        now - Duration::days(9),
    )
    .await?;
    insert_data(
        &mut txn,
        0x8c2681a306607ff_u64,
        OnChainStatus::Success,
        now - Duration::days(10),
    )
    .await?;

    txn.commit().await?;

    Ok(())
}

pub async fn insert_data(
    txn: &mut Transaction<'_, Postgres>,
    location: u64,
    status: OnChainStatus,
    last_updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into activated_hexes (
                location,
                activation_ts,
                boosted_hex_pubkey,
                boost_config_pubkey,
                status,
                updated_at
            ) values ($1, $2, $3, $4, $5, $6)
            on conflict do nothing
        "#,
    )
    .bind(location as i64)
    .bind(last_updated_at)
    .bind(BOOST_HEX_PUBKEY)
    .bind(BOOST_CONFIG_PUBKEY)
    .bind(status)
    .bind(last_updated_at)
    .execute(txn)
    .await?;
    Ok(())
}
