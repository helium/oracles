use crate::OnChainStatus;
use chrono::{DateTime, Utc};
use file_store::hex_boost::BoostedHexActivation;
use sqlx::{postgres::PgRow, FromRow, Pool, Postgres, Row, Transaction};

const MAX_RETRIES: i32 = 10;
const MAX_BATCH_COUNT: i32 = 200;

struct Boost(BoostedHexActivation);

impl FromRow<'_, PgRow> for Boost {
    fn from_row(row: &PgRow) -> sqlx::Result<Boost> {
        let boost = BoostedHexActivation {
            location: row.get::<i64, _>("location") as u64,
            activation_ts: row.get::<DateTime<Utc>, &str>("activation_ts"),
            boosted_hex_pubkey: row.get::<String, &str>("boosted_hex_pubkey"),
            boost_config_pubkey: row.get::<String, &str>("boost_config_pubkey"),
        };
        Ok(Boost(boost))
    }
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct TxnRow {
    pub txn_id: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct StatusRow {
    #[sqlx(try_from = "i64")]
    pub location: u64,
    pub status: OnChainStatus,
}

pub async fn insert_activated_hex(
    txn: &mut Transaction<'_, Postgres>,
    location: u64,
    boosted_hex_pubkey: &String,
    boost_config_pubkey: &String,
    activation_ts: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into activated_hexes (
                location,
                activation_ts,
                boosted_hex_pubkey,
                boost_config_pubkey,
                status
            ) values ($1, $2, $3, $4, $5)
            on conflict do nothing
        "#,
    )
    .bind(location as i64)
    .bind(activation_ts)
    .bind(boosted_hex_pubkey)
    .bind(boost_config_pubkey)
    .bind(OnChainStatus::Queued)
    .execute(txn)
    .await?;

    Ok(())
}

pub async fn get_queued_batch(
    db: &Pool<Postgres>,
) -> Result<Vec<BoostedHexActivation>, sqlx::Error> {
    Ok(sqlx::query_as::<_, Boost>(
        r#"
            SELECT location, activation_ts, boosted_hex_pubkey, boost_config_pubkey
            FROM activated_hexes
            WHERE status = $1 AND retries < $2 AND txn_id IS NULL
            ORDER BY activation_ts ASC
            LIMIT $3;
        "#,
    )
    .bind(OnChainStatus::Queued)
    .bind(MAX_RETRIES)
    .bind(MAX_BATCH_COUNT)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|boost| boost.0)
    .collect::<Vec<BoostedHexActivation>>())
}

pub async fn query_activation_statuses(db: &Pool<Postgres>) -> anyhow::Result<Vec<StatusRow>> {
    Ok(sqlx::query_as::<_, StatusRow>(
        r#"
            SELECT location, status
            FROM activated_hexes
    "#,
    )
    .fetch_all(db)
    .await?)
}

pub async fn save_batch_txn_id(
    db: &Pool<Postgres>,
    txn_id: &str,
    hexes: &[u64],
) -> anyhow::Result<()> {
    let hexes = hexes.iter().map(|x| *x as i64).collect::<Vec<i64>>();
    Ok(sqlx::query(
        r#"
        UPDATE activated_hexes
        SET txn_id = $1, updated_at = $2
        WHERE location IN (SELECT * FROM UNNEST($3))
        "#,
    )
    .bind(txn_id)
    .bind(Utc::now())
    .bind(hexes)
    .execute(db)
    .await
    .map(|_| ())?)
}

pub async fn update_success_batch(db: &Pool<Postgres>, hexes: &[u64]) -> anyhow::Result<()> {
    let hexes = hexes.iter().map(|x| *x as i64).collect::<Vec<i64>>();
    Ok(sqlx::query(
        r#"
        UPDATE activated_hexes
        SET status = $1, updated_at = $2
        WHERE location IN (SELECT * FROM UNNEST($3))
        "#,
    )
    .bind(OnChainStatus::Success)
    .bind(Utc::now())
    .bind(hexes)
    .execute(db)
    .await
    .map(|_| ())?)
}

pub async fn update_failed_batch(db: &Pool<Postgres>, hexes: &[u64]) -> anyhow::Result<()> {
    let hexes = hexes.iter().map(|x| *x as i64).collect::<Vec<i64>>();
    Ok(sqlx::query(
        r#"
        UPDATE activated_hexes
        SET updated_at = $1, retries = retries + 1, txn_id = NULL
        WHERE location IN (SELECT * FROM UNNEST($2))
        "#,
    )
    .bind(Utc::now())
    .bind(hexes)
    .execute(db)
    .await
    .map(|_| ())?)
}

pub async fn update_failed_activations(db: &Pool<Postgres>) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        r#"
        UPDATE activated_hexes
        SET status = $1, updated_at = $2
        WHERE status = $3 AND retries >= $4
        "#,
    )
    .bind(OnChainStatus::Failed)
    .bind(Utc::now())
    .bind(OnChainStatus::Queued)
    .bind(MAX_RETRIES)
    .execute(db)
    .await
    .map(|result| result.rows_affected())?)
}

pub async fn get_failed_activations_count(db: &Pool<Postgres>) -> Result<u64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        " select count(location) from activated_hexes where retries >= 10 and status != 'success' ",
    )
    .bind(MAX_RETRIES)
    .fetch_one(db)
    .await
    .map(|count| count as u64)
}

pub async fn get_txns_ids_to_verify(db: &Pool<Postgres>) -> Result<Vec<TxnRow>, sqlx::Error> {
    sqlx::query_as::<_, TxnRow>(
        r#"
            select distinct(txn_id) from activated_hexes where status = $1 and txn_id is not null
        "#,
    )
    .bind(OnChainStatus::Queued)
    .fetch_all(db)
    .await
}

pub async fn update_verified_txns_onchain(db: &Pool<Postgres>, txn_id: &str) -> anyhow::Result<()> {
    Ok(sqlx::query(
        r#"
        UPDATE activated_hexes
        SET status = $1, updated_at = $2
        WHERE txn_id = $3
        "#,
    )
    .bind(OnChainStatus::Success)
    .bind(Utc::now())
    .bind(txn_id)
    .execute(db)
    .await
    .map(|_| ())?)
}

pub async fn update_verified_txns_not_onchain(
    db: &Pool<Postgres>,
    txn_id: &str,
) -> anyhow::Result<()> {
    Ok(sqlx::query(
        r#"
        UPDATE activated_hexes
        SET txn_id = null, updated_at = $1
        WHERE txn_id = $2
        "#,
    )
    .bind(Utc::now())
    .bind(txn_id)
    .execute(db)
    .await
    .map(|_| ())?)
}
