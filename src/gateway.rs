use crate::{Error, PublicKey, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use helium_proto::BlockchainTxnAddGatewayV1;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Row};
use std::cmp::min;

pub const DEFAULT_GATEWAY_COUNT: usize = 100;
pub const MAX_GATEWAY_COUNT: u32 = 1000;

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct Gateway {
    pub pubkey: PublicKey,
    pub owner: PublicKey,
    pub payer: PublicKey,
    pub height: i64,
    pub txn_hash: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

impl Gateway {
    pub fn from_txn(
        height: u64,
        timestamp: u64,
        txn_hash: &[u8],
        txn: &BlockchainTxnAddGatewayV1,
    ) -> Result<Self> {
        let gateway = Self {
            pubkey: PublicKey::try_from(txn.gateway.as_ref())?,
            owner: PublicKey::try_from(txn.owner.as_ref())?,
            payer: PublicKey::try_from(txn.payer.as_ref())?,
            height: height as i64,
            txn_hash: txn_hash.to_vec(),
            timestamp: DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(timestamp as i64, 0),
                Utc,
            ),
        };
        Ok(gateway)
    }

    pub async fn insert_into<'e, 'c, E>(&self, executor: E) -> Result<PublicKey>
    where
        E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into gateway (pubkey, owner, payer, height, txn_hash, timestamp)
        values ($1, $2, $3, $4, $5, $6)
        returning pubkey
        on conflict do nothing
            "#,
        )
        .bind(&self.pubkey)
        .bind(&self.owner)
        .bind(&self.payer)
        .bind(&self.height)
        .bind(&self.txn_hash)
        .bind(self.timestamp)
        .fetch_one(executor)
        .await
        .and_then(|row| row.try_get("pubkey"))
        .map_err(Error::from)
    }

    pub async fn get(conn: &mut PgConnection, pubkey: &PublicKey) -> Result<Option<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from gateway 
            where pubkey = $1
            "#,
        )
        .bind(pubkey)
        .fetch_optional(conn)
        .await
        .map_err(Error::from)
    }

    pub async fn list(conn: &mut PgConnection, after: &After) -> Result<Vec<Self>> {
        sqlx::query_as::<_, Self>(
            r#"
            select * from gateway 
            where height >= $1 and pubkey > $2
            order by height asc, pubkey asc
            limit $3
            "#,
        )
        .bind(after.height.unwrap_or(0))
        .bind(
            after
                .pubkey
                .as_ref()
                .map_or_else(|| "".to_string(), |pubkey| pubkey.to_string()),
        )
        .bind(min(
            MAX_GATEWAY_COUNT as i32,
            after.count.unwrap_or(DEFAULT_GATEWAY_COUNT) as i32,
        ))
        .fetch_all(conn)
        .await
        .map_err(Error::from)
    }

    pub async fn max_height<'e, 'c, E>(executor: E) -> Result<Option<i64>>
    where
        E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_scalar::<_, i64>(
            r#"
            select max(height) from gateway
            "#,
        )
        .fetch_optional(executor)
        .await
        .map_err(Error::from)
    }
}

#[derive(Deserialize)]
pub struct After {
    pub height: Option<i64>,
    pub pubkey: Option<PublicKey>,
    pub count: Option<usize>,
}
