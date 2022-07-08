use crate::{
    api::{api_error, DatabaseConnection},
    datetime_from_epoch, Error, PublicKey, Result,
};
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use helium_proto::BlockchainTxnAddGatewayV1;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgConnection;
use std::cmp::min;

pub async fn get_gateway(
    Path(pubkey): Path<PublicKey>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let event = Gateway::get(&mut conn, &pubkey).await.map_err(api_error)?;
    if let Some(event) = event {
        let json = serde_json::to_value(event).map_err(api_error)?;
        Ok(Json(json))
    } else {
        Err(Error::not_found(format!("Gateway {pubkey} not found")).into())
    }
}

pub async fn get_gateways(
    Query(after): Query<After>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let gateways = Gateway::list(&mut conn, &after).await.map_err(api_error)?;
    let json = serde_json::to_value(gateways).map_err(api_error)?;
    Ok(Json(json))
}

pub const DEFAULT_GATEWAY_COUNT: usize = 100;
pub const MAX_GATEWAY_COUNT: u32 = 1000;

#[derive(sqlx::FromRow, Deserialize, Serialize)]
pub struct Gateway {
    pub pubkey: PublicKey,
    pub owner: PublicKey,
    pub payer: PublicKey,
    pub height: i64,
    pub txn_hash: Vec<u8>,
    pub block_timestamp: DateTime<Utc>,

    last_heartbeat: Option<DateTime<Utc>>,
    last_speedtest: Option<DateTime<Utc>>,
    last_attach: Option<DateTime<Utc>>,
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
            block_timestamp: datetime_from_epoch(timestamp as i64),

            last_heartbeat: None,
            last_speedtest: None,
            last_attach: None,
        };
        Ok(gateway)
    }

    pub async fn insert_into<'e, 'c, E>(&self, executor: E) -> Result
    where
        E: 'e + sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into gateway (
            pubkey, 
            owner, 
            payer, 
            height, 
            txn_hash, 
            block_timestamp, 
            last_heartbeat, 
            last_speedtest, 
            last_attach
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        on conflict do nothing
            "#,
        )
        .bind(&self.pubkey)
        .bind(&self.owner)
        .bind(&self.payer)
        .bind(&self.height)
        .bind(&self.txn_hash)
        .bind(self.block_timestamp)
        .bind(self.last_attach)
        .bind(self.last_heartbeat)
        .bind(self.last_attach)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    const UPDATE_LAST_HEARTBEAT: &'static str = r#"
        update gateway set
            last_heartbeat = $2
        where pubkey = $1
        "#;
    const UPDATE_LAST_SPEEDTEST: &'static str = r#"
        update gateway set
            last_speedtest = $2
        where pubkey = $1
        "#;
    const UPDATE_LAST_ATTACH: &'static str = r#"
        update gateway set
            last_attach = $2
        where pubkey = $1
        "#;

    async fn _update_last_timestamp<'c, 'q, E>(
        executor: E,
        query: &'q str,
        pubkey: &'q PublicKey,
        timestamp: &'q DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let rows_affected = sqlx::query(query)
            .bind(pubkey)
            .bind(timestamp)
            .execute(executor)
            .await
            .map(|res| res.rows_affected())
            .map_err(Error::from)?;
        if rows_affected == 0 {
            tracing::warn!("ignoring timestamp update for absent gateway: {pubkey}");
            Err(Error::not_found(format!("gateway {pubkey} not found")))
        } else {
            Ok(())
        }
    }

    pub async fn update_last_heartbeat<'c, 'q, E>(
        executor: E,
        pubkey: &'q PublicKey,
        timestamp: &'q DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Self::_update_last_timestamp(executor, Self::UPDATE_LAST_HEARTBEAT, pubkey, timestamp).await
    }

    pub async fn update_last_speedtest<'c, 'q, E>(
        executor: E,
        pubkey: &'q PublicKey,
        timestamp: &'q DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Self::_update_last_timestamp(executor, Self::UPDATE_LAST_SPEEDTEST, pubkey, timestamp).await
    }

    pub async fn update_last_attach<'c, E>(
        executor: E,
        pubkey: &'static PublicKey,
        timestamp: &'static DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Self::_update_last_timestamp(executor, Self::UPDATE_LAST_ATTACH, pubkey, timestamp).await
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

    pub async fn max_height<'c, E>(executor: E, default: i64) -> Result<i64>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_scalar(
            r#"
            select coalesce(max(height), $1) from gateway
            "#,
        )
        .bind(default)
        .fetch_one(executor)
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
