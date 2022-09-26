use crate::{Error, Result};
use chrono::{DateTime, Utc};
use file_store::datetime_from_epoch;
use helium_crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::cmp::min;

pub const DEFAULT_GATEWAY_COUNT: usize = 100;
pub const MAX_GATEWAY_COUNT: u32 = 1000;

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
pub struct Gateway {
    pub address: PublicKey,

    pub last_heartbeat: Option<DateTime<Utc>>,
    pub last_speedtest: Option<DateTime<Utc>>,
    pub last_attach: Option<DateTime<Utc>>,

    #[serde(skip_deserializing)]
    pub created_at: Option<DateTime<Utc>>,
}

impl Gateway {
    pub fn with_address(address: PublicKey) -> Self {
        Self {
            address,
            last_heartbeat: None,
            last_speedtest: None,
            last_attach: None,
            created_at: None,
        }
    }
}

enum TimestampField {
    Heartbeat,
    SpeedTest,
    Attach,
}

impl TimestampField {
    const UPDATE_LAST_HEARTBEAT: &'static str = r#"
        update gateway set
            last_heartbeat = greatest(last_heartbeat, $2)
        where address = $1
        "#;
    const UPDATE_LAST_SPEEDTEST: &'static str = r#"
        update gateway set
            last_speedtest = greatest(last_speedtest, $2)
        where address = $1
        "#;
    const UPDATE_LAST_ATTACH: &'static str = r#"
        update gateway set
            last_attach = greatest(last_attach, $2)
        where address = $1
        "#;

    fn update_query(&self) -> &'static str {
        match self {
            Self::Heartbeat => Self::UPDATE_LAST_HEARTBEAT,
            Self::SpeedTest => Self::UPDATE_LAST_SPEEDTEST,
            Self::Attach => Self::UPDATE_LAST_ATTACH,
        }
    }

    fn update_gateway(&self, gw: &mut Gateway, timestamp: DateTime<Utc>) {
        match self {
            Self::Heartbeat => gw.last_heartbeat = Some(timestamp),
            Self::SpeedTest => gw.last_speedtest = Some(timestamp),
            Self::Attach => gw.last_attach = Some(timestamp),
        }
    }
}

impl Gateway {
    pub async fn insert_into<'c, E>(&self, executor: E) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into gateway (
            address, 
            last_heartbeat, 
            last_speedtest, 
            last_attach
        ) values ($1, $2, $3, $4)
        on conflict (address) do nothing
            "#,
        )
        .bind(&self.address)
        .bind(self.last_heartbeat)
        .bind(self.last_speedtest)
        .bind(self.last_attach)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    async fn _update_last_timestamp<'c, 'q, E>(
        executor: E,
        field: TimestampField,
        address: &'q PublicKey,
        timestamp: &'q DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        let rows_affected = sqlx::query(field.update_query())
            .bind(address)
            .bind(timestamp)
            .execute(executor.clone())
            .await
            .map(|res| res.rows_affected())
            .map_err(Error::from)?;
        if rows_affected == 0 {
            let mut gw = Gateway::with_address(address.to_owned());
            field.update_gateway(&mut gw, *timestamp);
            gw.insert_into(executor).await
        } else {
            Ok(())
        }
    }

    pub async fn update_last_heartbeat<'c, 'q, E>(
        executor: E,
        address: &'q PublicKey,
        timestamp: &'q DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        Self::_update_last_timestamp(executor, TimestampField::Heartbeat, address, timestamp).await
    }

    pub async fn update_last_speedtest<'c, 'q, E>(
        executor: E,
        address: &'q PublicKey,
        timestamp: &'q DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        Self::_update_last_timestamp(executor, TimestampField::SpeedTest, address, timestamp).await
    }

    pub async fn update_last_attach<'c, E>(
        executor: E,
        address: &'static PublicKey,
        timestamp: &'static DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        Self::_update_last_timestamp(executor, TimestampField::Attach, address, timestamp).await
    }

    pub async fn get<'c, E>(executor: E, address: &PublicKey) -> Result<Option<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#"
            select * from gateway 
            where address = $1
            "#,
        )
        .bind(address)
        .fetch_optional(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn list<'c, E>(executor: E, after: &After) -> Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#"
            select * from gateway 
            where created_at > $1
            order by created_at asc
            limit $3
            "#,
        )
        .bind(after.created_at.unwrap_or_else(|| datetime_from_epoch(0)))
        .bind(min(
            MAX_GATEWAY_COUNT as i32,
            after.count.unwrap_or(DEFAULT_GATEWAY_COUNT) as i32,
        ))
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }
}

#[derive(Deserialize)]
pub struct After {
    pub created_at: Option<DateTime<Utc>>,
    pub count: Option<usize>,
}
