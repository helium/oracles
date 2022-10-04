use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const POC_LIFESPAN: i16 = 5; //minutes TODO: determine a sane value here
const BEACON_MAX_RETRY_ATTEMPTS: i16 = 5; //TODO: determine a sane value here
const WITNESS_MAX_RETRY_ATTEMPTS: i16 = 5; //TODO: determine a sane value here

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "reporttype", rename_all = "lowercase")]
pub enum ReportType {
    Witness,
    Beacon,
}

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "lorastatus", rename_all = "lowercase")]
pub enum LoraStatus {
    Pending,
    Valid,
    Invalid,
}

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "poc_report")]
pub struct Report {
    pub id: Vec<u8>,
    pub packet_data: Vec<u8>,
    pub report_data: Vec<u8>,
    pub report_type: ReportType,
    pub status: LoraStatus,
    pub attempts: i32,
    pub report_timestamp: Option<DateTime<Utc>>,
    pub last_processed: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

impl Report {
    pub async fn insert_into<'c, E>(
        executor: E,
        id: Vec<u8>,
        packet_data: Vec<u8>,
        report_data: Vec<u8>,
        report_timestamp: &DateTime<Utc>,
        report_type: ReportType,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
        insert into poc_report (
            id,
            packet_data,
            report_data,
            report_timestamp,
            report_type
        ) values ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(id)
        .bind(packet_data)
        .bind(report_data)
        .bind(report_timestamp)
        .bind(report_type)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn delete_poc<'c, 'q, E>(executor: E, packet_data: &'q Vec<u8>) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        sqlx::query(
            r#"
            delete from poc_report
            where packet_data = $1
            "#,
        )
        .bind(packet_data)
        .execute(executor.clone())
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn get_next_beacons<'c, E>(executor: E) -> Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where report_type = 'beacon' and status = 'pending'
            and report_timestamp < (NOW() - INTERVAL '$1 MINUTES')
            and attempts < $2
            order by report_timestamp asc
            limit 500
            "#,
        )
        .bind(POC_LIFESPAN)
        .bind(BEACON_MAX_RETRY_ATTEMPTS)
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }

    // TODO: maybe use a join here and include the witnesses in along with the beacons above ?
    pub async fn get_witnesses_for_beacon<'c, E>(
        executor: E,
        packet_data: &Vec<u8>,
    ) -> Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where packet_data = $1
            and report_type = 'witness'
            and attempts < $2
            order by created_at asc
            "#,
        )
        .bind(packet_data)
        .bind(WITNESS_MAX_RETRY_ATTEMPTS)
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn update_status<'c, E>(
        executor: E,
        id: &Vec<u8>,
        status: LoraStatus,
        timestamp: DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            update poc_report set
                status = $1,
                last_processed = $2
            where id = $3;
            "#,
        )
        .bind(status)
        .bind(timestamp)
        .bind(id)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn update_status_all<'c, E>(
        executor: E,
        packet_data: &Vec<u8>,
        status: LoraStatus,
        timestamp: DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            update poc_report set
                status = $1,
                last_processed = $2
            where packet_data = $3;
            "#,
        )
        .bind(status)
        .bind(timestamp)
        .bind(packet_data)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn update_attempts<'c, E>(
        executor: E,
        id: &Vec<u8>,
        timestamp: DateTime<Utc>,
    ) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            update poc_report set
                attempts = attempts + 1,
                last_processed = $1
            where id = $2;
            "#,
        )
        .bind(timestamp)
        .bind(id)
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }
}
