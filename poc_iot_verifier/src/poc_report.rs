use crate::{entropy::ENTROPY_LIFESPAN, Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// the period in seconds after when a beacon or witness report in the DB will be deemed stale
/// this period needs to be sufficiently long that we can be sure the beacon has
/// zero change of validating
/// NOTE: this period should permit the verifier to be down for an extended period
const REPORT_STALE_PERIOD: i32 = 60 * 60 * 8; // 8 hours in seconds;
/// the max number of attempts a failed beacon report will be retried
const BEACON_MAX_RETRY_ATTEMPTS: i16 = 5; //TODO: determine a sane value here
/// the max number of attempts a failed witness report will be retried
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
    pub remote_entropy: Vec<u8>,
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
        remote_entropy: Vec<u8>,
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
            remote_entropy,
            packet_data,
            report_data,
            report_timestamp,
            report_type
        ) values ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(id)
        .bind(remote_entropy)
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

    pub async fn delete_report<'c, 'q, E>(executor: E, id: &'q Vec<u8>) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        sqlx::query(
            r#"
            delete from poc_report
            where id = $1
            "#,
        )
        .bind(id)
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
            select  id,
                    remote_entropy,
                    packet_data,
                    report_data,
                    report_type,
                    status,
                    attempts,
                    report_timestamp,
                    last_processed,
                    created_at
            from poc_report
            left join entropy on poc_report.remote_entropy=entropy.data
            where poc_report.report_type = 'beacon' and status = 'pending'
            and entropy.timestamp < (NOW() - INTERVAL '$1 SECONDS')
            and attempts < $2
            order by report_timestamp asc
            limit 500
            "#,
        )
        .bind(ENTROPY_LIFESPAN)
        .bind(BEACON_MAX_RETRY_ATTEMPTS)
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }

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
            and report_type = 'witness' and status = 'pending'
            and attempts < $2
            order by report_timestamp asc
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

    pub async fn get_stale_pending_beacons<'c, E>(executor: E) -> Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        // NOTE: the query for stale beacons cannot rely on poc_report.attempts
        // as beacons could be deteached from any entropy report
        // ie we may receive a beacon report but never see an assocaited entropy report
        // in such cases, the beacon report will never be processed
        // as when pulling beacon reports, the verifier performs a join to entropy table
        // if the entropy is not there the beacon will never be processed
        // Such beacons will eventually be handled by the purger and failed there
        // stale beacon reports, for this reason, are determined solely based on time
        sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where poc_report.report_type = 'beacon' and status = 'pending'
            and created_at < (NOW() - INTERVAL '$1 SECONDS')
            order by created_at asc
            "#,
        )
        .bind(REPORT_STALE_PERIOD)
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn get_stale_pending_witnesses<'c, E>(executor: E) -> Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        // NOTE: the query for stale witnesses cannot rely on poc_report.attempts
        // as witnesses could be deteached from any beacon report
        // ie we may receive witnesses but not receive the associated beacon report
        // in such cases, the witness report will never be verified
        // as the verifier processes beacon reports and then pulls witness reports
        // linked to current beacon being processed
        // stale witness reports, for this reason, are determined solely based on time
        sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where report_type = 'witness' and status = 'pending'
            and created_at < (NOW() - INTERVAL '$1 SECONDS')
            order by created_at asc
            "#,
        )
        .bind(REPORT_STALE_PERIOD)
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }
}
