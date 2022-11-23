use crate::{entropy::ENTROPY_LIFESPAN, Error, Result};
use chrono::{DateTime, Utc, Duration};
use serde::{Deserialize, Serialize};

/// the max number of attempts a failed beacon report will be retried
const BEACON_MAX_RETRY_ATTEMPTS: i16 = 5; //TODO: determine a sane value here
/// the max number of attempts a failed witness report will be retried
const WITNESS_MAX_RETRY_ATTEMPTS: i16 = 5; //TODO: determine a sane value here
/// a period of time in seconds which needs to pass after the ENTROPY_LIFESPAN expires
/// before which any beacon & witness reports using that entropy will be processed
//  this is bascially a buffer period which is added to ENTROPY_LIFESPAN
//  to prevent POC reports being processed *immediately* after the entropy expires
//  instead there will be a further delay which allows for any *fashionably* late
//  witness reports to be processed as part of the overall POC
//  ( processing of POCs occur by first pulling a beacon and then any witnesses
//    for that beacon, if a witness comes in after the beacon has been processed
//    then it does not get verified as part of the overall POC but instead will
//    end up being deemed stale )
const BEACON_PROCESSING_DELAY: i64 = 60 * 32;

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
    #[sqlx(default)]
    pub timestamp: Option<DateTime<Utc>>,
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
        on conflict (id) do nothing
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
        .execute(executor)
        .await
        .map(|_| ())
        .map_err(Error::from)
    }

    pub async fn get_next_beacons<'c, E>(executor: E) -> Result<Vec<Self>>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let entropy_min_time= Utc::now() - Duration::seconds(ENTROPY_LIFESPAN);
        let report_min_time= Utc::now() - Duration::seconds(BEACON_PROCESSING_DELAY);
        sqlx::query_as::<_, Self>(
            r#"
            select poc_report.id,
                poc_report.remote_entropy,
                poc_report.packet_data,
                poc_report.report_data,
                poc_report.report_type,
                poc_report.status,
                poc_report.attempts,
                poc_report.report_timestamp,
                poc_report.last_processed,
                poc_report.created_at,
                entropy.timestamp
            from poc_report
            inner join entropy on poc_report.remote_entropy=entropy.data
            where poc_report.report_type = 'beacon' and status = 'pending'
            and entropy.timestamp < $1
            and poc_report.created_at < $2
            and poc_report.attempts < $3
            order by poc_report.created_at asc
            limit 25000
            "#,
        )
        .bind(entropy_min_time)
        .bind(report_min_time)
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

    pub async fn get_stale_pending_beacons<'c, E>(
        executor: E,
        stale_period: i64,
    ) -> Result<Vec<Self>>
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
        let stale_time= Utc::now() - Duration::seconds(stale_period);
        sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where report_type = 'beacon' and status = 'pending'
            and created_at < $1
            "#,
        )
        .bind(stale_time)
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }

    pub async fn get_stale_pending_witnesses<'c, E>(
        executor: E,
        stale_period: i64,
    ) -> Result<Vec<Self>>
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
        tracing::info!("*** purge stale period: {stale_period}");
        let stale_time= Utc::now() - Duration::seconds(stale_period);
        sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where report_type = 'witness' and status = 'pending'
            and created_at < $1
            "#,
        )
        .bind(stale_time)
        .fetch_all(executor)
        .await
        .map_err(Error::from)
    }
}
