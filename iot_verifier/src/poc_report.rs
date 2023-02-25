use crate::entropy::ENTROPY_LIFESPAN;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// the max number of attempts a failed beacon report will be retried
const BEACON_MAX_RETRY_ATTEMPTS: i16 = 5; //TODO: determine a sane value here
/// the max number of attempts a failed witness report will be retried
const WITNESS_MAX_RETRY_ATTEMPTS: i16 = 5; //TODO: determine a sane value here

const REPORT_INSERT_SQL: &str = "insert into poc_report (
    id,
    remote_entropy,
    packet_data,
    report_data,
    report_timestamp,
    report_type,
    status
) ";

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "reporttype", rename_all = "lowercase")]
pub enum ReportType {
    Witness,
    Beacon,
}

#[derive(sqlx::Type, Serialize, Deserialize, Debug)]
#[sqlx(type_name = "iotstatus", rename_all = "lowercase")]
pub enum IotStatus {
    Pending,
    Ready,
    Valid,
    Invalid,
}

pub struct InsertBindings {
    pub id: Vec<u8>,
    pub remote_entropy: Vec<u8>,
    pub packet_data: Vec<u8>,
    pub buf: Vec<u8>,
    pub received_ts: DateTime<Utc>,
    pub report_type: ReportType,
    pub status: IotStatus,
}

#[derive(sqlx::FromRow, Deserialize, Serialize, Debug)]
#[sqlx(type_name = "poc_report")]
pub struct Report {
    pub id: Vec<u8>,
    pub remote_entropy: Vec<u8>,
    pub packet_data: Vec<u8>,
    pub report_data: Vec<u8>,
    pub report_type: ReportType,
    pub status: IotStatus,
    pub attempts: i32,
    pub report_timestamp: Option<DateTime<Utc>>,
    pub last_processed: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    #[sqlx(default)] // required to facilitate queries that don't include entropy
    pub timestamp: Option<DateTime<Utc>>,
    #[sqlx(default)]
    pub version: Option<i32>,
}

#[derive(thiserror::Error, Debug)]
#[error("report error: {0}")]
pub struct ReportError(#[from] sqlx::Error);

impl Report {
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_into<'c, E>(
        executor: E,
        id: Vec<u8>,
        remote_entropy: Vec<u8>,
        packet_data: Vec<u8>,
        report_data: Vec<u8>,
        report_timestamp: &DateTime<Utc>,
        report_type: ReportType,
        status: IotStatus,
    ) -> Result<(), ReportError>
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
            report_type,
            status
        ) values ($1, $2, $3, $4, $5, $6, $7)
        on conflict (id) do nothing
            "#,
        )
        .bind(id)
        .bind(remote_entropy)
        .bind(packet_data)
        .bind(report_data)
        .bind(report_timestamp)
        .bind(report_type)
        .bind(status)
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn bulk_insert<'c, E>(
        executor: E,
        bindings: Vec<InsertBindings>,
    ) -> Result<(), ReportError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(REPORT_INSERT_SQL);
        query_builder.push_values(bindings, |mut b, insert| {
            b.push_bind(insert.id)
                .push_bind(insert.remote_entropy)
                .push_bind(insert.packet_data)
                .push_bind(insert.buf)
                .push_bind(insert.received_ts)
                .push_bind(insert.report_type)
                .push_bind(insert.status);
        });
        // append conflict strategy to each insert row
        query_builder.push(" on conflict (id) do nothing ");
        let query = query_builder.build();
        query
            .execute(executor)
            .await
            .map(|_| ())
            .map_err(ReportError::from)
    }

    pub async fn delete_poc<'c, 'q, E>(
        executor: E,
        packet_data: &'q Vec<u8>,
    ) -> Result<(), ReportError>
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
        .await?;
        Ok(())
    }

    pub async fn delete_poc_witnesses<'c, 'q, E>(
        executor: E,
        packet_data: &'q Vec<u8>,
    ) -> Result<(), ReportError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres> + Clone,
    {
        sqlx::query(
            r#"
            delete from poc_report
            where packet_data = $1
            and report_type = "witness"
            "#,
        )
        .bind(packet_data)
        .execute(executor.clone())
        .await?;
        Ok(())
    }

    pub async fn delete_report(
        executor: impl sqlx::PgExecutor<'_>,
        id: &Vec<u8>,
    ) -> Result<(), ReportError> {
        sqlx::query(
            r#"
            delete from poc_report
            where id = $1
            "#,
        )
        .bind(id)
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn get_next_beacons<'c, E>(executor: E) -> Result<Vec<Self>, ReportError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let entropy_min_time = Utc::now() - Duration::seconds(ENTROPY_LIFESPAN);
        Ok(sqlx::query_as::<_, Self>(
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
                entropy.timestamp,
                entropy.version
            from poc_report
            inner join entropy on poc_report.remote_entropy=entropy.data
            where poc_report.report_type = 'beacon' and status = 'ready'
            and entropy.timestamp < $1
            and poc_report.attempts < $2
            order by poc_report.created_at asc
            limit 25000
            "#,
        )
        .bind(entropy_min_time)
        .bind(BEACON_MAX_RETRY_ATTEMPTS)
        .fetch_all(executor)
        .await?)
    }

    pub async fn get_pending_beacon_packet_data<'c, E>(
        executor: E,
    ) -> Result<Vec<Self>, ReportError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where report_type = 'beacon' and status = 'pending'
            "#,
        )
        .fetch_all(executor)
        .await?)
    }

    pub async fn pending_beacons_to_ready<'c, E>(
        executor: E,
        timestamp: DateTime<Utc>,
    ) -> Result<(), ReportError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        sqlx::query(
            r#"
            update poc_report set
                status = 'ready',
                last_processed = $1
            where report_type = 'beacon' and status = 'pending';
            "#,
        )
        .bind(timestamp)
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn get_witnesses_for_beacon<'c, E>(
        executor: E,
        packet_data: &Vec<u8>,
    ) -> Result<Vec<Self>, ReportError>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        Ok(sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where packet_data = $1
            and report_type = 'witness'
            and attempts < $2
            order by report_timestamp asc
            "#,
        )
        .bind(packet_data)
        .bind(WITNESS_MAX_RETRY_ATTEMPTS)
        .fetch_all(executor)
        .await?)
    }

    pub async fn update_status<'c, E>(
        executor: E,
        id: &Vec<u8>,
        status: IotStatus,
        timestamp: DateTime<Utc>,
    ) -> Result<(), ReportError>
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
        .await?;
        Ok(())
    }

    pub async fn update_status_all<'c, E>(
        executor: E,
        packet_data: &Vec<u8>,
        status: IotStatus,
        timestamp: DateTime<Utc>,
    ) -> Result<(), ReportError>
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
        .await?;
        Ok(())
    }

    pub async fn update_attempts<'c, E>(
        executor: E,
        id: &Vec<u8>,
        timestamp: DateTime<Utc>,
    ) -> Result<(), ReportError>
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
        .await?;
        Ok(())
    }

    pub async fn get_stale_beacons<'c, E>(
        executor: E,
        stale_period: i64,
    ) -> Result<Vec<Self>, ReportError>
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
        let stale_time = Utc::now() - Duration::seconds(stale_period);
        Ok(sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where report_type = 'beacon' and status = 'ready'
            and created_at < $1
            "#,
        )
        .bind(stale_time)
        .fetch_all(executor)
        .await?)
    }

    pub async fn count_all_beacons(
        executor: impl sqlx::PgExecutor<'_>,
    ) -> Result<u64, ReportError> {
        Ok(sqlx::query_scalar::<_, i64>(
            r#"
            select count(*) from poc_report
            where report_type = 'beacon' and status in ('pending','ready')
            "#,
        )
        .fetch_one(executor)
        .await
        .map(|count| count as u64)?)
    }

    pub async fn get_stale_witnesses<'c, E>(
        executor: E,
        stale_period: i64,
    ) -> Result<Vec<Self>, ReportError>
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
        let stale_time = Utc::now() - Duration::seconds(stale_period);
        Ok(sqlx::query_as::<_, Self>(
            r#"
            select * from poc_report
            where report_type = 'witness' and status = 'ready'
            and created_at < $1
            "#,
        )
        .bind(stale_time)
        .fetch_all(executor)
        .await?)
    }
}
