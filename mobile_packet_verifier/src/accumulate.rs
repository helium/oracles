use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store::mobile_session::{
    DataTransferSessionIngestReport, VerifiedDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_proto::services::poc_mobile::{
    verified_data_transfer_ingest_report_v1::ReportStatus, VerifiedDataTransferIngestReportV1,
};
use sqlx::{Postgres, Transaction};

use crate::{event_ids, MobileConfigResolverExt};

pub async fn accumulate_sessions(
    mobile_config: &impl MobileConfigResolverExt,
    conn: &mut Transaction<'_, Postgres>,
    verified_data_session_report_sink: &FileSinkClient<VerifiedDataTransferIngestReportV1>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    tokio::pin!(reports);

    while let Some(report) = reports.next().await {
        let report_validity = verify_report(conn, mobile_config, &report).await?;
        write_verified_report(
            verified_data_session_report_sink,
            report_validity,
            report.clone(),
        )
        .await?;

        if report_validity != ReportStatus::Valid {
            continue;
        }

        if report.report.rewardable_bytes == 0 {
            continue;
        }

        let event = report.report.data_transfer_usage;
        sqlx::query(
            r#"
            INSERT INTO data_transfer_sessions (pub_key, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $6)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            rewardable_bytes = data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
            last_timestamp = GREATEST(data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
            "#
        )
            .bind(event.pub_key)
            .bind(event.payer)
            .bind(event.upload_bytes as i64)
            .bind(event.download_bytes as i64)
            .bind(report.report.rewardable_bytes as i64)
            .bind(curr_file_ts)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

async fn verify_report(
    txn: &mut Transaction<'_, Postgres>,
    mobile_config: &impl MobileConfigResolverExt,
    report: &DataTransferSessionIngestReport,
) -> anyhow::Result<ReportStatus> {
    if is_duplicate(txn, report).await? {
        return Ok(ReportStatus::Duplicate);
    }

    let gw_pub_key = &report.report.data_transfer_usage.pub_key;
    let routing_pub_key = &report.report.pub_key;

    if !mobile_config.is_gateway_known(gw_pub_key).await {
        return Ok(ReportStatus::InvalidGatewayKey);
    }

    if !mobile_config.is_routing_key_known(routing_pub_key).await {
        return Ok(ReportStatus::InvalidRoutingKey);
    }

    Ok(ReportStatus::Valid)
}

async fn is_duplicate(
    txn: &mut Transaction<'_, Postgres>,
    report: &DataTransferSessionIngestReport,
) -> anyhow::Result<bool> {
    event_ids::is_duplicate(
        txn,
        report.report.data_transfer_usage.event_id.clone(),
        report.received_timestamp,
    )
    .await
}

async fn write_verified_report(
    verified_data_session_report_sink: &FileSinkClient<VerifiedDataTransferIngestReportV1>,
    status: ReportStatus,
    report: DataTransferSessionIngestReport,
) -> Result<(), file_store::Error> {
    let proto: VerifiedDataTransferIngestReportV1 = VerifiedDataTransferIngestReport {
        report,
        status,
        timestamp: Utc::now(),
    }
    .into();

    verified_data_session_report_sink
        .write(proto, &[("status", status.as_str_name())])
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use file_store::{
        file_sink::FileSinkClient,
        mobile_session::{DataTransferEvent, DataTransferSessionReq},
    };
    use helium_crypto::PublicKeyBinary;
    use helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology;
    use sqlx::PgPool;

    use crate::burner::DataTransferSession;

    use super::*;

    struct MockResolver {
        gateway_known: bool,
        routing_key_known: bool,
    }

    impl MockResolver {
        fn new() -> Self {
            Self {
                gateway_known: true,
                routing_key_known: true,
            }
        }

        fn unknown_gateway(self) -> Self {
            Self {
                gateway_known: false,
                routing_key_known: self.routing_key_known,
            }
        }
        fn unknown_routing_key(self) -> Self {
            Self {
                gateway_known: self.gateway_known,
                routing_key_known: false,
            }
        }
    }

    #[async_trait::async_trait]
    impl MobileConfigResolverExt for MockResolver {
        async fn is_gateway_known(&self, _public_key: &PublicKeyBinary) -> bool {
            self.gateway_known
        }

        async fn is_routing_key_known(&self, _public_key: &PublicKeyBinary) -> bool {
            self.routing_key_known
        }
    }

    #[sqlx::test]
    async fn accumulate_no_reports(pool: PgPool) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        accumulate_sessions(
            &MockResolver::new(),
            &mut txn,
            &invalid_data_session_report_sink,
            Utc::now(),
            futures::stream::iter(vec![]),
        )
        .await?;

        txn.commit().await?;

        // channel is empty
        rx.assert_is_empty()?;

        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * from data_transfer_sessions")
                .fetch_all(&pool)
                .await?;
        assert!(sessions.is_empty());

        Ok(())
    }

    #[sqlx::test]
    async fn accumulate_writes_zero_data_event_as_verified_but_not_for_burning(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        let report = make_data_transfer_with_rewardable_bytes(0);

        accumulate_sessions(
            &MockResolver::new(),
            &mut txn,
            &invalid_data_session_report_sink,
            Utc::now(),
            futures::stream::iter(vec![report]),
        )
        .await?;

        txn.commit().await?;

        // single record written to verified sink
        rx.assert_not_empty()?;

        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * from data_transfer_sessions")
                .fetch_all(&pool)
                .await?;
        assert!(sessions.is_empty());

        Ok(())
    }

    #[sqlx::test]
    async fn write_valid_event_to_db(pool: PgPool) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        let report = make_data_transfer_with_rewardable_bytes(3);

        accumulate_sessions(
            &MockResolver::new(),
            &mut txn,
            &invalid_data_session_report_sink,
            Utc::now(),
            futures::stream::iter(vec![report]),
        )
        .await?;

        txn.commit().await?;

        // record written as verified
        rx.assert_not_empty()?;

        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * from data_transfer_sessions")
                .fetch_all(&pool)
                .await?;
        assert_eq!(sessions.len(), 1);

        Ok(())
    }

    #[sqlx::test]
    async fn unknown_gateway(pool: PgPool) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        let report = make_data_transfer_with_rewardable_bytes(3);

        accumulate_sessions(
            &MockResolver::new().unknown_gateway(),
            &mut txn,
            &invalid_data_session_report_sink,
            Utc::now(),
            futures::stream::iter(vec![report]),
        )
        .await?;

        txn.commit().await?;

        // record written as verified
        rx.assert_not_empty()?;

        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * from data_transfer_sessions")
                .fetch_all(&pool)
                .await?;
        assert!(sessions.is_empty());

        Ok(())
    }

    #[sqlx::test]
    fn unknown_routing_key(pool: PgPool) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        let report = make_data_transfer_with_rewardable_bytes(3);

        accumulate_sessions(
            &MockResolver::new().unknown_routing_key(),
            &mut txn,
            &invalid_data_session_report_sink,
            Utc::now(),
            futures::stream::iter(vec![report]),
        )
        .await?;

        txn.commit().await?;

        // record written as verified
        rx.assert_not_empty()?;

        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * from data_transfer_sessions")
                .fetch_all(&pool)
                .await?;
        assert!(sessions.is_empty());

        Ok(())
    }

    fn make_data_transfer_with_rewardable_bytes(
        rewardable_bytes: u64,
    ) -> DataTransferSessionIngestReport {
        DataTransferSessionIngestReport {
            report: DataTransferSessionReq {
                data_transfer_usage: DataTransferEvent {
                    pub_key: vec![0].into(),
                    upload_bytes: 1,
                    download_bytes: 2,
                    radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                    event_id: "test".to_string(),
                    payer: vec![0].into(),
                    timestamp: Utc::now(),
                    signature: vec![],
                },
                rewardable_bytes,
                pub_key: vec![0].into(),
                signature: vec![],
            },
            received_timestamp: Utc::now(),
        }
    }

    trait ChannelExt {
        fn assert_not_empty(&mut self) -> anyhow::Result<()>;
        fn assert_is_empty(&mut self) -> anyhow::Result<()>;
    }

    impl<T: std::fmt::Debug> ChannelExt for tokio::sync::mpsc::Receiver<T> {
        fn assert_not_empty(&mut self) -> anyhow::Result<()> {
            match self.try_recv() {
                Ok(_) => (),
                other => panic!("expected message in channel: {other:?}"),
            }
            Ok(())
        }

        fn assert_is_empty(&mut self) -> anyhow::Result<()> {
            match self.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => (),
                other => panic!("expected channel to be empty: {other:?}"),
            }
            Ok(())
        }
    }
}
