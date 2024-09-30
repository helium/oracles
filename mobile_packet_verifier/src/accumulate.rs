use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store::mobile_session::{
    DataTransferSessionIngestReport, InvalidDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_proto::services::poc_mobile::{
    invalid_data_transfer_ingest_report_v1::DataTransferIngestReportStatus,
    InvalidDataTransferIngestReportV1,
};
use sqlx::{Postgres, Transaction};

use crate::{event_ids, MobileConfigResolverExt};

pub async fn accumulate_sessions(
    mobile_config: &impl MobileConfigResolverExt,
    conn: &mut Transaction<'_, Postgres>,
    invalid_data_session_report_sink: &FileSinkClient<InvalidDataTransferIngestReportV1>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    tokio::pin!(reports);

    while let Some(report) = reports.next().await {
        let report_validity = verify_report(conn, mobile_config, &report).await?;
        if report_validity != DataTransferIngestReportStatus::Valid {
            // If the reward has been cancelled or it fails verification checks then skip
            // the report and write it out to s3 as invalid
            write_invalid_report(invalid_data_session_report_sink, report_validity, report).await?;
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
) -> anyhow::Result<DataTransferIngestReportStatus> {
    if report.report.rewardable_bytes == 0 {
        return Ok(DataTransferIngestReportStatus::Cancelled);
    }

    if is_duplicate(txn, report).await? {
        return Ok(DataTransferIngestReportStatus::Duplicate);
    }

    let gw_pub_key = &report.report.data_transfer_usage.pub_key;
    let routing_pub_key = &report.report.pub_key;

    if !mobile_config.is_gateway_known(gw_pub_key).await {
        return Ok(DataTransferIngestReportStatus::InvalidGatewayKey);
    }

    if !mobile_config.is_routing_key_known(routing_pub_key).await {
        return Ok(DataTransferIngestReportStatus::InvalidRoutingKey);
    }

    Ok(DataTransferIngestReportStatus::Valid)
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

async fn write_invalid_report(
    invalid_data_session_report_sink: &FileSinkClient<InvalidDataTransferIngestReportV1>,
    reason: DataTransferIngestReportStatus,
    report: DataTransferSessionIngestReport,
) -> Result<(), file_store::Error> {
    let proto: InvalidDataTransferIngestReportV1 = InvalidDataTransferIngestReport {
        report,
        reason,
        timestamp: Utc::now(),
    }
    .into();

    invalid_data_session_report_sink
        .write(proto, &[("reason", reason.as_str_name())])
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

    struct MockResolver;

    #[async_trait::async_trait]
    impl MobileConfigResolverExt for MockResolver {
        async fn is_gateway_known(&self, _public_key: &PublicKeyBinary) -> bool {
            true
        }

        async fn is_routing_key_known(&self, _public_key: &PublicKeyBinary) -> bool {
            true
        }
    }

    #[sqlx::test]
    async fn accumulate_no_reports(pool: PgPool) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        accumulate_sessions(
            &MockResolver,
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
    async fn accumulate_writes_zero_data_event_as_invalid(pool: PgPool) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        let report = DataTransferSessionIngestReport {
            report: DataTransferSessionReq {
                data_transfer_usage: DataTransferEvent {
                    pub_key: vec![0].into(),
                    upload_bytes: 0,
                    download_bytes: 0,
                    radio_access_technology: DataTransferRadioAccessTechnology::Wlan,
                    event_id: "test".to_string(),
                    payer: vec![0].into(),
                    timestamp: Utc::now(),
                    signature: vec![],
                },
                rewardable_bytes: 0,
                pub_key: vec![0].into(),
                signature: vec![],
            },
            received_timestamp: Utc::now(),
        };

        accumulate_sessions(
            &MockResolver,
            &mut txn,
            &invalid_data_session_report_sink,
            Utc::now(),
            futures::stream::iter(vec![report]),
        )
        .await?;

        txn.commit().await?;

        // single record written to invalid sink
        match rx.try_recv() {
            Ok(_) => (),
            other => panic!("unexpected: {other:?}"),
        }

        Ok(())
    }

    #[sqlx::test]
    async fn write_valid_event_to_db(pool: PgPool) -> anyhow::Result<()> {
        let mut txn = pool.begin().await?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let invalid_data_session_report_sink = FileSinkClient::new(tx, "test");

        let report = DataTransferSessionIngestReport {
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
                rewardable_bytes: 3,
                pub_key: vec![0].into(),
                signature: vec![],
            },
            received_timestamp: Utc::now(),
        };

        accumulate_sessions(
            &MockResolver,
            &mut txn,
            &invalid_data_session_report_sink,
            Utc::now(),
            futures::stream::iter(vec![report]),
        )
        .await?;

        txn.commit().await?;

        // no records written to invalid sink
        rx.assert_is_empty()?;

        let sessions: Vec<DataTransferSession> =
            sqlx::query_as("SELECT * from data_transfer_sessions")
                .fetch_all(&pool)
                .await?;
        assert_eq!(sessions.len(), 1);

        Ok(())
    }

    trait ChannelExt {
        fn assert_is_empty(&mut self) -> anyhow::Result<()>;
    }

    impl<T: std::fmt::Debug> ChannelExt for tokio::sync::mpsc::Receiver<T> {
        fn assert_is_empty(&mut self) -> anyhow::Result<()> {
            match self.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => (),
                other => panic!("unexpected message: {other:?}"),
            }
            Ok(())
        }
    }
}
