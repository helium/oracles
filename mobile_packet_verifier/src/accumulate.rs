use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store::mobile_session::{
    DataTransferSessionIngestReport, VerifiedDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology;
use helium_proto::services::poc_mobile::{
    verified_data_transfer_ingest_report_v1::ReportStatus, VerifiedDataTransferIngestReportV1,
};
use sqlx::{Postgres, Transaction};

use crate::{banning::BannedRadios, event_ids, pending_burns, MobileConfigResolverExt};

pub async fn accumulate_sessions(
    mobile_config: &impl MobileConfigResolverExt,
    banned_radios: BannedRadios,
    txn: &mut Transaction<'_, Postgres>,
    verified_data_session_report_sink: &FileSinkClient<VerifiedDataTransferIngestReportV1>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    tokio::pin!(reports);

    while let Some(report) = reports.next().await {
        if report.report.data_transfer_usage.radio_access_technology
        // Eutran means CBRS radio
            == DataTransferRadioAccessTechnology::Eutran
        {
            continue;
        }

        let report_validity = verify_report(txn, mobile_config, &banned_radios, &report).await?;
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

        pending_burns::save_data_transfer_session_req(&mut *txn, &report.report, curr_file_ts)
            .await?;
    }

    Ok(())
}

async fn verify_report(
    txn: &mut Transaction<'_, Postgres>,
    mobile_config: &impl MobileConfigResolverExt,
    banned_radios: &BannedRadios,
    report: &DataTransferSessionIngestReport,
) -> anyhow::Result<ReportStatus> {
    if is_duplicate(txn, report).await? {
        return Ok(ReportStatus::Duplicate);
    }

    let gw_pub_key = &report.report.data_transfer_usage.pub_key;
    let routing_pub_key = &report.report.pub_key;

    if banned_radios.contains(gw_pub_key) {
        return Ok(ReportStatus::Banned);
    }

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

    use crate::pending_burns::DataTransferSession;

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
            BannedRadios::default(),
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
            BannedRadios::default(),
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
            BannedRadios::default(),
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
            BannedRadios::default(),
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
            BannedRadios::default(),
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
