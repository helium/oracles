use chrono::{DateTime, Utc};
use file_store::file_sink::FileSinkClient;
use file_store::mobile_session::{
    DataTransferEvent, DataTransferSessionIngestReport, InvalidDataTransferIngestReport,
};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::mobile_config::NetworkKeyRole;
use helium_proto::services::poc_mobile::DataTransferSessionSettlementStatus;
use helium_proto::services::poc_mobile::{
    invalid_data_transfer_ingest_report_v1::DataTransferIngestReportStatus,
    InvalidDataTransferIngestReportV1,
};
use mobile_config::client::{
    authorization_client::AuthorizationVerifier, gateway_client::GatewayInfoResolver,
};
use sqlx::{Postgres, Transaction};

use crate::event_ids;

pub async fn accumulate_sessions(
    gateway_info_resolver: &impl GatewayInfoResolver,
    authorization_verifier: &impl AuthorizationVerifier,
    conn: &mut Transaction<'_, Postgres>,
    invalid_data_session_report_sink: &FileSinkClient<InvalidDataTransferIngestReportV1>,
    curr_file_ts: DateTime<Utc>,
    reports: impl Stream<Item = DataTransferSessionIngestReport>,
) -> anyhow::Result<()> {
    tokio::pin!(reports);

    while let Some(report) = reports.next().await {
        // If the reward has been cancelled or it fails verification checks then skip
        // the report and write it out to s3 as invalid
        if report.report.rewardable_bytes == 0 {
            write_invalid_report(
                invalid_data_session_report_sink,
                DataTransferIngestReportStatus::Cancelled,
                report,
            )
            .await?;
            continue;
        }

        let report_validity =
            verify_report(conn, gateway_info_resolver, authorization_verifier, &report).await?;
        if report_validity != DataTransferIngestReportStatus::Valid {
            write_invalid_report(invalid_data_session_report_sink, report_validity, report).await?;
            continue;
        }

        let status = report.report.status;
        match status {
            DataTransferSessionSettlementStatus::Settled => {
                save_settled_data_transfer_session(
                    conn,
                    report.report.data_transfer_usage,
                    report.report.rewardable_bytes,
                    curr_file_ts,
                )
                .await?;
            }
            DataTransferSessionSettlementStatus::Pending => {
                save_pending_data_transfer_session(
                    conn,
                    report.report.data_transfer_usage,
                    report.report.rewardable_bytes,
                    curr_file_ts,
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn save_pending_data_transfer_session(
    conn: &mut Transaction<'_, Postgres>,
    data_transfer_event: DataTransferEvent,
    rewardable_bytes: u64,
    curr_file_ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
            INSERT INTO pending_data_transfer_sessions (pub_key, event_id, payer, uploaded_bytes, downloaded_bytes, rewardable_bytes, first_timestamp, last_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
            ON CONFLICT (pub_key, payer) DO UPDATE SET
            uploaded_bytes = pending_data_transfer_sessions.uploaded_bytes + EXCLUDED.uploaded_bytes,
            downloaded_bytes = pending_data_transfer_sessions.downloaded_bytes + EXCLUDED.downloaded_bytes,
            rewardable_bytes = pending_data_transfer_sessions.rewardable_bytes + EXCLUDED.rewardable_bytes,
            last_timestamp = GREATEST(pending_data_transfer_sessions.last_timestamp, EXCLUDED.last_timestamp)
        "#,
    )
    .bind(data_transfer_event.pub_key)
    .bind(data_transfer_event.event_id)
    .bind(data_transfer_event.payer)
    .bind(data_transfer_event.upload_bytes as i64)
    .bind(data_transfer_event.download_bytes as i64)
    .bind(rewardable_bytes as i64)
    .bind(curr_file_ts)
    .execute(conn)
    .await?;
    Ok(())
}

async fn save_settled_data_transfer_session(
    conn: &mut Transaction<'_, Postgres>,
    data_transfer_event: DataTransferEvent,
    rewardable_bytes: u64,
    curr_file_ts: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
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
        .bind(data_transfer_event.pub_key)
        .bind(data_transfer_event.payer)
        .bind(data_transfer_event.upload_bytes as i64)
        .bind(data_transfer_event.download_bytes as i64)
        .bind(rewardable_bytes as i64)
        .bind(curr_file_ts)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

async fn verify_report(
    txn: &mut Transaction<'_, Postgres>,
    gateway_info_resolver: &impl GatewayInfoResolver,
    authorization_verifier: &impl AuthorizationVerifier,
    report: &DataTransferSessionIngestReport,
) -> anyhow::Result<DataTransferIngestReportStatus> {
    if is_duplicate(txn, report).await? {
        return Ok(DataTransferIngestReportStatus::Duplicate);
    }

    if !verify_gateway(
        gateway_info_resolver,
        &report.report.data_transfer_usage.pub_key,
    )
    .await
    {
        return Ok(DataTransferIngestReportStatus::InvalidGatewayKey);
    };
    if !verify_known_routing_key(authorization_verifier, &report.report.pub_key).await {
        return Ok(DataTransferIngestReportStatus::InvalidRoutingKey);
    };
    Ok(DataTransferIngestReportStatus::Valid)
}

async fn verify_gateway(
    gateway_info_resolver: &impl GatewayInfoResolver,
    public_key: &PublicKeyBinary,
) -> bool {
    match gateway_info_resolver.resolve_gateway_info(public_key).await {
        Ok(res) => res.is_some(),
        Err(_err) => false,
    }
}

async fn verify_known_routing_key(
    authorization_verifier: &impl AuthorizationVerifier,
    public_key: &PublicKeyBinary,
) -> bool {
    match authorization_verifier
        .verify_authorized_key(public_key, NetworkKeyRole::MobileRouter)
        .await
    {
        Ok(res) => res,
        Err(_err) => false,
    }
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

    use file_store::mobile_session::DataTransferSessionReq;
    use helium_crypto::{KeyTag, Keypair};
    use mobile_config::gateway_info::{DeviceType, GatewayInfo, GatewayInfoStream};
    use sqlx::PgPool;

    use super::*;

    #[derive(thiserror::Error, Debug)]
    enum MockError {}

    #[derive(Clone)]
    struct MockGatewayInfoResolver;

    #[async_trait::async_trait]
    impl GatewayInfoResolver for MockGatewayInfoResolver {
        type Error = MockError;

        async fn resolve_gateway_info(
            &self,
            address: &PublicKeyBinary,
        ) -> Result<Option<GatewayInfo>, Self::Error> {
            Ok(Some(GatewayInfo {
                address: address.clone(),
                metadata: None,
                device_type: DeviceType::Cbrs,
            }))
        }

        async fn stream_gateways_info(&mut self) -> Result<GatewayInfoStream, Self::Error> {
            todo!()
        }
    }

    struct AllVerified;

    #[async_trait::async_trait]
    impl AuthorizationVerifier for AllVerified {
        type Error = MockError;

        async fn verify_authorized_key(
            &self,
            _pubkey: &PublicKeyBinary,
            _role: helium_proto::services::mobile_config::NetworkKeyRole,
        ) -> Result<bool, Self::Error> {
            Ok(true)
        }
    }

    #[sqlx::test]
    async fn pending_data_transfer_sessions_not_stored_for_burning(
        pool: PgPool,
    ) -> anyhow::Result<()> {
        use rand::rngs::OsRng;
        let radio_pubkey = Keypair::generate(KeyTag::default(), &mut OsRng);
        let signing_pubkey = Keypair::generate(KeyTag::default(), &mut OsRng);
        let payer_pubkey = Keypair::generate(KeyTag::default(), &mut OsRng);

        let reports = futures::stream::iter(vec![DataTransferSessionIngestReport {
            received_timestamp: Utc::now(),
            report: DataTransferSessionReq {
                data_transfer_usage: DataTransferEvent {
                    pub_key: radio_pubkey.public_key().to_owned().into(),
                    upload_bytes: 50,
                    download_bytes: 50,
                    radio_access_technology: helium_proto::services::poc_mobile::DataTransferRadioAccessTechnology::default(),
                    event_id: "event-id".to_string(),
                    payer: payer_pubkey.public_key().to_owned().into(),
                    timestamp: Utc::now(),
                    signature: vec![]
                },
                rewardable_bytes: 100,
                pub_key: signing_pubkey.public_key().to_owned().into(),
                signature: vec![],
                status: DataTransferSessionSettlementStatus::Pending,
            },
        }]);

        let (invalid_report_tx, mut invalid_report_rx) = tokio::sync::mpsc::channel(1);
        let invalid_data_session_report_sink = FileSinkClient::new(invalid_report_tx, "testing");

        let mut conn = pool.begin().await?;
        accumulate_sessions(
            &MockGatewayInfoResolver,
            &AllVerified,
            &mut conn,
            &invalid_data_session_report_sink,
            Utc::now(),
            reports,
        )
        .await?;
        conn.commit().await?;

        let pending_rows = sqlx::query("SELECT * FROM pending_data_transfer_sessions")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(pending_rows.len(), 1);

        let settled_rows = sqlx::query("SELECT * FROM data_transfer_sessions")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(settled_rows.len(), 0);

        if invalid_report_rx.try_recv().is_ok() {
            panic!("expected invalid report sink to be empty")
        }

        Ok(())
    }
}
