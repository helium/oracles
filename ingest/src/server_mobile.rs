use crate::Settings;
use anyhow::{bail, Error, Result};
use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient,
    file_upload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, MsgVerify},
};
use futures::future::LocalBoxFuture;
use futures_util::TryFutureExt;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatIngestReportV1, CellHeartbeatReqV1, CellHeartbeatRespV1,
    CoverageObjectIngestReportV1, CoverageObjectReqV1, CoverageObjectRespV1,
    DataTransferSessionIngestReportV1, DataTransferSessionReqV1, DataTransferSessionRespV1,
    InvalidatedRadioThresholdIngestReportV1, InvalidatedRadioThresholdReportReqV1,
    InvalidatedRadioThresholdReportRespV1, RadioLocationEstimatesIngestReportV1,
    RadioLocationEstimatesReqV1, RadioLocationEstimatesRespV1, RadioThresholdIngestReportV1,
    RadioThresholdReportReqV1, RadioThresholdReportRespV1,
    ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    ServiceProviderBoostedRewardsBannedRadioReqV1, ServiceProviderBoostedRewardsBannedRadioRespV1,
    SpeedtestIngestReportV1, SpeedtestReqV1, SpeedtestRespV1, SubscriberLocationIngestReportV1,
    SubscriberLocationReqV1, SubscriberLocationRespV1,
    SubscriberVerifiedMappingEventIngestReportV1, SubscriberVerifiedMappingEventReqV1,
    SubscriberVerifiedMappingEventResV1, WifiHeartbeatIngestReportV1, WifiHeartbeatReqV1,
    WifiHeartbeatRespV1,
};
use std::{net::SocketAddr, path::Path};
use task_manager::{ManagedTask, TaskManager};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport, Request, Response, Status,
};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type VerifyResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    heartbeat_report_sink: FileSinkClient<CellHeartbeatIngestReportV1>,
    wifi_heartbeat_report_sink: FileSinkClient<WifiHeartbeatIngestReportV1>,
    speedtest_report_sink: FileSinkClient<SpeedtestIngestReportV1>,
    data_transfer_session_sink: FileSinkClient<DataTransferSessionIngestReportV1>,
    subscriber_location_report_sink: FileSinkClient<SubscriberLocationIngestReportV1>,
    radio_threshold_report_sink: FileSinkClient<RadioThresholdIngestReportV1>,
    invalidated_radio_threshold_report_sink:
        FileSinkClient<InvalidatedRadioThresholdIngestReportV1>,
    coverage_object_report_sink: FileSinkClient<CoverageObjectIngestReportV1>,
    sp_boosted_rewards_ban_sink:
        FileSinkClient<ServiceProviderBoostedRewardsBannedRadioIngestReportV1>,
    subscriber_mapping_event_sink: FileSinkClient<SubscriberVerifiedMappingEventIngestReportV1>,
    radio_location_estimate_sink: FileSinkClient<RadioLocationEstimatesIngestReportV1>,
    required_network: Network,
    address: SocketAddr,
    api_token: MetadataValue<Ascii>,
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

fn make_span(_request: &http::request::Request<helium_proto::services::Body>) -> tracing::Span {
    tracing::info_span!(
        custom_tracing::DEFAULT_SPAN,
        pub_key = tracing::field::Empty,
        subscriber_id = tracing::field::Empty,
    )
}

impl GrpcServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        heartbeat_report_sink: FileSinkClient<CellHeartbeatIngestReportV1>,
        wifi_heartbeat_report_sink: FileSinkClient<WifiHeartbeatIngestReportV1>,
        speedtest_report_sink: FileSinkClient<SpeedtestIngestReportV1>,
        data_transfer_session_sink: FileSinkClient<DataTransferSessionIngestReportV1>,
        subscriber_location_report_sink: FileSinkClient<SubscriberLocationIngestReportV1>,
        radio_threshold_report_sink: FileSinkClient<RadioThresholdIngestReportV1>,
        invalidated_radio_threshold_report_sink: FileSinkClient<
            InvalidatedRadioThresholdIngestReportV1,
        >,
        coverage_object_report_sink: FileSinkClient<CoverageObjectIngestReportV1>,
        sp_boosted_rewards_ban_sink: FileSinkClient<
            ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
        >,
        subscriber_mapping_event_sink: FileSinkClient<SubscriberVerifiedMappingEventIngestReportV1>,
        radio_location_estimate_sink: FileSinkClient<RadioLocationEstimatesIngestReportV1>,
        required_network: Network,
        address: SocketAddr,
        api_token: MetadataValue<Ascii>,
    ) -> Self {
        GrpcServer {
            heartbeat_report_sink,
            wifi_heartbeat_report_sink,
            speedtest_report_sink,
            data_transfer_session_sink,
            subscriber_location_report_sink,
            radio_threshold_report_sink,
            invalidated_radio_threshold_report_sink,
            coverage_object_report_sink,
            sp_boosted_rewards_ban_sink,
            subscriber_mapping_event_sink,
            radio_location_estimate_sink,
            required_network,
            address,
            api_token,
        }
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        let api_token = self.api_token.clone();
        let address = self.address;

        transport::Server::builder()
            .layer(custom_tracing::grpc_layer::new_with_span(make_span))
            .layer(poc_metrics::request_layer!("ingest_server_grpc_connection"))
            .add_service(poc_mobile::Server::with_interceptor(
                self,
                move |req: Request<()>| match req.metadata().get("authorization") {
                    Some(t) if api_token == t => Ok(req),
                    _ => Err(Status::unauthenticated("No valid auth token")),
                },
            ))
            .serve_with_shutdown(address, shutdown)
            .map_err(Error::from)
            .await
    }

    fn verify_network(&self, public_key: PublicKey) -> VerifyResult<PublicKey> {
        if self.required_network == public_key.network {
            Ok(public_key)
        } else {
            Err(Status::invalid_argument("invalid network"))
        }
    }

    fn verify_public_key(&self, bytes: &[u8]) -> VerifyResult<PublicKey> {
        PublicKey::try_from(bytes).map_err(|_| Status::invalid_argument("invalid public key"))
    }

    fn verify_signature<E>(&self, public_key: PublicKey, event: E) -> VerifyResult<(PublicKey, E)>
    where
        E: MsgVerify,
    {
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        Ok((public_key, event))
    }
}

#[tonic::async_trait]
impl poc_mobile::PocMobile for GrpcServer {
    async fn submit_speedtest(
        &self,
        request: Request<SpeedtestReqV1>,
    ) -> GrpcResult<SpeedtestRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        custom_tracing::record_b58("pub_key", &event.pub_key);

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| SpeedtestIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.speedtest_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(SpeedtestRespV1 { id }))
    }

    async fn submit_cell_heartbeat(
        &self,
        request: Request<CellHeartbeatReqV1>,
    ) -> GrpcResult<CellHeartbeatRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        custom_tracing::record_b58("pub_key", &event.pub_key);

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| CellHeartbeatIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.heartbeat_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(CellHeartbeatRespV1 { id }))
    }

    async fn submit_wifi_heartbeat(
        &self,
        request: Request<WifiHeartbeatReqV1>,
    ) -> GrpcResult<WifiHeartbeatRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        custom_tracing::record_b58("pub_key", &event.pub_key);

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| WifiHeartbeatIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.wifi_heartbeat_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(WifiHeartbeatRespV1 { id }))
    }

    async fn submit_data_transfer_session(
        &self,
        request: Request<DataTransferSessionReqV1>,
    ) -> GrpcResult<DataTransferSessionRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        custom_tracing::record_b58("pub_key", &event.pub_key);

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| DataTransferSessionIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.data_transfer_session_sink.write(report, []).await;

        Ok(Response::new(DataTransferSessionRespV1 {
            id: timestamp.to_string(),
        }))
    }

    async fn submit_subscriber_location(
        &self,
        request: Request<SubscriberLocationReqV1>,
    ) -> GrpcResult<SubscriberLocationRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let subscriber_id = event.subscriber_id.clone();
        let timestamp_millis = event.timestamp;

        custom_tracing::record(
            "pub_key",
            bs58::encode(&event.carrier_pub_key).into_string(),
        );

        custom_tracing::record("subscriber_id", bs58::encode(&subscriber_id).into_string());

        let report = self
            .verify_public_key(event.carrier_pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| SubscriberLocationIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })
            .map_err(|status| {
                tracing::debug!(
                    timestamp = %timestamp_millis,
                    status = %status
                );
                status
            })?;

        _ = self.subscriber_location_report_sink.write(report, []).await;

        Ok(Response::new(SubscriberLocationRespV1 {
            id: timestamp.to_string(),
        }))
    }

    async fn submit_threshold_report(
        &self,
        request: Request<RadioThresholdReportReqV1>,
    ) -> GrpcResult<RadioThresholdReportRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let hotspot_pubkey = event.hotspot_pubkey.clone();
        let cbsd_id = event.cbsd_id.clone();
        let threshold_timestamp = event.threshold_timestamp;

        custom_tracing::record_b58("pub_key", &hotspot_pubkey);

        let report = self
            .verify_public_key(event.carrier_pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| RadioThresholdIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })
            .map_err(|status| {
                tracing::debug!(
                    cbsd_id = ?cbsd_id,
                    threshold_timestamp = %threshold_timestamp,
                    status = %status
                );
                status
            })?;

        _ = self.radio_threshold_report_sink.write(report, []).await;

        Ok(Response::new(RadioThresholdReportRespV1 {
            id: timestamp.to_string(),
        }))
    }

    async fn submit_invalidated_threshold_report(
        &self,
        request: Request<InvalidatedRadioThresholdReportReqV1>,
    ) -> GrpcResult<InvalidatedRadioThresholdReportRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let hotspot_pubkey = event.hotspot_pubkey.clone();
        let cbsd_id = event.cbsd_id.clone();
        let invalidated_timestamp = event.timestamp;

        custom_tracing::record_b58("pub_key", &hotspot_pubkey);

        let report = self
            .verify_public_key(event.carrier_pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| InvalidatedRadioThresholdIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })
            .map_err(|status| {
                tracing::debug!(
                    cbsd_id = ?cbsd_id,
                    invalidated_timestamp = %invalidated_timestamp,
                    status = %status
                );
                status
            })?;

        _ = self
            .invalidated_radio_threshold_report_sink
            .write(report, [])
            .await;

        Ok(Response::new(InvalidatedRadioThresholdReportRespV1 {
            id: timestamp.to_string(),
        }))
    }

    async fn submit_coverage_object(
        &self,
        request: Request<CoverageObjectReqV1>,
    ) -> GrpcResult<CoverageObjectRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        custom_tracing::record_b58("pub_key", &event.pub_key);

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| CoverageObjectIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.coverage_object_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(CoverageObjectRespV1 { id }))
    }

    async fn submit_sp_boosted_rewards_banned_radio(
        &self,
        request: Request<ServiceProviderBoostedRewardsBannedRadioReqV1>,
    ) -> GrpcResult<ServiceProviderBoostedRewardsBannedRadioRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        custom_tracing::record_b58("pub_key", &event.pubkey);

        let report = self
            .verify_public_key(event.pubkey.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(
                |(_, event)| ServiceProviderBoostedRewardsBannedRadioIngestReportV1 {
                    received_timestamp: timestamp,
                    report: Some(event),
                },
            )?;

        _ = self.sp_boosted_rewards_ban_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(
            ServiceProviderBoostedRewardsBannedRadioRespV1 { id },
        ))
    }

    async fn submit_subscriber_verified_mapping_event(
        &self,
        request: Request<SubscriberVerifiedMappingEventReqV1>,
    ) -> GrpcResult<SubscriberVerifiedMappingEventResV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event: SubscriberVerifiedMappingEventReqV1 = request.into_inner();

        custom_tracing::record_b58("subscriber_id", &event.subscriber_id);
        custom_tracing::record_b58("pub_key", &event.carrier_mapping_key);

        let report = self
            .verify_public_key(event.carrier_mapping_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| SubscriberVerifiedMappingEventIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.subscriber_mapping_event_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(SubscriberVerifiedMappingEventResV1 { id }))
    }

    async fn submit_radio_location_estimates(
        &self,
        request: Request<RadioLocationEstimatesReqV1>,
    ) -> GrpcResult<RadioLocationEstimatesRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let req: RadioLocationEstimatesReqV1 = request.into_inner();

        custom_tracing::record_b58("pub_key", &req.carrier_key);

        let report = self
            .verify_public_key(req.carrier_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, req))
            .map(|(_, req)| RadioLocationEstimatesIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(req),
            })?;

        _ = self.radio_location_estimate_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(RadioLocationEstimatesRespV1 { id }))
    }
}

pub async fn grpc_server(settings: &Settings) -> Result<()> {
    // Initialize uploader
    let (file_upload, file_upload_server) =
        file_upload::FileUpload::from_settings_tm(&settings.output).await?;

    let store_base_path = Path::new(&settings.cache);

    let (heartbeat_report_sink, heartbeat_report_sink_server) =
        CellHeartbeatIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (wifi_heartbeat_report_sink, wifi_heartbeat_report_sink_server) =
        WifiHeartbeatIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    // speedtests
    let (speedtest_report_sink, speedtest_report_sink_server) = SpeedtestIngestReportV1::file_sink(
        store_base_path,
        file_upload.clone(),
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(settings.roll_time),
        env!("CARGO_PKG_NAME"),
    )
    .await?;

    let (data_transfer_session_sink, data_transfer_session_sink_server) =
        DataTransferSessionIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (subscriber_location_report_sink, subscriber_location_report_sink_server) =
        SubscriberLocationIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (radio_threshold_report_sink, radio_threshold_report_sink_server) =
        RadioThresholdIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (invalidated_radio_threshold_report_sink, invalidated_radio_threshold_report_sink_server) =
        InvalidatedRadioThresholdIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (coverage_object_report_sink, coverage_object_report_sink_server) =
        CoverageObjectIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (sp_boosted_rewards_ban_sink, sp_boosted_rewards_ban_sink_server) =
        ServiceProviderBoostedRewardsBannedRadioIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (subscriber_mapping_event_sink, subscriber_mapping_event_server) =
        SubscriberVerifiedMappingEventIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (radio_location_estimates_sink, radio_location_estimates_server) =
        RadioLocationEstimatesIngestReportV1::file_sink(
            store_base_path,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let Some(api_token) = settings
        .token
        .as_ref()
        .and_then(|token| format!("Bearer {token}").parse::<MetadataValue<_>>().ok())
    else {
        bail!("expected valid api token in settings");
    };

    let grpc_server = GrpcServer::new(
        heartbeat_report_sink,
        wifi_heartbeat_report_sink,
        speedtest_report_sink,
        data_transfer_session_sink,
        subscriber_location_report_sink,
        radio_threshold_report_sink,
        invalidated_radio_threshold_report_sink,
        coverage_object_report_sink,
        sp_boosted_rewards_ban_sink,
        subscriber_mapping_event_sink,
        radio_location_estimates_sink,
        settings.network,
        settings.listen_addr,
        api_token,
    );

    tracing::info!(
        "grpc listening on {} and server mode {:?}",
        settings.listen_addr,
        settings.mode
    );

    TaskManager::builder()
        .add_task(file_upload_server)
        .add_task(heartbeat_report_sink_server)
        .add_task(wifi_heartbeat_report_sink_server)
        .add_task(speedtest_report_sink_server)
        .add_task(data_transfer_session_sink_server)
        .add_task(subscriber_location_report_sink_server)
        .add_task(radio_threshold_report_sink_server)
        .add_task(invalidated_radio_threshold_report_sink_server)
        .add_task(coverage_object_report_sink_server)
        .add_task(sp_boosted_rewards_ban_sink_server)
        .add_task(subscriber_mapping_event_server)
        .add_task(radio_location_estimates_server)
        .add_task(grpc_server)
        .build()
        .start()
        .await
}
