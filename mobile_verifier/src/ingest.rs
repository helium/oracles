use chrono::{DateTime, Utc};
use file_store::{
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    mobile_transfer::ValidDataTransferSession,
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    traits::MsgDecode,
    FileStore, FileType,
};
use futures::{Stream, StreamExt};
use std::ops::Range;

pub async fn ingest_heartbeats(
    file_store: &FileStore,
    epoch: &Range<DateTime<Utc>>,
) -> impl Stream<Item = CellHeartbeat> {
    file_store
        .source(
            file_store
                .list(FileType::CellHeartbeatIngestReport, epoch.start, epoch.end)
                .boxed(),
        )
        .filter_map(|msg| async move {
            msg.map_err(|err| {
                tracing::error!("Error fetching heartbeat ingest report: {:?}", err);
                err
            })
            .ok()
        })
        .filter_map(|msg| async move {
            CellHeartbeatIngestReport::decode(msg).map_or_else(
                |err| {
                    tracing::error!("Could not decode cell heartbeat ingest report: {:?}", err);
                    None
                },
                |report| Some(report.report),
            )
        })
}

pub async fn ingest_speedtests(
    file_store: &FileStore,
    epoch: &Range<DateTime<Utc>>,
) -> impl Stream<Item = CellSpeedtest> {
    file_store
        .source(
            file_store
                .list(FileType::CellSpeedtestIngestReport, epoch.start, epoch.end)
                .boxed(),
        )
        .filter_map(|msg| async move {
            msg.map_err(|err| {
                tracing::error!("Error fetching speedtest ingest report: {:?}", err);
                err
            })
            .ok()
        })
        .filter_map(|msg| async move {
            CellSpeedtestIngestReport::decode(msg).map_or_else(
                |err| {
                    tracing::error!("Could not decode cell speedtest ingest report: {:?}", err);
                    None
                },
                |report| Some(report.report),
            )
        })
}

pub async fn ingest_valid_data_transfers(
    file_store: &FileStore,
    epoch: &Range<DateTime<Utc>>,
) -> impl Stream<Item = ValidDataTransferSession> {
    file_store
        .source(
            file_store
                .list(FileType::ValidDataTransferSession, epoch.start, epoch.end)
                .boxed(),
        )
        .filter_map(|msg| async move {
            msg.map_err(|err| {
                tracing::error!("Error fetching valid data transfer session: {:?}", err);
                err
            })
            .ok()
        })
        .filter_map(|msg| async move {
            ValidDataTransferSession::decode(msg).map_or_else(
                |err| {
                    tracing::error!("Could not decode valid data transfer session: {:?}", err);
                    None
                },
                Some,
            )
        })
}
