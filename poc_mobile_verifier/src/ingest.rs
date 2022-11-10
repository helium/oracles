use chrono::{DateTime, Utc};
use file_store::{
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    traits::MsgDecode,
    FileStore, FileType,
};
use futures::{stream, Stream, StreamExt};
use std::ops::Range;

pub async fn ingest_heartbeats(
    file_store: &FileStore,
    epoch: &Range<DateTime<Utc>>,
) -> file_store::Result<impl Stream<Item = CellHeartbeat>> {
    Ok(file_store
        .source(
            stream::iter(
                file_store
                    .list_all(FileType::CellHeartbeatIngestReport, epoch.start, epoch.end)
                    .await?,
            )
            .map(Ok)
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
        }))
}

pub async fn ingest_speedtests(
    file_store: &FileStore,
    epoch: &Range<DateTime<Utc>>,
) -> file_store::Result<impl Stream<Item = CellSpeedtest>> {
    Ok(file_store
        .source(
            stream::iter(
                file_store
                    .list_all(FileType::CellSpeedtestIngestReport, epoch.start, epoch.end)
                    .await?,
            )
            .map(Ok)
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
        }))
}
