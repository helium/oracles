use chrono::{DateTime, Utc};
use file_store::{FileStore, FileType};
use futures::{Stream, StreamExt};
use helium_proto::services::router::PacketRouterPacketReportV1;
use prost::Message;
use std::ops::Range;

pub const DOWNLOAD_WORKERS: usize = 50;

pub fn ingest_reports(
    file_store: &FileStore,
    epoch: &Range<DateTime<Utc>>,
) -> impl Stream<Item = PacketRouterPacketReportV1> {
    file_store
        .source_unordered(
            DOWNLOAD_WORKERS,
            file_store
                .list(FileType::IotPacketReport, epoch.start, epoch.end)
                .boxed(),
        )
        .filter_map(|msg| async move {
            msg.map_err(|err| {
                tracing::error!("Error fetching packet report: {:?}", err);
                err
            })
            .ok()
        })
        .filter_map(|msg| async move {
            PacketRouterPacketReportV1::decode(msg).map_or_else(
                |err| {
                    tracing::error!("Could not decode packet report: {:?}", err);
                    None
                },
                |report| Some(report),
            )
        })
}
