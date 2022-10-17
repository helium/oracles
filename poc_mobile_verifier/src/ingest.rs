use crate::Result;
use chrono::{DateTime, Utc};
use file_store::{
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    traits::MsgDecode,
    FileStore, FileType,
};
use futures::stream::{self, StreamExt};
use std::ops::Range;

pub async fn new_heartbeat_reports(
    file_store: &FileStore,
    epoch: &Range<DateTime<Utc>>,
) -> Result<Vec<CellHeartbeat>> {
    let file_list = file_store
        .list_all(FileType::CellHeartbeatIngestReport, epoch.start, epoch.end)
        .await?;
    let mut stream = file_store.source(stream::iter(file_list.clone()).map(Ok).boxed());

    let mut reports = vec![];

    while let Some(Ok(msg)) = stream.next().await {
        let heartbeat_report = match CellHeartbeatIngestReport::decode(msg) {
            Ok(report) => report.report,
            Err(err) => {
                tracing::error!("Could not decode cell heartbeat ingest report: {:?}", err);
                continue;
            }
        };

        reports.push(heartbeat_report);
    }

    Ok(reports)
}
