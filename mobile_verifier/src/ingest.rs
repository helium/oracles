use chrono::{DateTime, Utc};
use file_store::{
    mobile_transfer::ValidDataTransferSession, traits::MsgDecode, FileStore, FileType,
};
use futures::{Stream, StreamExt};
use std::ops::Range;

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
