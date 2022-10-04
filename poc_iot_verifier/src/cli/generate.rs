use crate::{cli::print_json, write_json, Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::{self, StreamExt};
use helium_proto::services::poc_lora::LoraValidPocV1;
use helium_proto::Message;
use poc_store::{datetime_from_naive, BytesMutStream, FileStore, FileType};
use serde_json::json;

/// load and print POCs
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required start time to look for (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
    /// Required before time to look for (inclusive)
    #[clap(long)]
    before: NaiveDateTime,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;
        from_period(
            store,
            datetime_from_naive(self.after),
            datetime_from_naive(self.before),
        )
        .await?
        .map_or_else(
            || Ok(()),
            |pocs| {
                for poc in pocs {
                    print_json(&json!({ "poc_id": poc.poc_id, "beacon_report": poc.beacon_report, "witness_reports": poc.witness_reports }));
                }
                Ok(())
            },
        )
    }
}

pub async fn from_period(
    store: FileStore,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result<Option<Vec<LoraValidPocV1>>> {
    if let Some(pocs) = get_pocs(store, after_utc, before_utc).await? {
        return Ok(Some(pocs));
    }
    Ok(None)
}

async fn get_pocs(
    store: FileStore,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result<Option<Vec<LoraValidPocV1>>> {
    if before_utc <= after_utc {
        tracing::error!(
            "cannot reward future period, before: {:?}, after: {:?}",
            before_utc,
            after_utc
        );
        return Err(Error::NotFound("cannot reward future".to_string()));
    }
    let after_ts = after_utc.timestamp() as u64;
    let before_ts = before_utc.timestamp() as u64;

    let file_list = store
        .list_all(FileType::LoraValidPoc, after_utc, before_utc)
        .await?;

    write_json(
        "file_list",
        after_ts,
        before_ts,
        &json!({ "file_list": file_list }),
    )?;
    let mut stream = store.source(stream::iter(file_list).map(Ok).boxed());

    let pocs = gather_pocs(&mut stream).await?;
    write_json("pocs", after_ts, before_ts, &json!({ "pocs": pocs }))?;

    return Ok(Some(pocs));
}

pub async fn gather_pocs(stream: &mut BytesMutStream) -> Result<Vec<LoraValidPocV1>> {
    let mut pocs: Vec<LoraValidPocV1> = Vec::new();

    while let Some(Ok(msg)) = stream.next().await {
        let poc: LoraValidPocV1 = LoraValidPocV1::decode(msg)?;
        pocs.push(poc)
    }
    Ok(pocs)
}
