//! Decoding Arrow `RecordBatch`es (read out of Iceberg data files) into typed
//! rows. This is the inverse of `iceberg_table::records_to_batch`, which
//! serializes `T: Serialize` into a batch via arrow-json on the way in.
//!
//! arrow-json 57 has no Arrow→serde reader, so we go the other direction:
//! `arrow_json::ArrayWriter` renders the batch as a JSON array of row objects
//! and `serde_json` deserializes that into `Vec<T>`. Both crates are already
//! dependencies, so this adds nothing new. Any `T` that round-trips through the
//! writer round-trips back here.

use crate::{Error, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use serde::de::DeserializeOwned;

/// Converts a `RecordBatch` of rows added by a snapshot into typed messages.
#[async_trait]
pub trait IcebergStreamParser<T>: Send + Sync + 'static {
    async fn parse(&self, batch: RecordBatch) -> Result<Vec<T>>;
}

/// Default parser: deserialize each row via serde, using arrow-json as the
/// Arrow→JSON bridge.
///
/// Caveat: `ArrayWriter` omits keys whose value is null, so `T`'s optional
/// columns must tolerate missing keys (serde's default `Option` handling and
/// `#[serde(default)]` both do).
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonIcebergStreamParser;

/// Render an Arrow `RecordBatch` to `Vec<T>` via arrow-json + serde_json.
///
/// Returned as a free function so it can be unit-tested directly against
/// `records_to_batch` without spinning up a poller.
pub fn batch_to_records<T: DeserializeOwned>(batch: &RecordBatch) -> Result<Vec<T>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(&mut buf);
    writer
        .write(batch)
        .map_err(|e| Error::Reader(format!("arrow-json write error: {e}")))?;
    writer
        .finish()
        .map_err(|e| Error::Reader(format!("arrow-json finish error: {e}")))?;

    serde_json::from_slice::<Vec<T>>(&buf)
        .map_err(|e| Error::Reader(format!("deserialize error: {e}")))
}

#[async_trait]
impl<T> IcebergStreamParser<T> for JsonIcebergStreamParser
where
    T: DeserializeOwned + Send + 'static,
{
    async fn parse(&self, batch: RecordBatch) -> Result<Vec<T>> {
        batch_to_records(&batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Row {
        name: String,
        age: i64,
        score: f64,
        // Exercises the null-omission caveat: missing keys must deserialize.
        #[serde(default)]
        nickname: Option<String>,
    }

    /// Build a `RecordBatch` from serde records the same way
    /// `iceberg_table::records_to_batch` does (arrow-json decoder), so this
    /// test is the literal inverse round-trip of the writer path.
    fn records_to_batch(schema: Arc<Schema>, records: &[Row]) -> RecordBatch {
        let mut decoder = arrow_json::reader::ReaderBuilder::new(schema)
            .build_decoder()
            .expect("decoder");
        decoder.serialize(records).expect("serialize");
        decoder.flush().expect("flush").expect("batch")
    }

    #[test]
    fn round_trips_records_through_a_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("nickname", DataType::Utf8, true),
        ]));

        let input = vec![
            Row {
                name: "alice".into(),
                age: 42,
                score: 1.5,
                nickname: Some("al".into()),
            },
            Row {
                name: "bob".into(),
                age: 7,
                score: -3.25,
                nickname: None,
            },
        ];

        let batch = records_to_batch(schema, &input);
        let output: Vec<Row> = batch_to_records(&batch).expect("batch_to_records");

        assert_eq!(input, output);
    }

    #[test]
    fn empty_batch_yields_no_records() {
        let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema);
        let output: Vec<Row> = batch_to_records(&batch).expect("batch_to_records");
        assert!(output.is_empty());
    }
}
