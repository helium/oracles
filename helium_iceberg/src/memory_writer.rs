use crate::{DataWriter, Error, Result};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde::Serialize;
use std::any::Any;
use std::sync::Mutex;

/// An in-memory implementation of `DataWriter` for testing purposes.
///
/// This writer stores records in memory rather than writing to Iceberg,
/// allowing tests to verify what was written without requiring actual
/// Iceberg infrastructure.
///
/// # Type Safety
///
/// `MemoryDataWriter<T>` is generic over the record type. When `write` is called,
/// it verifies at runtime that the records being written match the expected type `T`.
/// If a type mismatch occurs, an error is returned.
pub struct MemoryDataWriter<T> {
    records: Mutex<Vec<T>>,
}

impl<T: Clone> MemoryDataWriter<T> {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }

    /// Returns a clone of all records written to this writer.
    pub fn records(&self) -> Vec<T> {
        self.records.lock().expect("lock poisoned").clone()
    }

    /// Returns the number of records written.
    pub fn len(&self) -> usize {
        self.records.lock().expect("lock poisoned").len()
    }

    /// Returns true if no records have been written.
    pub fn is_empty(&self) -> bool {
        self.records.lock().expect("lock poisoned").is_empty()
    }

    /// Clears all stored records.
    pub fn clear(&self) {
        self.records.lock().expect("lock poisoned").clear();
    }
}

impl<T: Clone> Default for MemoryDataWriter<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> DataWriter for MemoryDataWriter<T> {
    async fn write<U>(&self, records: Vec<U>) -> Result
    where
        U: Serialize + Send + Sync + 'static,
    {
        if records.is_empty() {
            return Ok(());
        }

        // Use Any to downcast and verify type consistency
        let boxed: Box<dyn Any> = Box::new(records);
        let typed_records = boxed.downcast::<Vec<T>>().map_err(|_| {
            Error::Writer(format!(
                "type mismatch: expected {}, got different type",
                std::any::type_name::<T>()
            ))
        })?;

        self.records
            .lock()
            .expect("lock poisoned")
            .extend(*typed_records);

        Ok(())
    }

    async fn write_stream<U, S>(&self, stream: S) -> Result
    where
        U: Serialize + Send + Sync + 'static,
        S: Stream<Item = U> + Send + 'static,
    {
        let records: Vec<U> = stream.collect().await;
        self.write(records).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestRecord {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn test_write_records() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        let records = vec![
            TestRecord {
                id: 1,
                name: "alice".to_string(),
            },
            TestRecord {
                id: 2,
                name: "bob".to_string(),
            },
        ];

        writer.write(records.clone()).await.unwrap();

        assert_eq!(writer.len(), 2);
        assert_eq!(writer.records(), records);
    }

    #[tokio::test]
    async fn test_write_empty_records() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        writer.write(Vec::<TestRecord>::new()).await.unwrap();

        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_writes() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        writer
            .write(vec![TestRecord {
                id: 1,
                name: "alice".to_string(),
            }])
            .await
            .unwrap();

        writer
            .write(vec![TestRecord {
                id: 2,
                name: "bob".to_string(),
            }])
            .await
            .unwrap();

        assert_eq!(writer.len(), 2);
    }

    #[tokio::test]
    async fn test_clear() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        writer
            .write(vec![TestRecord {
                id: 1,
                name: "alice".to_string(),
            }])
            .await
            .unwrap();

        assert!(!writer.is_empty());

        writer.clear();

        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_default() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::default();
        assert!(writer.is_empty());
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct OtherRecord {
        value: i32,
    }

    #[tokio::test]
    async fn test_type_mismatch_error() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        let result = writer.write(vec![OtherRecord { value: 42 }]).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Writer(_)));
    }

    #[tokio::test]
    async fn test_write_stream() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        let records = vec![
            TestRecord {
                id: 1,
                name: "alice".to_string(),
            },
            TestRecord {
                id: 2,
                name: "bob".to_string(),
            },
        ];

        let stream = futures::stream::iter(records.clone());
        writer.write_stream(stream).await.unwrap();

        assert_eq!(writer.len(), 2);
        assert_eq!(writer.records(), records);
    }

    #[tokio::test]
    async fn test_write_stream_empty() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        let stream = futures::stream::iter(Vec::<TestRecord>::new());
        writer.write_stream(stream).await.unwrap();

        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_write_stream_type_mismatch() {
        let writer: MemoryDataWriter<TestRecord> = MemoryDataWriter::new();

        let stream = futures::stream::iter(vec![OtherRecord { value: 42 }]);
        let result = writer.write_stream(stream).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Writer(_)));
    }
}
