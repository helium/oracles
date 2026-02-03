use crate::{DataWriter, Result};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde::Serialize;
use std::sync::Mutex;

/// An in-memory implementation of `DataWriter` for testing purposes.
///
/// This writer stores records in memory rather than writing to Iceberg,
/// allowing tests to verify what was written without requiring actual
/// Iceberg infrastructure.
///
/// # Type Safety
///
/// `MemoryDataWriter<T>` is generic over the record type `T`, providing
/// compile-time type safety through the `DataWriter<T>` trait.
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
impl<T: Clone + Serialize + Send + Sync + 'static> DataWriter<T> for MemoryDataWriter<T> {
    async fn write(&self, records: Vec<T>) -> Result {
        if records.is_empty() {
            return Ok(());
        }

        self.records.lock().expect("lock poisoned").extend(records);

        Ok(())
    }

    async fn write_stream<S>(&self, stream: S) -> Result
    where
        S: Stream<Item = T> + Send + 'static,
    {
        let records: Vec<T> = stream.collect().await;
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

    // Note: Type mismatch tests are no longer needed because the trait-level generic
    // provides compile-time type safety. Attempting to write a different type will
    // result in a compile error rather than a runtime error.

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
}
