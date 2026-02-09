use crate::{BranchWriter, DataWriter, Error, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;
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
}

/// An in-memory implementation of `BranchWriter` for testing purposes.
///
/// This writer tracks branches as named collections of records and supports
/// publishing branch records to a separate "published" collection. Since
/// `BranchWriter` uses `&mut self`, no `Mutex` is needed.
pub struct MemoryBranchWriter<T> {
    branches: HashMap<String, Vec<T>>,
    published: Vec<T>,
}

impl<T> MemoryBranchWriter<T> {
    pub fn new() -> Self {
        Self {
            branches: HashMap::new(),
            published: Vec::new(),
        }
    }

    /// Returns a reference to the records on a given branch.
    pub fn branch_records(&self, branch_name: &str) -> Option<&Vec<T>> {
        self.branches.get(branch_name)
    }

    /// Returns true if the named branch exists.
    pub fn branch_exists(&self, branch_name: &str) -> bool {
        self.branches.contains_key(branch_name)
    }

    /// Returns the names of all existing branches.
    pub fn branch_names(&self) -> Vec<&String> {
        self.branches.keys().collect()
    }

    /// Returns a reference to all published records.
    pub fn published(&self) -> &[T] {
        &self.published
    }

    /// Returns the number of published records.
    pub fn published_len(&self) -> usize {
        self.published.len()
    }
}

impl<T> Default for MemoryBranchWriter<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync + 'static> BranchWriter<T> for MemoryBranchWriter<T> {
    async fn create_branch(&mut self, branch_name: &str) -> Result {
        if self.branches.contains_key(branch_name) {
            return Err(Error::Branch(format!(
                "branch '{branch_name}' already exists"
            )));
        }
        self.branches.insert(branch_name.to_string(), Vec::new());
        Ok(())
    }

    async fn write_to_branch(&mut self, branch_name: &str, records: Vec<T>, _wap_id: &str) -> Result {
        let branch = self
            .branches
            .get_mut(branch_name)
            .ok_or_else(|| Error::Branch(format!("branch '{branch_name}' does not exist")))?;
        branch.extend(records);
        Ok(())
    }

    async fn publish_branch(&mut self, branch_name: &str) -> Result {
        let records = self
            .branches
            .remove(branch_name)
            .ok_or_else(|| Error::Branch(format!("branch '{branch_name}' does not exist")))?;
        self.published.extend(records);
        Ok(())
    }

    async fn delete_branch(&mut self, branch_name: &str) -> Result {
        if self.branches.remove(branch_name).is_none() {
            return Err(Error::Branch(format!(
                "branch '{branch_name}' does not exist"
            )));
        }
        Ok(())
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

    // MemoryBranchWriter tests

    #[tokio::test]
    async fn test_branch_create() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();
        assert!(writer.branch_exists("staging"));
    }

    #[tokio::test]
    async fn test_branch_create_duplicate() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();
        assert!(writer.create_branch("staging").await.is_err());
    }

    #[tokio::test]
    async fn test_branch_write() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();

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

        writer.write_to_branch("staging", records, "test-wap-id").await.unwrap();
        assert_eq!(writer.branch_records("staging").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_branch_write_to_missing() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        let result = writer
            .write_to_branch(
                "nonexistent",
                vec![TestRecord {
                    id: 1,
                    name: "alice".to_string(),
                }],
                "test-wap-id",
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_branch_publish() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();
        writer
            .write_to_branch(
                "staging",
                vec![TestRecord {
                    id: 1,
                    name: "alice".to_string(),
                }],
                "test-wap-id",
            )
            .await
            .unwrap();

        writer.publish_branch("staging").await.unwrap();

        assert!(!writer.branch_exists("staging"));
        assert_eq!(writer.published_len(), 1);
        assert_eq!(writer.published()[0].id, 1);
    }

    #[tokio::test]
    async fn test_branch_delete() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();
        writer.delete_branch("staging").await.unwrap();
        assert!(!writer.branch_exists("staging"));
    }

    #[tokio::test]
    async fn test_branch_delete_missing() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        assert!(writer.delete_branch("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_branch_full_workflow() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();

        writer.create_branch("staging").await.unwrap();

        writer
            .write_to_branch(
                "staging",
                vec![
                    TestRecord {
                        id: 1,
                        name: "alice".to_string(),
                    },
                    TestRecord {
                        id: 2,
                        name: "bob".to_string(),
                    },
                ],
                "test-wap-id",
            )
            .await
            .unwrap();

        writer
            .write_to_branch(
                "staging",
                vec![TestRecord {
                    id: 3,
                    name: "carol".to_string(),
                }],
                "test-wap-id",
            )
            .await
            .unwrap();

        assert_eq!(writer.branch_records("staging").unwrap().len(), 3);

        writer.publish_branch("staging").await.unwrap();

        assert!(!writer.branch_exists("staging"));
        assert_eq!(writer.published_len(), 3);
    }

    #[tokio::test]
    async fn test_branch_write_stream() {
        let mut writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();

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

        let stream = futures::stream::iter(records);
        writer
            .write_stream_to_branch("staging", stream, "test-wap-id")
            .await
            .unwrap();

        assert_eq!(writer.branch_records("staging").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_branch_default() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::default();
        assert!(writer.branch_names().is_empty());
        assert_eq!(writer.published_len(), 0);
    }
}
