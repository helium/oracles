use crate::writer::{StagedWriter, WriteOutcome};
use crate::{DataWriter, Error, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
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
impl<T: Clone + Serialize + Send + Sync> DataWriter<T> for MemoryDataWriter<T> {
    async fn write(&self, records: Vec<T>) -> Result {
        if records.is_empty() {
            return Ok(());
        }

        self.records.lock().expect("lock poisoned").extend(records);

        Ok(())
    }
}

#[cfg(test)]
pub(crate) struct MemoryBranchWriter<T> {
    branches: Mutex<HashMap<String, Vec<T>>>,
    published: Mutex<Vec<T>>,
}

#[cfg(test)]
impl<T: Clone> MemoryBranchWriter<T> {
    pub fn new() -> Self {
        Self {
            branches: Mutex::new(HashMap::new()),
            published: Mutex::new(Vec::new()),
        }
    }

    pub fn branch_records(&self, branch_name: &str) -> Option<Vec<T>> {
        self.branches
            .lock()
            .expect("lock poisoned")
            .get(branch_name)
            .cloned()
    }

    pub fn branch_exists(&self, branch_name: &str) -> bool {
        self.branches
            .lock()
            .expect("lock poisoned")
            .contains_key(branch_name)
    }

    pub fn branch_names(&self) -> Vec<String> {
        self.branches
            .lock()
            .expect("lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn published(&self) -> Vec<T> {
        self.published.lock().expect("lock poisoned").clone()
    }

    pub fn published_len(&self) -> usize {
        self.published.lock().expect("lock poisoned").len()
    }
}

#[cfg(test)]
impl<T: Clone> Default for MemoryBranchWriter<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[async_trait]
impl<T: Clone + Serialize + Send + Sync> crate::writer::BranchWriter<T> for MemoryBranchWriter<T> {
    async fn create_branch(&self, branch_name: &str) -> Result {
        let mut branches = self.branches.lock().expect("lock poisoned");
        if branches.contains_key(branch_name) {
            return Err(Error::Branch(format!(
                "branch '{branch_name}' already exists"
            )));
        }
        branches.insert(branch_name.to_string(), Vec::new());
        Ok(())
    }

    async fn write_to_branch(
        &self,
        branch_name: &str,
        records: Vec<T>,
        _wap_id: &str,
    ) -> Result {
        let mut branches = self.branches.lock().expect("lock poisoned");
        let branch = branches
            .get_mut(branch_name)
            .ok_or_else(|| Error::Branch(format!("branch '{branch_name}' does not exist")))?;
        branch.extend(records);
        Ok(())
    }

    async fn publish_branch(&self, branch_name: &str) -> Result {
        let records = self
            .branches
            .lock()
            .expect("lock poisoned")
            .remove(branch_name)
            .ok_or_else(|| Error::Branch(format!("branch '{branch_name}' does not exist")))?;
        self.published.lock().expect("lock poisoned").extend(records);
        Ok(())
    }

    async fn delete_branch(&self, branch_name: &str) -> Result {
        if self
            .branches
            .lock()
            .expect("lock poisoned")
            .remove(branch_name)
            .is_none()
        {
            return Err(Error::Branch(format!(
                "branch '{branch_name}' does not exist"
            )));
        }
        Ok(())
    }
}

/// An in-memory implementation of [`StagedWriter`] for testing WAP workflows.
///
/// Tracks staged records per `wap_id`, published records, and completed
/// wap_ids to simulate the full idempotent WAP lifecycle.
pub struct MemoryStagedWriter<T> {
    staged: Mutex<HashMap<String, Vec<T>>>,
    published: Mutex<Vec<T>>,
    completed: Mutex<HashSet<String>>,
}

impl<T: Clone> MemoryStagedWriter<T> {
    pub fn new() -> Self {
        Self {
            staged: Mutex::new(HashMap::new()),
            published: Mutex::new(Vec::new()),
            completed: Mutex::new(HashSet::new()),
        }
    }

    /// Returns a clone of all published records.
    pub fn published(&self) -> Vec<T> {
        self.published.lock().expect("lock poisoned").clone()
    }

    /// Returns the number of published records.
    pub fn published_len(&self) -> usize {
        self.published.lock().expect("lock poisoned").len()
    }

    /// Returns the staged records for a given wap_id, if any.
    pub fn staged_records(&self, wap_id: &str) -> Option<Vec<T>> {
        self.staged
            .lock()
            .expect("lock poisoned")
            .get(wap_id)
            .cloned()
    }

    /// Returns true if the given wap_id has been completed (published).
    pub fn is_completed(&self, wap_id: &str) -> bool {
        self.completed
            .lock()
            .expect("lock poisoned")
            .contains(wap_id)
    }
}

impl<T: Clone> Default for MemoryStagedWriter<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Clone + Serialize + Send + Sync> StagedWriter<T> for MemoryStagedWriter<T> {
    async fn stage(&self, wap_id: &str, records: Vec<T>) -> Result<WriteOutcome> {
        if self.is_completed(wap_id) {
            return Ok(WriteOutcome::AlreadyComplete);
        }

        self.staged
            .lock()
            .expect("lock poisoned")
            .insert(wap_id.to_string(), records);
        Ok(WriteOutcome::Written)
    }

    async fn publish(&self, wap_id: &str) -> Result {
        let records = self
            .staged
            .lock()
            .expect("lock poisoned")
            .remove(wap_id)
            .ok_or_else(|| Error::Branch(format!("no staged records for wap_id '{wap_id}'")))?;

        self.published
            .lock()
            .expect("lock poisoned")
            .extend(records);
        self.completed
            .lock()
            .expect("lock poisoned")
            .insert(wap_id.to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::BranchWriter;
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

    // MemoryBranchWriter tests

    #[tokio::test]
    async fn test_branch_create() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();
        assert!(writer.branch_exists("staging"));
    }

    #[tokio::test]
    async fn test_branch_create_duplicate() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();
        assert!(writer.create_branch("staging").await.is_err());
    }

    #[tokio::test]
    async fn test_branch_write() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
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

        writer
            .write_to_branch("staging", records, "test-wap-id")
            .await
            .unwrap();
        assert_eq!(writer.branch_records("staging").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_branch_write_to_missing() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
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
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
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
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        writer.create_branch("staging").await.unwrap();
        writer.delete_branch("staging").await.unwrap();
        assert!(!writer.branch_exists("staging"));
    }

    #[tokio::test]
    async fn test_branch_delete_missing() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();
        assert!(writer.delete_branch("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_branch_full_workflow() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::new();

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
    async fn test_branch_default() {
        let writer: MemoryBranchWriter<TestRecord> = MemoryBranchWriter::default();
        assert!(writer.branch_names().is_empty());
        assert_eq!(writer.published_len(), 0);
    }

    // MemoryStagedWriter tests

    #[tokio::test]
    async fn test_staged_writer_stage_and_publish() {
        let writer: MemoryStagedWriter<TestRecord> = MemoryStagedWriter::new();

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

        let outcome = writer.stage("wap-1", records).await.unwrap();
        assert_eq!(outcome, WriteOutcome::Written);
        assert_eq!(writer.staged_records("wap-1").unwrap().len(), 2);

        writer.publish("wap-1").await.unwrap();
        assert_eq!(writer.published_len(), 2);
        assert!(writer.is_completed("wap-1"));
        assert!(writer.staged_records("wap-1").is_none());
    }

    #[tokio::test]
    async fn test_staged_writer_already_complete() {
        let writer: MemoryStagedWriter<TestRecord> = MemoryStagedWriter::new();

        let records = vec![TestRecord {
            id: 1,
            name: "alice".to_string(),
        }];

        writer.stage("wap-1", records).await.unwrap();
        writer.publish("wap-1").await.unwrap();

        let outcome = writer
            .stage(
                "wap-1",
                vec![TestRecord {
                    id: 2,
                    name: "bob".to_string(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(outcome, WriteOutcome::AlreadyComplete);
        assert_eq!(writer.published_len(), 1);
    }

    #[tokio::test]
    async fn test_staged_writer_publish_missing() {
        let writer: MemoryStagedWriter<TestRecord> = MemoryStagedWriter::new();
        assert!(writer.publish("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_staged_writer_default() {
        let writer: MemoryStagedWriter<TestRecord> = MemoryStagedWriter::default();
        assert_eq!(writer.published_len(), 0);
        assert!(!writer.is_completed("wap-1"));
    }
}
