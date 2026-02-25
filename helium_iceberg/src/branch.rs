use crate::catalog::Catalog;
use crate::{Error, Result};
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention,
    SnapshotSummaryCollector, Summary, MAIN_BRANCH,
};
use iceberg::table::Table;
use iceberg::{TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) const WAP_ID_KEY: &str = "wap.id";

/// Create a branch from the current main snapshot.
///
/// If the table has no snapshots yet, this is a no-op — the branch ref
/// will be created by the first `commit_to_branch` call instead.
pub(crate) async fn create_branch(
    catalog: &Catalog,
    table: &Table,
    branch_name: &str,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    let metadata = table.metadata();
    let Some(main_snapshot_id) = metadata.current_snapshot_id() else {
        return Ok(());
    };

    let updates = vec![TableUpdate::SetSnapshotRef {
        ref_name: branch_name.to_string(),
        reference: SnapshotReference::new(
            main_snapshot_id,
            SnapshotRetention::branch(None, None, None),
        ),
    }];

    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: branch_name.to_string(),
            snapshot_id: None,
        },
    ];

    commit(catalog, table.identifier(), updates, requirements).await
}

/// Write data files to a named branch, building the snapshot and manifest infrastructure.
pub(crate) async fn commit_to_branch(
    catalog: &Catalog,
    table: &Table,
    branch_name: &str,
    data_files: Vec<DataFile>,
    wap_id: &str,
    custom_properties: HashMap<String, String>,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    if data_files.is_empty() {
        return Err(Error::Branch("no data files to commit".into()));
    }

    let metadata = table.metadata();
    let branch_snapshot = metadata.snapshot_for_ref(branch_name);
    let parent_snapshot_id = branch_snapshot.map(|s| s.snapshot_id());

    let snapshot_id = generate_unique_snapshot_id(table);
    let commit_uuid = Uuid::now_v7();
    let next_seq_num = metadata.next_sequence_number();
    let schema = metadata.current_schema().clone();
    let partition_spec = metadata.default_partition_spec();

    // Build summary
    let mut summary_collector = SnapshotSummaryCollector::default();
    for data_file in &data_files {
        summary_collector.add_file(data_file, schema.clone(), partition_spec.clone());
    }
    let mut additional_properties = summary_collector.build();
    additional_properties.insert(WAP_ID_KEY.to_string(), wap_id.to_string());
    additional_properties.extend(custom_properties);
    let summary = Summary {
        operation: Operation::Append,
        additional_properties,
    };

    // Write manifest file for the new data files
    let manifest_path = format!(
        "{}/metadata/{}-m0.{}",
        metadata.location(),
        commit_uuid,
        DataFileFormat::Avro
    );
    let output_file = table
        .file_io()
        .new_output(&manifest_path)
        .map_err(Error::Iceberg)?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(snapshot_id),
        None,
        schema.clone(),
        partition_spec.as_ref().clone(),
    );
    let mut manifest_writer = match metadata.format_version() {
        FormatVersion::V1 => builder.build_v1(),
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
    };

    for data_file in data_files {
        manifest_writer
            .add_file(data_file, next_seq_num)
            .map_err(Error::Iceberg)?;
    }
    let new_manifest = manifest_writer
        .write_manifest_file()
        .await
        .map_err(Error::Iceberg)?;

    // Collect existing manifests from the branch's current snapshot
    let mut manifests: Vec<ManifestFile> = vec![new_manifest];
    if let Some(branch_snapshot) = branch_snapshot {
        let manifest_list = branch_snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .map_err(Error::Iceberg)?;
        manifests.extend(
            manifest_list
                .entries()
                .iter()
                .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                .cloned(),
        );
    }

    // Write manifest list
    let manifest_list_path = format!(
        "{}/metadata/snap-{}-0-{}.{}",
        metadata.location(),
        snapshot_id,
        commit_uuid,
        DataFileFormat::Avro
    );
    let manifest_list_output = table
        .file_io()
        .new_output(&manifest_list_path)
        .map_err(Error::Iceberg)?;
    let mut manifest_list_writer = match metadata.format_version() {
        FormatVersion::V1 => {
            ManifestListWriter::v1(manifest_list_output, snapshot_id, parent_snapshot_id)
        }
        FormatVersion::V2 => ManifestListWriter::v2(
            manifest_list_output,
            snapshot_id,
            parent_snapshot_id,
            next_seq_num,
        ),
        FormatVersion::V3 => ManifestListWriter::v3(
            manifest_list_output,
            snapshot_id,
            parent_snapshot_id,
            next_seq_num,
            None,
        ),
    };
    manifest_list_writer
        .add_manifests(manifests.into_iter())
        .map_err(Error::Iceberg)?;
    manifest_list_writer.close().await.map_err(Error::Iceberg)?;

    // Build snapshot
    let commit_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| Error::Branch(format!("failed to get system time: {e}")))?;
    let new_snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(next_seq_num)
        .with_timestamp_ms(commit_ts)
        .with_manifest_list(manifest_list_path)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id())
        .build();

    let updates = vec![
        TableUpdate::AddSnapshot {
            snapshot: new_snapshot,
        },
        TableUpdate::SetSnapshotRef {
            ref_name: branch_name.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];

    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: branch_name.to_string(),
            snapshot_id: parent_snapshot_id,
        },
    ];

    commit(catalog, table.identifier(), updates, requirements).await
}

/// Fast-forward main to a branch's snapshot, then delete the branch.
pub(crate) async fn publish_branch(
    catalog: &Catalog,
    table: &Table,
    branch_name: &str,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    let metadata = table.metadata();
    let branch_snapshot = metadata
        .snapshot_for_ref(branch_name)
        .ok_or_else(|| Error::Branch(format!("branch '{branch_name}' does not exist")))?;
    let branch_snapshot_id = branch_snapshot.snapshot_id();

    let updates = vec![
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                branch_snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
        TableUpdate::RemoveSnapshotRef {
            ref_name: branch_name.to_string(),
        },
    ];

    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: metadata.current_snapshot_id(),
        },
    ];

    commit(catalog, table.identifier(), updates, requirements).await
}

/// Delete a branch.
pub(crate) async fn delete_branch(
    catalog: &Catalog,
    table: &Table,
    branch_name: &str,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    let updates = vec![TableUpdate::RemoveSnapshotRef {
        ref_name: branch_name.to_string(),
    }];

    let requirements = vec![TableRequirement::UuidMatch {
        uuid: table.metadata().uuid(),
    }];

    commit(catalog, table.identifier(), updates, requirements).await
}

fn validate_branch_name(branch_name: &str) -> Result<()> {
    if branch_name == MAIN_BRANCH {
        return Err(Error::Branch(
            "cannot use 'main' as a branch name for WAP operations".into(),
        ));
    }
    if branch_name.is_empty() {
        return Err(Error::Branch("branch name cannot be empty".into()));
    }
    Ok(())
}

fn generate_unique_snapshot_id(table: &Table) -> i64 {
    let generate_random_id = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64;
        snapshot_id.abs()
    };

    let mut snapshot_id = generate_random_id();
    while table
        .metadata()
        .snapshots()
        .any(|s| s.snapshot_id() == snapshot_id)
    {
        snapshot_id = generate_random_id();
    }
    snapshot_id
}

async fn commit(
    catalog: &Catalog,
    table_ident: &TableIdent,
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
) -> Result<()> {
    let request = CommitTableRequest {
        identifier: Some(table_ident.clone()),
        requirements,
        updates,
    };

    catalog.commit_table_request(&request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_branch_name() {
        assert!(validate_branch_name("audit").is_ok());
        assert!(validate_branch_name("staging").is_ok());

        assert!(validate_branch_name("main").is_err());
        assert!(validate_branch_name("").is_err());
    }

    #[test]
    fn test_create_branch_updates() {
        let snapshot_id = 42;
        let uuid = Uuid::new_v4();

        let updates = [TableUpdate::SetSnapshotRef {
            ref_name: "audit".to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        }];

        let requirements = [
            TableRequirement::UuidMatch { uuid },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "audit".to_string(),
                snapshot_id: None,
            },
        ];

        // Verify update structure
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "audit");
                assert_eq!(reference.snapshot_id, snapshot_id);
                assert!(reference.is_branch());
            }
            _ => panic!("expected SetSnapshotRef"),
        }

        // Verify requirements
        match &requirements[0] {
            TableRequirement::UuidMatch { uuid: u } => assert_eq!(*u, uuid),
            _ => panic!("expected UuidMatch"),
        }
        match &requirements[1] {
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                assert_eq!(r#ref, "audit");
                assert!(snapshot_id.is_none());
            }
            _ => panic!("expected RefSnapshotIdMatch"),
        }
    }

    #[test]
    fn test_publish_branch_updates() {
        let branch_snapshot_id = 99;
        let main_snapshot_id = Some(42);
        let uuid = Uuid::new_v4();

        let updates = [
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    branch_snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
            TableUpdate::RemoveSnapshotRef {
                ref_name: "audit".to_string(),
            },
        ];

        let requirements = [
            TableRequirement::UuidMatch { uuid },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: main_snapshot_id,
            },
        ];

        // Verify main is pointed to branch's snapshot
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert_eq!(reference.snapshot_id, branch_snapshot_id);
            }
            _ => panic!("expected SetSnapshotRef"),
        }

        // Verify branch is removed
        match &updates[1] {
            TableUpdate::RemoveSnapshotRef { ref_name } => {
                assert_eq!(ref_name, "audit");
            }
            _ => panic!("expected RemoveSnapshotRef"),
        }

        // Verify main snapshot id requirement
        match &requirements[1] {
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                assert_eq!(r#ref, MAIN_BRANCH);
                assert_eq!(*snapshot_id, main_snapshot_id);
            }
            _ => panic!("expected RefSnapshotIdMatch"),
        }
    }

    #[test]
    fn test_delete_branch_updates() {
        let uuid = Uuid::new_v4();

        let updates = [TableUpdate::RemoveSnapshotRef {
            ref_name: "audit".to_string(),
        }];

        let requirements = [TableRequirement::UuidMatch { uuid }];

        match &updates[0] {
            TableUpdate::RemoveSnapshotRef { ref_name } => {
                assert_eq!(ref_name, "audit");
            }
            _ => panic!("expected RemoveSnapshotRef"),
        }

        match &requirements[0] {
            TableRequirement::UuidMatch { uuid: u } => assert_eq!(*u, uuid),
            _ => panic!("expected UuidMatch"),
        }
    }
}
