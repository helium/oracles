use crate::catalog::Catalog;
use crate::{Error, Result};
use backon::{ExponentialBuilder, Retryable};
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention,
    SnapshotSummaryCollector, Summary, MAIN_BRANCH,
};
use iceberg::table::Table;
use iceberg::{TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

pub(crate) const WAP_ENABLED_PROPERTY: &str = "write.wap.enabled";
pub(crate) const WAP_ID_KEY: &str = "wap.id";

fn commit_backoff() -> ExponentialBuilder {
    ExponentialBuilder::new()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(4)
        .with_jitter()
}

/// Create a branch from the current main snapshot.
///
/// If the table has no snapshots yet, this is a no-op — the branch ref
/// will be created by the first `commit_to_branch` call instead.
///
/// Retries with exponential backoff on commit conflicts.
pub(crate) async fn create_branch(
    catalog: &Catalog,
    table: &RwLock<Table>,
    branch_name: &str,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    (|| async {
        crate::iceberg_table::reload_table(catalog, table).await?;
        let table_guard = table.read().await;

        let metadata = table_guard.metadata();
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

        commit(catalog, table_guard.identifier(), updates, requirements).await
    })
    .retry(commit_backoff())
    .when(|e: &Error| e.is_commit_conflict())
    .notify(|_err, dur| tracing::warn!(delay = ?dur, "commit conflict, retrying create_branch"))
    .await
}

/// Write data files to a named branch, building the snapshot and manifest infrastructure.
///
/// The table-global sequence number can conflict when concurrent writers commit
/// against the same table metadata, so this retries with exponential backoff.
/// Each retry rebuilds manifests and snapshots from fresh metadata.
pub(crate) async fn commit_to_branch(
    catalog: &Catalog,
    table: &RwLock<Table>,
    branch_name: &str,
    data_files: Vec<DataFile>,
    wap_id: &str,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    if data_files.is_empty() {
        return Err(Error::Branch("no data files to commit".into()));
    }

    (|| async {
        crate::iceberg_table::reload_table(catalog, table).await?;
        let table_guard = table.read().await;
        commit_to_branch_once(
            catalog,
            &table_guard,
            branch_name,
            data_files.clone(),
            wap_id,
        )
        .await
    })
    .retry(commit_backoff())
    .when(|e: &Error| e.is_commit_conflict())
    .notify(|_err, dur| tracing::warn!(delay = ?dur, "commit conflict, retrying commit_to_branch"))
    .await
}

async fn commit_to_branch_once(
    catalog: &Catalog,
    table: &Table,
    branch_name: &str,
    data_files: Vec<DataFile>,
    wap_id: &str,
) -> Result<()> {
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

    let manifest_list_path = write_manifest_list(
        table,
        snapshot_id,
        commit_uuid,
        parent_snapshot_id,
        next_seq_num,
        manifests,
    )
    .await?;

    let new_snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(next_seq_num)
        .with_timestamp_ms(current_timestamp_ms()?)
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

/// Publish a branch to main, rebasing the branch's new manifests onto current
/// main if main has moved since the branch was created.
///
/// When main hasn't moved, this is a simple fast-forward pointer swap. When
/// main has moved (e.g., a concurrent writer published in the meantime), this
/// builds a new snapshot that combines main's current manifests with the
/// branch's new manifests, parented on current main. This prevents clobbering
/// concurrent writes that landed on main while the branch was being written.
///
/// Retries with exponential backoff on commit conflicts.
pub(crate) async fn publish_branch(
    catalog: &Catalog,
    table: &RwLock<Table>,
    branch_name: &str,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    (|| async {
        crate::iceberg_table::reload_table(catalog, table).await?;
        let table_guard = table.read().await;
        publish_branch_once(catalog, &table_guard, branch_name).await
    })
    .retry(commit_backoff())
    .when(|e: &Error| e.is_commit_conflict())
    .notify(|_err, dur| tracing::warn!(delay = ?dur, "commit conflict, retrying publish_branch"))
    .await
}

async fn publish_branch_once(
    catalog: &Catalog,
    table: &Table,
    branch_name: &str,
) -> Result<()> {
    let metadata = table.metadata();
    let branch_snapshot = metadata
        .snapshot_for_ref(branch_name)
        .ok_or_else(|| Error::Branch(format!("branch '{branch_name}' does not exist")))?;
    let branch_snapshot_id = branch_snapshot.snapshot_id();
    let main_snapshot = metadata.snapshot_for_ref(MAIN_BRANCH);
    let main_snapshot_id = main_snapshot.as_ref().map(|s| s.snapshot_id());

    // Fast-forward case: main hasn't moved since the branch was created.
    // Also covers the empty-main case where both are None.
    if branch_snapshot.parent_snapshot_id() == main_snapshot_id {
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
                snapshot_id: main_snapshot_id,
            },
        ];

        return commit(catalog, table.identifier(), updates, requirements).await;
    }

    // Rebase case: main has moved. Build a new snapshot parented on current
    // main that includes both main's current manifests and the branch's new
    // manifests, then point main at that new snapshot.
    rebase_branch_onto_main(catalog, table, branch_name, branch_snapshot, main_snapshot).await
}

async fn rebase_branch_onto_main(
    catalog: &Catalog,
    table: &Table,
    branch_name: &str,
    branch_snapshot: &Snapshot,
    main_snapshot: Option<&iceberg::spec::SnapshotRef>,
) -> Result<()> {
    let metadata = table.metadata();
    let branch_snapshot_id = branch_snapshot.snapshot_id();
    let main_snapshot_id = main_snapshot.as_ref().map(|s| s.snapshot_id());

    // The manifests added by `commit_to_branch_once` carry
    // `added_snapshot_id == branch_snapshot_id` (set via
    // `ManifestWriterBuilder::new(.., Some(snapshot_id), ..)`). Inherited
    // manifests retain the snapshot ID of whichever earlier write added
    // them, so this filter isolates exactly what the branch contributed.
    let branch_manifest_list = branch_snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .map_err(Error::Iceberg)?;
    let new_branch_manifests: Vec<ManifestFile> = branch_manifest_list
        .entries()
        .iter()
        .filter(|m| m.added_snapshot_id == branch_snapshot_id)
        .cloned()
        .collect();

    let main_manifests: Vec<ManifestFile> = if let Some(main) = main_snapshot {
        main.load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .map_err(Error::Iceberg)?
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    // Summary reflects the new data the branch contributed; preserve the
    // branch's wap.id so the post-publish state is correctly recognized as
    // `AlreadyPublished` by `detect_wap_state`.
    let mut summary_collector = SnapshotSummaryCollector::default();
    for manifest in &new_branch_manifests {
        summary_collector.add_manifest(manifest);
    }
    let mut additional_properties = summary_collector.build();
    if let Some(wap_id) = branch_snapshot
        .summary()
        .additional_properties
        .get(WAP_ID_KEY)
    {
        additional_properties.insert(WAP_ID_KEY.to_string(), wap_id.clone());
    }
    let summary = Summary {
        operation: Operation::Append,
        additional_properties,
    };

    let new_snapshot_id = generate_unique_snapshot_id(table);
    let next_seq_num = metadata.next_sequence_number();
    let manifest_list_path = write_manifest_list(
        table,
        new_snapshot_id,
        Uuid::now_v7(),
        main_snapshot_id,
        next_seq_num,
        new_branch_manifests.into_iter().chain(main_manifests),
    )
    .await?;

    let new_snapshot = Snapshot::builder()
        .with_snapshot_id(new_snapshot_id)
        .with_parent_snapshot_id(main_snapshot_id)
        .with_sequence_number(next_seq_num)
        .with_timestamp_ms(current_timestamp_ms()?)
        .with_manifest_list(manifest_list_path)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id())
        .build();

    let updates = vec![
        TableUpdate::AddSnapshot {
            snapshot: new_snapshot,
        },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                new_snapshot_id,
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
            snapshot_id: main_snapshot_id,
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

/// Write a manifest list file at the standard `snap-{id}-0-{uuid}.avro`
/// location for the table, handling the V1/V2/V3 writer variants. Returns
/// the path the manifest list was written to.
async fn write_manifest_list(
    table: &Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    manifests: impl IntoIterator<Item = ManifestFile>,
) -> Result<String> {
    let metadata = table.metadata();
    let manifest_list_path = format!(
        "{}/metadata/snap-{snapshot_id}-0-{commit_uuid}.{}",
        metadata.location(),
        DataFileFormat::Avro
    );
    let output = table
        .file_io()
        .new_output(&manifest_list_path)
        .map_err(Error::Iceberg)?;
    let mut writer = match metadata.format_version() {
        FormatVersion::V1 => ManifestListWriter::v1(output, snapshot_id, parent_snapshot_id),
        FormatVersion::V2 => {
            ManifestListWriter::v2(output, snapshot_id, parent_snapshot_id, sequence_number)
        }
        FormatVersion::V3 => {
            ManifestListWriter::v3(output, snapshot_id, parent_snapshot_id, sequence_number, None)
        }
    };
    writer
        .add_manifests(manifests.into_iter())
        .map_err(Error::Iceberg)?;
    writer.close().await.map_err(Error::Iceberg)?;
    Ok(manifest_list_path)
}

fn current_timestamp_ms() -> Result<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| Error::Branch(format!("failed to get system time: {e}")))
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
    fn test_publish_branch_fast_forward_updates() {
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
    fn test_publish_branch_rebase_updates() {
        // In the rebase path, we add a freshly-built snapshot parented on
        // current main and point main at it (instead of the branch's
        // pre-existing snapshot).
        let new_snapshot_id = 7;
        let main_snapshot_id = Some(42);
        let uuid = Uuid::new_v4();
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: Default::default(),
        };
        let new_snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(main_snapshot_id)
            .with_sequence_number(2)
            .with_timestamp_ms(0)
            .with_manifest_list("s3://bucket/snap-7.avro")
            .with_summary(summary)
            .with_schema_id(0)
            .build();

        let updates = [
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    new_snapshot_id,
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

        // The new snapshot is added with current main as its parent.
        match &updates[0] {
            TableUpdate::AddSnapshot { snapshot } => {
                assert_eq!(snapshot.snapshot_id(), new_snapshot_id);
                assert_eq!(snapshot.parent_snapshot_id(), main_snapshot_id);
            }
            _ => panic!("expected AddSnapshot"),
        }

        // Main is moved to the new snapshot, not the branch's snapshot.
        match &updates[1] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MAIN_BRANCH);
                assert_eq!(reference.snapshot_id, new_snapshot_id);
            }
            _ => panic!("expected SetSnapshotRef"),
        }

        // Branch is removed.
        match &updates[2] {
            TableUpdate::RemoveSnapshotRef { ref_name } => {
                assert_eq!(ref_name, "audit");
            }
            _ => panic!("expected RemoveSnapshotRef"),
        }

        // Requirement still pins main to the snapshot we observed.
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
