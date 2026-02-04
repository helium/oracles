use crate::catalog::Catalog;
use crate::writer::IcebergTable;
use crate::{Error, Result, Settings};
use iceberg::spec::{NestedField, PartitionSpec, Schema, Transform, Type};
use iceberg::{Catalog as IcebergCatalog, NamespaceIdent, TableCreation};
use std::collections::HashMap;
use std::sync::Arc;

/// Defines a single field (column) in a table schema.
#[derive(Debug, Clone)]
pub struct FieldDefinition {
    name: String,
    field_type: Type,
    required: bool,
    doc: Option<String>,
}

impl FieldDefinition {
    /// Create a new field definition.
    pub fn new(name: impl Into<String>, field_type: Type, required: bool) -> Self {
        Self {
            name: name.into(),
            field_type,
            required,
            doc: None,
        }
    }

    /// Create a required field.
    pub fn required(name: impl Into<String>, field_type: Type) -> Self {
        Self::new(name, field_type, true)
    }

    /// Create an optional field.
    pub fn optional(name: impl Into<String>, field_type: Type) -> Self {
        Self::new(name, field_type, false)
    }

    /// Add documentation to this field.
    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }
}

/// Defines a partition field for a table.
#[derive(Debug, Clone)]
pub struct PartitionDefinition {
    source_name: String,
    partition_name: String,
    transform: Transform,
}

impl PartitionDefinition {
    /// Create a new partition definition.
    pub fn new(
        source_name: impl Into<String>,
        partition_name: impl Into<String>,
        transform: Transform,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            partition_name: partition_name.into(),
            transform,
        }
    }

    /// Create an identity partition (no transformation).
    pub fn identity(source_name: impl Into<String>) -> Self {
        let name = source_name.into();
        Self::new(name.clone(), name, Transform::Identity)
    }

    /// Create a day partition from a timestamp field.
    pub fn day(source_name: impl Into<String>, partition_name: impl Into<String>) -> Self {
        Self::new(source_name, partition_name, Transform::Day)
    }

    /// Create an hour partition from a timestamp field.
    pub fn hour(source_name: impl Into<String>, partition_name: impl Into<String>) -> Self {
        Self::new(source_name, partition_name, Transform::Hour)
    }

    /// Create a month partition from a timestamp field.
    pub fn month(source_name: impl Into<String>, partition_name: impl Into<String>) -> Self {
        Self::new(source_name, partition_name, Transform::Month)
    }

    /// Create a year partition from a timestamp field.
    pub fn year(source_name: impl Into<String>, partition_name: impl Into<String>) -> Self {
        Self::new(source_name, partition_name, Transform::Year)
    }

    /// Create a bucket partition.
    pub fn bucket(
        source_name: impl Into<String>,
        partition_name: impl Into<String>,
        num_buckets: u32,
    ) -> Self {
        Self::new(source_name, partition_name, Transform::Bucket(num_buckets))
    }

    /// Create a truncate partition.
    pub fn truncate(
        source_name: impl Into<String>,
        partition_name: impl Into<String>,
        width: u32,
    ) -> Self {
        Self::new(source_name, partition_name, Transform::Truncate(width))
    }
}

/// A complete table definition including schema, partitioning, and properties.
#[derive(Debug, Clone)]
pub struct TableDefinition {
    name: String,
    fields: Vec<FieldDefinition>,
    partitions: Vec<PartitionDefinition>,
    properties: HashMap<String, String>,
    location: Option<String>,
}

impl TableDefinition {
    /// Create a new table definition builder.
    pub fn builder(name: impl Into<String>) -> TableDefinitionBuilder {
        TableDefinitionBuilder::new(name)
    }

    /// Get the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Build the Iceberg schema from field definitions.
    fn build_schema(&self) -> Result<Schema> {
        let fields: Vec<Arc<NestedField>> = self
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let field_id = (idx + 1) as i32;
                let mut nested = if field.required {
                    NestedField::required(field_id, &field.name, field.field_type.clone())
                } else {
                    NestedField::optional(field_id, &field.name, field.field_type.clone())
                };
                if let Some(ref doc) = field.doc {
                    nested = nested.with_doc(doc);
                }
                Arc::new(nested)
            })
            .collect();

        Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(Error::Iceberg)
    }

    /// Build the partition spec from partition definitions.
    fn build_partition_spec(&self, schema: &Schema) -> Result<PartitionSpec> {
        let schema_ref = Arc::new(schema.clone());
        let mut builder = PartitionSpec::builder(schema_ref);

        for partition in &self.partitions {
            builder = builder
                .add_partition_field(
                    &partition.source_name,
                    partition.partition_name.clone(),
                    partition.transform,
                )
                .map_err(Error::Iceberg)?;
        }

        builder.build().map_err(Error::Iceberg)
    }
}

/// Builder for creating a TableDefinition.
#[derive(Debug, Clone)]
pub struct TableDefinitionBuilder {
    name: String,
    fields: Vec<FieldDefinition>,
    partitions: Vec<PartitionDefinition>,
    properties: HashMap<String, String>,
    location: Option<String>,
}

impl TableDefinitionBuilder {
    /// Create a new builder with the given table name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
            partitions: Vec::new(),
            properties: HashMap::new(),
            location: None,
        }
    }

    /// Add a single field to the table definition.
    pub fn with_field(mut self, field: FieldDefinition) -> Self {
        self.fields.push(field);
        self
    }

    /// Add multiple fields to the table definition.
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = FieldDefinition>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Add a single partition definition.
    pub fn with_partition(mut self, partition: PartitionDefinition) -> Self {
        self.partitions.push(partition);
        self
    }

    /// Add multiple partition definitions.
    pub fn with_partitions(
        mut self,
        partitions: impl IntoIterator<Item = PartitionDefinition>,
    ) -> Self {
        self.partitions.extend(partitions);
        self
    }

    /// Add a table property.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Add multiple table properties.
    pub fn with_properties(
        mut self,
        properties: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (k, v) in properties {
            self.properties.insert(k.into(), v.into());
        }
        self
    }

    /// Set explicit storage location for the table (e.g., s3://bucket/path/to/table).
    /// If not set, the catalog will use default location based on Settings.warehouse.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Build the TableDefinition.
    pub fn build(self) -> Result<TableDefinition> {
        if self.fields.is_empty() {
            return Err(Error::Catalog(
                "table definition must have at least one field".to_string(),
            ));
        }

        Ok(TableDefinition {
            name: self.name,
            fields: self.fields,
            partitions: self.partitions,
            properties: self.properties,
            location: self.location,
        })
    }
}

/// API for creating Iceberg tables using a shared catalog connection.
pub struct TableCreator {
    catalog: Catalog,
}

impl TableCreator {
    /// Create a new TableCreator with the given catalog.
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    /// Create a new TableCreator by connecting to a catalog using the given settings.
    ///
    /// This is a convenience method that combines `Catalog::connect()` with `TableCreator::new()`.
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        Catalog::connect(settings).await.map(Self::new)
    }

    /// Check if a table exists in the given namespace.
    pub async fn table_exists(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<bool> {
        self.catalog.table_exists(namespace, table_name).await
    }

    /// Create a new table in the given namespace.
    /// Returns an IcebergTable ready for immediate use.
    pub async fn create_table(
        &self,
        namespace: impl Into<String>,
        definition: TableDefinition,
    ) -> Result<IcebergTable> {
        let namespace = namespace.into();
        let namespace_ident = NamespaceIdent::new(namespace);

        let schema = definition.build_schema()?;
        let partition_spec = definition.build_partition_spec(&schema)?;

        let table_creation = definition
            .location
            .map(|loc| {
                TableCreation::builder()
                    .name(definition.name.clone())
                    .location(loc)
                    .schema(schema.clone())
                    .partition_spec(partition_spec.clone())
                    .properties(definition.properties.clone())
                    .build()
            })
            .unwrap_or_else(|| {
                TableCreation::builder()
                    .name(definition.name)
                    .schema(schema)
                    .partition_spec(partition_spec)
                    .properties(definition.properties)
                    .build()
            });

        let table = self
            .catalog
            .as_ref()
            .create_table(&namespace_ident, table_creation)
            .await
            .map_err(Error::Iceberg)?;

        Ok(IcebergTable {
            catalog: self.catalog.clone(),
            table,
        })
    }

    /// Create a table if it doesn't exist, otherwise load the existing table.
    /// Returns an IcebergTable ready for immediate use.
    pub async fn create_table_if_not_exists(
        &self,
        namespace: impl Into<String>,
        definition: TableDefinition,
    ) -> Result<IcebergTable> {
        let namespace = namespace.into();
        let table_name = definition.name.clone();

        if self.table_exists(&namespace, &table_name).await? {
            tracing::debug!(
                namespace,
                table_name,
                "table already exists, loading existing table"
            );
            IcebergTable::from_catalog(self.catalog.clone(), &namespace, &table_name).await
        } else {
            tracing::debug!(namespace, table_name, "creating new table");
            self.create_table(namespace, definition).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::PrimitiveType;

    #[test]
    fn test_field_definition_required() {
        let field = FieldDefinition::required("id", Type::Primitive(PrimitiveType::Long));
        assert_eq!(field.name, "id");
        assert!(field.required);
        assert!(field.doc.is_none());
    }

    #[test]
    fn test_field_definition_optional_with_doc() {
        let field = FieldDefinition::optional("email", Type::Primitive(PrimitiveType::String))
            .with_doc("User email address");
        assert_eq!(field.name, "email");
        assert!(!field.required);
        assert_eq!(field.doc.as_deref(), Some("User email address"));
    }

    #[test]
    fn test_partition_definition_identity() {
        let partition = PartitionDefinition::identity("region");
        assert_eq!(partition.source_name, "region");
        assert_eq!(partition.partition_name, "region");
        assert_eq!(partition.transform, Transform::Identity);
    }

    #[test]
    fn test_partition_definition_day() {
        let partition = PartitionDefinition::day("timestamp", "ts_day");
        assert_eq!(partition.source_name, "timestamp");
        assert_eq!(partition.partition_name, "ts_day");
        assert_eq!(partition.transform, Transform::Day);
    }

    #[test]
    fn test_partition_definition_bucket() {
        let partition = PartitionDefinition::bucket("user_id", "user_bucket", 16);
        assert_eq!(partition.source_name, "user_id");
        assert_eq!(partition.partition_name, "user_bucket");
        assert_eq!(partition.transform, Transform::Bucket(16));
    }

    #[test]
    fn test_table_definition_builder() {
        let definition = TableDefinition::builder("events")
            .with_fields([
                FieldDefinition::required("id", Type::Primitive(PrimitiveType::Long)),
                FieldDefinition::required("event_type", Type::Primitive(PrimitiveType::String)),
                FieldDefinition::required("timestamp", Type::Primitive(PrimitiveType::Timestamptz)),
                FieldDefinition::optional("payload", Type::Primitive(PrimitiveType::String)),
            ])
            .with_partition(PartitionDefinition::day("timestamp", "ts_day"))
            .with_property("format-version", "2")
            .with_location("s3://my-bucket/warehouse/nova/events")
            .build()
            .expect("should build successfully");

        assert_eq!(definition.name, "events");
        assert_eq!(definition.fields.len(), 4);
        assert_eq!(definition.partitions.len(), 1);
        assert_eq!(
            definition.properties.get("format-version"),
            Some(&"2".to_string())
        );
        assert_eq!(
            definition.location,
            Some("s3://my-bucket/warehouse/nova/events".to_string())
        );
    }

    #[test]
    fn test_table_definition_builder_no_fields_error() {
        let result = TableDefinition::builder("empty_table").build();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("at least one field"));
    }

    #[test]
    fn test_build_schema() {
        let definition = TableDefinition::builder("test")
            .with_fields([
                FieldDefinition::required("id", Type::Primitive(PrimitiveType::Long)),
                FieldDefinition::optional("name", Type::Primitive(PrimitiveType::String))
                    .with_doc("User name"),
            ])
            .build()
            .expect("should build");

        let schema = definition.build_schema().expect("should build schema");

        assert_eq!(schema.as_struct().fields().len(), 2);

        let id_field = schema.field_by_name("id").expect("id field should exist");
        assert!(id_field.required);
        assert_eq!(id_field.id, 1);

        let name_field = schema
            .field_by_name("name")
            .expect("name field should exist");
        assert!(!name_field.required);
        assert_eq!(name_field.id, 2);
        assert_eq!(name_field.doc.as_deref(), Some("User name"));
    }

    #[test]
    fn test_build_partition_spec() {
        let definition = TableDefinition::builder("test")
            .with_fields([
                FieldDefinition::required("id", Type::Primitive(PrimitiveType::Long)),
                FieldDefinition::required("timestamp", Type::Primitive(PrimitiveType::Timestamptz)),
                FieldDefinition::required("region", Type::Primitive(PrimitiveType::String)),
            ])
            .with_partitions([
                PartitionDefinition::day("timestamp", "ts_day"),
                PartitionDefinition::identity("region"),
            ])
            .build()
            .expect("should build");

        let schema = definition.build_schema().expect("should build schema");
        let partition_spec = definition
            .build_partition_spec(&schema)
            .expect("should build partition spec");

        assert_eq!(partition_spec.fields().len(), 2);
    }

    #[test]
    fn test_partition_spec_missing_source_field() {
        let definition = TableDefinition::builder("test")
            .with_field(FieldDefinition::required(
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))
            .with_partition(PartitionDefinition::day("nonexistent", "ts_day"))
            .build()
            .expect("should build definition");

        let schema = definition.build_schema().expect("should build schema");
        let result = definition.build_partition_spec(&schema);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }
}
