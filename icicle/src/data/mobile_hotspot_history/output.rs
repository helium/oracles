//! The Iceberg table [`MobileHotspotHistory`] rows are written to.

use super::{MobileHotspotHistory, Rejected, DEFAULT_NAMESPACE, DEFAULT_TABLE_NAME};
use glacon_helium::{
    helium_iceberg::{
        self, FieldDefinition, PartitionDefinition, SortFieldDefinition, TableDefinition,
    },
    GlaconOutput,
};

/// The table the route writes to. Defaults to `chain.mobile_hotspot_history`
/// in whichever catalog the output's settings name.
#[derive(Clone, Debug)]
pub struct MobileHotspotHistoryTable {
    namespace: String,
    table_name: String,
}

impl MobileHotspotHistoryTable {
    pub fn new(namespace: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            table_name: table_name.into(),
        }
    }
}

impl Default for MobileHotspotHistoryTable {
    fn default() -> Self {
        Self::new(DEFAULT_NAMESPACE, DEFAULT_TABLE_NAME)
    }
}

impl GlaconOutput<MobileHotspotHistory, Rejected> for MobileHotspotHistoryTable {
    type Row = MobileHotspotHistory;

    /// The columns of `network.chain.mobile_hotspot_history`, as `glacon-rs`
    /// creates it, plus `asserted_lat` and `asserted_lon`. glacon calls
    /// `create_table_if_not_exists`, so this only takes effect on a new
    /// catalog: changes to an existing table are not handled.
    fn table(&self) -> helium_iceberg::Result<TableDefinition> {
        TableDefinition::builder(&self.namespace, &self.table_name)
            .with_fields([
                FieldDefinition::required_long("file_ts"),
                FieldDefinition::required_long("record_index"),
                FieldDefinition::required_timestamptz("received_timestamp"),
                FieldDefinition::required_long("block"),
                FieldDefinition::required_timestamptz("timestamp"),
                FieldDefinition::required_string("pub_key"),
                FieldDefinition::optional_string("animal_name"),
                FieldDefinition::required_string("asset"),
                FieldDefinition::optional_string("serial_number"),
                FieldDefinition::required_string("device_type"),
                FieldDefinition::optional_string("asserted_hex"),
                FieldDefinition::optional_double("asserted_lat")
                    .with_doc("Latitude of the center of asserted_hex, in degrees"),
                FieldDefinition::optional_double("asserted_lon")
                    .with_doc("Longitude of the center of asserted_hex, in degrees"),
                FieldDefinition::optional_int("azimuth"),
                FieldDefinition::required_string("signer"),
            ])
            .with_partition(PartitionDefinition::day("timestamp", "timestamp_day"))
            .with_sort_fields([
                SortFieldDefinition::ascending("pub_key"),
                SortFieldDefinition::ascending("timestamp"),
            ])
            .build()
    }

    /// One row per accepted report: the table is the full event log.
    /// Rejected reports write nothing (the default `to_error_rows`); glacon
    /// counts them in `glacon_middleware_errors_total`.
    fn to_rows(&self, record: &MobileHotspotHistory, rows: &mut Vec<MobileHotspotHistory>) {
        rows.push(record.clone());
    }
}
