use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, IntoBoxedDataWriter};

// `data_transfer` schemas live in `helium-iceberg-oracles`; re-exported here so
// existing `iceberg::*` paths keep resolving.
pub use helium_iceberg_oracles::data_transfer::{
    burned_session, invalid_session, multiplier_ticket_history, session,
    IcebergBurnedDataTransferSession, IcebergDataTransferSession,
    IcebergInvalidDataTransferSession, IcebergMultiplierTicket, NAMESPACE, REASON_COLUMN,
};

// Valid sessions go to `data_transfer.sessions`; rejected sessions go to the
// sibling `data_transfer.invalid_sessions` (same schema plus a `reason` column).
// Burned sessions have no invalid counterpart.
pub type DataTransferWriter = BoxedDataWriter<IcebergDataTransferSession>;
pub type InvalidDataTransferWriter = BoxedDataWriter<IcebergInvalidDataTransferSession>;
pub type BurnedDataTransferWriter = BoxedDataWriter<IcebergBurnedDataTransferSession>;
/// HIP-150: every multiplier ticket seen, accepted or refused.
pub type MultiplierTicketWriter = BoxedDataWriter<IcebergMultiplierTicket>;

/// Every Iceberg writer this service uses.
pub struct Writers {
    pub session: DataTransferWriter,
    pub invalid_session: InvalidDataTransferWriter,
    pub burned_session: BurnedDataTransferWriter,
    pub multiplier_ticket: MultiplierTicketWriter,
}

/// Columns added to `sessions` and `invalid_sessions` after the tables were
/// first created.
const SESSION_KEY_COLUMNS: [&str; 2] = ["file_ts", "record_index"];

pub async fn get_writers(settings: &helium_iceberg::Settings) -> anyhow::Result<Writers> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let session_writer = catalog
        .create_table_if_not_exists(session::table_definition()?)
        .await?;
    let invalid_session_writer = catalog
        .create_table_if_not_exists(invalid_session::table_definition()?)
        .await?;

    // `create_table_if_not_exists` never changes a table that already exists,
    // so these columns reach the live tables only by ALTER TABLE. The writer
    // serializes against the live schema and drops fields it lacks, so a
    // missing column would lose its values without an error.
    for table in [session::TABLE_NAME, invalid_session::TABLE_NAME] {
        require_columns(&catalog, table, &SESSION_KEY_COLUMNS).await?;
    }

    let burned_session_writer = catalog
        .create_table_if_not_exists(burned_session::table_definition()?)
        .await?;
    let multiplier_ticket_writer = catalog
        .create_table_if_not_exists(multiplier_ticket_history::table_definition()?)
        .await?;

    Ok(Writers {
        session: session_writer.boxed(),
        invalid_session: invalid_session_writer.boxed(),
        burned_session: burned_session_writer.boxed(),
        multiplier_ticket: multiplier_ticket_writer.boxed(),
    })
}

async fn require_columns(
    catalog: &helium_iceberg::Catalog,
    table: &str,
    columns: &[&str],
) -> anyhow::Result<()> {
    let live = catalog.load_table(NAMESPACE, table).await?;
    let schema = live.metadata().current_schema();
    let missing: Vec<&str> = columns
        .iter()
        .copied()
        .filter(|column| schema.field_by_name(column).is_none())
        .collect();

    anyhow::ensure!(
        missing.is_empty(),
        "{NAMESPACE}.{table} is missing columns {missing:?}; add them with ALTER TABLE before starting"
    );
    Ok(())
}
