use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, IntoBoxedDataWriter};
use serde::Serialize;

// `data_transfer` schemas live in `helium-iceberg-oracles`; re-exported here so
// existing `iceberg::*` paths keep resolving.
pub use helium_iceberg_oracles::data_transfer::{
    burned_session, invalid_session, multiplier_ticket_history, multiplier_ticket_inventory,
    session, IcebergBurnedDataTransferSession, IcebergDataTransferSession,
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

pub async fn get_writers(settings: &helium_iceberg::Settings) -> anyhow::Result<Writers> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let session_writer = catalog
        .create_table_if_not_exists(session::table_definition()?)
        .await?;
    let invalid_session_writer = catalog
        .create_table_if_not_exists(invalid_session::table_definition()?)
        .await?;
    let burned_session_writer = catalog
        .create_table_if_not_exists(burned_session::table_definition()?)
        .await?;
    let multiplier_ticket_writer = catalog
        .create_table_if_not_exists(multiplier_ticket_history::table_definition()?)
        .await?;

    // The inventory is maintained in place by a Trino MERGE, not by a writer —
    // created here only so the merge has a target. The returned writer is
    // deliberately dropped.
    let _ = catalog
        .create_table_if_not_exists::<multiplier_ticket_inventory::IcebergMultiplierInventory>(
            multiplier_ticket_inventory::table_definition()?,
        )
        .await?;
    Ok(Writers {
        session: session_writer.boxed(),
        invalid_session: invalid_session_writer.boxed(),
        burned_session: burned_session_writer.boxed(),
        multiplier_ticket: multiplier_ticket_writer.boxed(),
    })
}

/// Optional idempotent append — no-op when `writer` is `None` (iceberg
/// writes are optional in some deployments).
pub async fn maybe_write_idempotent<T: Serialize + Send + 'static>(
    writer: Option<&BoxedDataWriter<T>>,
    id: &str,
    records: Vec<T>,
) -> anyhow::Result<()> {
    if let Some(data_writer) = writer {
        data_writer
            .write_idempotent(id, records)
            .await
            .context("writing idempotent")?;
    }
    Ok(())
}
