use helium_proto::{BlockchainRegionParamsV1, Message};

pub async fn persist_params(
    region: i32,
    params: BlockchainRegionParamsV1,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into regions (region, params)
        values ($1, $2)
        on conflict (region) do update set params = excluded.params
        "#,
    )
    .bind(region)
    .bind(params.encode_to_vec())
    .execute(db)
    .await?;

    Ok(())
}

pub async fn persist_indexes(
    region: i32,
    indexes: &[u8],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into regions (region, indexes)
        values ($1, $2)
        on conflict (region) do update set indexes = excluded.indexes
        "#,
    )
    .bind(region)
    .bind(indexes)
    .execute(db)
    .await?;

    Ok(())
}
