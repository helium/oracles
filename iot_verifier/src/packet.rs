pub async fn insert<'c, E>(
    executor: E,
    payload_size: i32,
    gateway: Vec<u8>,
    payload_hash: Vec<u8>,
) -> anyhow::Result<()>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    sqlx::query(
        r#"
            insert into packet(payload_size, gateway, payload_hash)
            values ($1, $2, $3)
            "#,
    )
    .bind(payload_size)
    .bind(gateway)
    .bind(payload_hash)
    .execute(executor)
    .await?;
    Ok(())
}
