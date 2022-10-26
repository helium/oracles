use std::str::FromStr;

use crate::MetaError;

pub async fn save<T>(exec: impl sqlx::PgExecutor<'_>, key: &str, value: T) -> Result<(), MetaError>
where
    T: ToString,
{
    sqlx::query(
        r#"
        insert into meta(key, value)
        values ($1, $2)
        on conflict (key) do update set
        value = EXCLUDED.value
        "#,
    )
    .bind(key)
    .bind(value.to_string())
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn get<T>(exec: impl sqlx::PgExecutor<'_>, key: &str) -> Result<T, MetaError>
where
    T: FromStr,
{
    sqlx::query_scalar::<_, String>(
        r#"
        select value from meta where key = $1
        "#,
    )
    .bind(key)
    .fetch_optional(exec)
    .await?
    .ok_or_else(|| MetaError::NotFound(key.to_string()))
    .and_then(|value| value.parse().map_err(|_| MetaError::DecodeError))
}
