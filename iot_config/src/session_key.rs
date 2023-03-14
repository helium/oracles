use crate::lora_field::DevAddrField;
use futures::stream::{Stream, StreamExt};
use helium_proto::services::iot_config::{
    ActionV1, SessionKeyFilterStreamResV1, SessionKeyFilterV1,
};
use sqlx::{postgres::PgRow, FromRow, Row};
use tokio::sync::broadcast::Sender;

#[derive(Debug)]
pub struct SessionKeyFilter {
    pub oui: u64,
    pub devaddr: DevAddrField,
    pub session_key: String,
}

impl FromRow<'_, PgRow> for SessionKeyFilter {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            oui: row.get::<i64, &str>("oui") as u64,
            devaddr: row.get::<i32, &str>("devaddr").into(),
            session_key: row.get::<String, &str>("session_key"),
        })
    }
}

pub fn list_stream<'a>(
    db: impl sqlx::PgExecutor<'a> + 'a,
) -> impl Stream<Item = SessionKeyFilter> + 'a {
    sqlx::query_as::<_, SessionKeyFilter>(r#" select * from session_key_filters "#)
        .fetch(db)
        .filter_map(|filter| async move { filter.ok() })
        .boxed()
}

pub fn list_for_oui<'a>(
    oui: u64,
    db: impl sqlx::PgExecutor<'a> + 'a,
) -> impl Stream<Item = Result<SessionKeyFilter, sqlx::Error>> + 'a {
    sqlx::query_as::<_, SessionKeyFilter>(
        r#"
	    select * from session_key_filters
		where oui = $1
	    "#,
    )
    .bind(oui as i64)
    .fetch(db)
    .boxed()
}

pub fn list_for_oui_and_devaddr<'a>(
    oui: u64,
    devaddr: DevAddrField,
    db: impl sqlx::PgExecutor<'a> + 'a,
) -> impl Stream<Item = Result<SessionKeyFilter, sqlx::Error>> + 'a {
    sqlx::query_as::<_, SessionKeyFilter>(
        r#"
		select * from session_key_filters
		where oui = $1 and devaddr = $2
		"#,
    )
    .bind(oui as i64)
    .bind(i32::from(devaddr))
    .fetch(db)
    .boxed()
}

pub async fn update_session_keys(
    to_add: &[SessionKeyFilter],
    to_remove: &[SessionKeyFilter],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Sender<SessionKeyFilterStreamResV1>,
) -> Result<(), sqlx::Error> {
    let mut transaction = db.begin().await?;

    if !to_add.is_empty() {
        insert_session_key_filters(to_add, &mut transaction).await?;
    }

    if !to_remove.is_empty() {
        remove_session_key_filters(to_remove, &mut transaction).await?;
    }

    transaction.commit().await?;

    for added in to_add {
        let update = SessionKeyFilterStreamResV1 {
            action: ActionV1::Add.into(),
            filter: Some(added.into()),
        };
        if update_tx.send(update).is_err() {
            break;
        }
    }

    for removed in to_remove {
        let update = SessionKeyFilterStreamResV1 {
            action: ActionV1::Remove.into(),
            filter: Some(removed.into()),
        };
        if update_tx.send(update).is_err() {
            break;
        }
    }

    Ok(())
}

async fn insert_session_key_filters(
    session_key_filters: &[SessionKeyFilter],
    db: impl sqlx::PgExecutor<'_>,
) -> sqlx::Result<()> {
    const SESSION_KEY_FILTER_INSERT_VALS: &str =
        " insert into session_key_filters (oui, devaddr, session_key) ";
    const SESSION_KEY_FILTER_INSERT_CONFLICT: &str =
        " on conflict (oui, devaddr, session_key) do nothing ";

    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(SESSION_KEY_FILTER_INSERT_VALS);
    query_builder
        .push_values(session_key_filters, |mut builder, session_key_filter| {
            builder
                .push_bind(session_key_filter.oui as i64)
                .push_bind(i32::from(session_key_filter.devaddr))
                .push_bind(session_key_filter.session_key.clone());
        })
        .push(SESSION_KEY_FILTER_INSERT_CONFLICT);

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

async fn remove_session_key_filters(
    session_key_filters: &[SessionKeyFilter],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    const SESSION_KEY_FILTER_DELETE_SQL: &str =
        " delete from session_key_filters where (oui, devaddr, session_key) in ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(SESSION_KEY_FILTER_DELETE_SQL);

    query_builder.push_tuples(session_key_filters, |mut builder, session_key_filter| {
        builder
            .push_bind(session_key_filter.oui as i64)
            .push_bind(i32::from(session_key_filter.devaddr))
            .push_bind(session_key_filter.session_key.clone());
    });

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

impl From<SessionKeyFilterV1> for SessionKeyFilter {
    fn from(value: SessionKeyFilterV1) -> Self {
        Self {
            oui: value.oui,
            devaddr: value.devaddr.into(),
            session_key: value.session_key,
        }
    }
}

impl From<&SessionKeyFilterV1> for SessionKeyFilter {
    fn from(value: &SessionKeyFilterV1) -> Self {
        Self {
            oui: value.oui,
            devaddr: value.devaddr.into(),
            session_key: value.session_key.to_owned(),
        }
    }
}

impl From<SessionKeyFilter> for SessionKeyFilterV1 {
    fn from(value: SessionKeyFilter) -> Self {
        Self {
            oui: value.oui,
            devaddr: value.devaddr.into(),
            session_key: value.session_key,
        }
    }
}

impl From<&SessionKeyFilter> for SessionKeyFilterV1 {
    fn from(value: &SessionKeyFilter) -> Self {
        Self {
            oui: value.oui,
            devaddr: value.devaddr.into(),
            session_key: value.session_key.to_owned(),
        }
    }
}
