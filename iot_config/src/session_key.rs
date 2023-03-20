use crate::{broadcast_update, lora_field::DevAddrField};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use helium_proto::services::iot_config::{
    ActionV1, SessionKeyFilterStreamResV1, SessionKeyFilterV1,
};
use sqlx::{postgres::PgRow, FromRow, Row};
use tokio::sync::broadcast::Sender;

#[derive(Clone, Debug)]
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

    let added_updates: Vec<(SessionKeyFilter, ActionV1)> =
        insert_session_key_filters(to_add, &mut transaction)
            .await?
            .into_iter()
            .map(|added_skf| (added_skf, ActionV1::Add))
            .collect();

    let removed_updates: Vec<(SessionKeyFilter, ActionV1)> =
        remove_session_key_filters(to_remove, &mut transaction)
            .await?
            .into_iter()
            .map(|removed_skf| (removed_skf, ActionV1::Remove))
            .collect();

    transaction.commit().await?;

    tokio::spawn(async move {
        stream::iter([added_updates, removed_updates].concat())
            .map(Ok)
            .try_for_each(|(update, action)| {
                broadcast_update::<SessionKeyFilterStreamResV1>(
                    SessionKeyFilterStreamResV1 {
                        action: i32::from(action),
                        filter: Some(update.into()),
                    },
                    update_tx.clone(),
                )
            })
            .await
    });

    Ok(())
}

async fn insert_session_key_filters(
    session_key_filters: &[SessionKeyFilter],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SessionKeyFilter>, sqlx::Error> {
    if session_key_filters.is_empty() {
        return Ok(vec![]);
    }

    const SESSION_KEY_FILTER_INSERT_VALS: &str =
        " insert into session_key_filters (oui, devaddr, session_key) ";
    const SESSION_KEY_FILTER_INSERT_CONFLICT: &str =
        " on conflict (oui, devaddr, session_key) do nothing returning * ";

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

    query_builder
        .build_query_as::<SessionKeyFilter>()
        .fetch_all(db)
        .await
}

async fn remove_session_key_filters(
    session_key_filters: &[SessionKeyFilter],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SessionKeyFilter>, sqlx::Error> {
    if session_key_filters.is_empty() {
        return Ok(vec![]);
    }

    const SESSION_KEY_FILTER_DELETE_VALS: &str =
        " delete from session_key_filters where (oui, devaddr, session_key) in ";
    const SESSION_KEY_FILTER_DELETE_RETURN: &str = " returning * ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(SESSION_KEY_FILTER_DELETE_VALS);
    query_builder
        .push_tuples(session_key_filters, |mut builder, session_key_filter| {
            builder
                .push_bind(session_key_filter.oui as i64)
                .push_bind(i32::from(session_key_filter.devaddr))
                .push_bind(session_key_filter.session_key.clone());
        })
        .push(SESSION_KEY_FILTER_DELETE_RETURN);

    query_builder
        .build_query_as::<SessionKeyFilter>()
        .fetch_all(db)
        .await
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
