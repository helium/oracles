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
            oui: row.try_get::<i64, &str>("oui")? as u64,
            devaddr: row.try_get::<i64, &str>("devaddr")?.into(),
            session_key: row.try_get::<String, &str>("session_key")?,
        })
    }
}

pub async fn list_stream<'a>(
    db: impl sqlx::PgExecutor<'a> + 'a,
) -> impl Stream<Item = SessionKeyFilter> + 'a {
    sqlx::query_as::<_, SessionKeyFilter>("select * from session_key_filters")
        .fetch(db)
        .filter_map(|filter| async move { filter.ok() })
}

pub async fn list_for_oui(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SessionKeyFilter>, sqlx::Error> {
    Ok(sqlx::query_as::<_, SessionKeyFilter>(
        r#"
	select * from session_key_filters
		where oui = $1
	"#,
    )
    .bind(oui as i64)
    .fetch(db)
    .filter_map(|sk| async move { sk.ok() })
    .collect()
    .await)
}

pub async fn list_for_oui_and_devaddr(
    oui: u64,
    devaddr: DevAddrField,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SessionKeyFilter>, sqlx::Error> {
    Ok(sqlx::query_as::<_, SessionKeyFilter>(
        r#"
		select * from session_key_filters
			where oui = $1 and devaddr = $2
		"#,
    )
    .bind(oui as i64)
    .bind(i64::from(devaddr))
    .fetch(db)
    .filter_map(|sk| async move { sk.ok() })
    .collect()
    .await)
}

pub async fn update_session_keys(
    to_add: &[SessionKeyFilter],
    to_remove: &[SessionKeyFilter],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Sender<SessionKeyFilterStreamResV1>,
) -> sqlx::Result<()> {
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
    const SESSION_KEY_FILTER_INSERT_SQL: &str =
        " insert into session_key_filters (oui, devaddr, session_key) ";
    const SESSION_KEY_FILTER_CONFLICT_SQL: &str =
        " on conflict (oui, devaddr, session_key) do nothing ";

    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(SESSION_KEY_FILTER_INSERT_SQL);
    query_builder.push_values(session_key_filters, |mut builder, session_key_filter| {
        builder
            .push_bind(session_key_filter.oui as i64)
            .push_bind(i64::from(session_key_filter.devaddr))
            .push_bind(session_key_filter.session_key.clone());
    });
    query_builder.push(SESSION_KEY_FILTER_CONFLICT_SQL);

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

async fn remove_session_key_filters(
    session_key_filters: &[SessionKeyFilter],
    db: impl sqlx::PgExecutor<'_>,
) -> sqlx::Result<()> {
    const SESSION_KEY_FILTER_DELETE_SQL: &str =
        " delete from session_key_filters where (oui, devaddr, session_key) in ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(SESSION_KEY_FILTER_DELETE_SQL);

    query_builder.push_tuples(session_key_filters, |mut builder, session_key_filter| {
        builder
            .push_bind(session_key_filter.oui as i64)
            .push_bind(i64::from(session_key_filter.devaddr))
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
            session_key: String::from_utf8(value.session_key).expect("utf8 session key"),
        }
    }
}

impl From<SessionKeyFilter> for SessionKeyFilterV1 {
    fn from(value: SessionKeyFilter) -> Self {
        Self {
            oui: value.oui,
            devaddr: value.devaddr.into(),
            session_key: value.session_key.into(),
        }
    }
}

impl From<&SessionKeyFilter> for SessionKeyFilterV1 {
    fn from(value: &SessionKeyFilter) -> Self {
        Self {
            oui: value.oui,
            devaddr: value.devaddr.into(),
            session_key: value.session_key.to_owned().into(),
        }
    }
}
