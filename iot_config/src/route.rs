use crate::{
    broadcast_update,
    lora_field::{DevAddrField, DevAddrRange, EuiPair, NetIdField, Skf},
};
use anyhow::anyhow;
use chrono::Utc;
use file_store::traits::TimestampEncode;
use futures::{
    future::TryFutureExt,
    stream::{self, Stream, StreamExt, TryStreamExt},
};
use helium_crypto::{Keypair, Sign};
use helium_proto::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{types::Uuid, Row};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::broadcast::Sender;

pub mod proto {
    pub use helium_proto::{
        services::iot_config::{
            protocol_http_roaming_v1::FlowTypeV1, route_stream_res_v1, server_v1::Protocol,
            ActionV1, ProtocolGwmpMappingV1, ProtocolGwmpV1, ProtocolHttpRoamingV1,
            ProtocolPacketRouterV1, RouteStreamResV1, RouteV1, ServerV1,
        },
        Message, Region,
    };
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Route {
    pub id: String,
    pub net_id: NetIdField,
    pub oui: u64,
    pub server: RouteServer,
    pub max_copies: u32,
    pub active: bool,
    pub locked: bool,
    pub ignore_empty_skf: bool,
}

impl Route {
    pub fn new(net_id: NetIdField, oui: u64, max_copies: u32) -> Self {
        Self {
            id: "".into(),
            net_id,
            oui,
            server: RouteServer::default(),
            max_copies,
            active: true,
            locked: false,
            ignore_empty_skf: false,
        }
    }

    pub fn set_server(&mut self, server: RouteServer) {
        self.server = server;
    }

    pub fn gwmp_add_mapping(&mut self, map: GwmpMap) -> Result<(), RouteServerError> {
        self.server.gwmp_add_mapping(map)
    }

    pub fn http_update(&mut self, http: Http) -> Result<(), RouteServerError> {
        self.server.http_update(http)
    }

    pub fn set_ignore_empty_skf(&mut self, ignore: bool) {
        self.ignore_empty_skf = ignore;
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct StorageRoute {
    pub id: Uuid,
    pub oui: i64,
    pub net_id: i32,
    pub max_copies: i32,
    pub server_host: String,
    pub server_port: i32,
    pub server_protocol_opts: serde_json::Value,
    pub active: bool,
    pub locked: bool,
    pub ignore_empty_skf: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum RouteStorageError {
    #[error("db persist failed: {0}")]
    StorageError(#[from] sqlx::Error),
    #[error("uuid parse error: {0}")]
    UuidParse(#[from] sqlx::types::uuid::Error),
    #[error("protocol serialize error: {0}")]
    ProtocolSerde(#[from] serde_json::Error),
    #[error("protocol error: {0}")]
    ServerProtocol(String),
}

pub async fn create_route(
    route: Route,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    signing_key: &Keypair,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> anyhow::Result<Route> {
    let net_id: i32 = route.net_id.into();
    let protocol_opts = route
        .server
        .protocol
        .as_ref()
        .ok_or("no protocol defined")
        .map_err(|e| RouteStorageError::ServerProtocol(e.to_string()))?;

    let mut transaction = db.begin().await?;

    let row = sqlx::query(
            r#"
            insert into routes (oui, net_id, max_copies, server_host, server_port, server_protocol_opts, active, ignore_empty_skf)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            returning id
            "#,
        )
        .bind(route.oui as i64)
        .bind(net_id)
        .bind(route.max_copies as i32)
        .bind(&route.server.host)
        .bind(route.server.port as i32)
        .bind(json!(&protocol_opts))
        .bind(route.active)
        .bind(route.ignore_empty_skf)
        .fetch_one(&mut transaction)
        .await?;

    let route_id = row.get::<Uuid, &str>("id").to_string();

    let new_route = get_route(&route_id, &mut transaction).await?;

    transaction.commit().await?;

    let timestamp = Utc::now().encode_timestamp();
    let signer = signing_key.public_key().into();
    let mut update = proto::RouteStreamResV1 {
        action: proto::ActionV1::Add.into(),
        data: Some(proto::route_stream_res_v1::Data::Route(
            new_route.clone().into(),
        )),
        timestamp,
        signer,
        signature: vec![],
    };
    _ = futures::future::ready(signing_key.sign(&update.encode_to_vec()))
        .map_err(|err| {
            tracing::error!(error = ?err, "error signing route create");
            anyhow!("error signing route create")
        })
        .and_then(|signature| {
            update.signature = signature;
            broadcast_update(update, update_tx)
                .map_err(|_| anyhow!("failed broadcasting route create"))
        })
        .await;

    Ok(new_route)
}

pub async fn update_route(
    route: Route,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    signing_key: &Keypair,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> anyhow::Result<Route> {
    let protocol_opts = route
        .server
        .protocol
        .as_ref()
        .ok_or("no protocol defined")
        .map_err(|e| RouteStorageError::ServerProtocol(e.to_string()))?;

    let uuid = Uuid::try_parse(&route.id)?;

    let mut transaction = db.begin().await?;

    sqlx::query(
        r#"
        update routes
        set max_copies = $2, server_host = $3, server_port = $4, server_protocol_opts = $5, active = $6, ignore_empty_skf = $7
        where id = $1
        "#,
    )
    .bind(uuid)
    .bind(route.max_copies as i32)
    .bind(&route.server.host)
    .bind(route.server.port as i32)
    .bind(json!(&protocol_opts))
    .bind(route.active)
    .bind(route.ignore_empty_skf)
    .execute(&mut transaction)
    .await?;

    let updated_route = get_route(&route.id, &mut transaction).await?;

    transaction.commit().await?;

    let timestamp = Utc::now().encode_timestamp();
    let signer = signing_key.public_key().into();
    let mut update_res = proto::RouteStreamResV1 {
        action: proto::ActionV1::Add.into(),
        data: Some(proto::route_stream_res_v1::Data::Route(
            updated_route.clone().into(),
        )),
        timestamp,
        signer,
        signature: vec![],
    };

    _ = futures::future::ready(signing_key.sign(&update_res.encode_to_vec()))
        .map_err(|err| {
            tracing::error!(error = ?err, "error signing route update");
            anyhow!("error signing route update")
        })
        .and_then(|signature| {
            update_res.signature = signature;
            broadcast_update(update_res, update_tx)
                .map_err(|_| anyhow!("failed broadcasting route update"))
        })
        .await;

    Ok(updated_route)
}

async fn insert_euis(
    euis: &[EuiPair],
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<Vec<EuiPair>> {
    if euis.is_empty() {
        return Ok(vec![]);
    }
    // We don't want to take any actions if a route_id cannot be parsed.
    let eui_values = euis
        .iter()
        .map(|eui_pair| eui_pair.try_into())
        .collect::<Result<Vec<(Uuid, i64, i64)>, _>>()?;

    const EUI_INSERT_VALS: &str = " insert into route_eui_pairs (route_id, app_eui, dev_eui) ";
    const EUI_INSERT_ON_CONF: &str =
        " on conflict (route_id, app_eui, dev_eui) do nothing returning * ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(EUI_INSERT_VALS);
    query_builder
        .push_values(eui_values, |mut builder, (id, app_eui, dev_eui)| {
            builder.push_bind(id).push_bind(app_eui).push_bind(dev_eui);
        })
        .push(EUI_INSERT_ON_CONF);

    Ok(query_builder
        .build_query_as::<EuiPair>()
        .fetch_all(db)
        .await?)
}

async fn remove_euis(
    euis: &[EuiPair],
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<Vec<EuiPair>> {
    if euis.is_empty() {
        return Ok(vec![]);
    }
    // We don't want to take any actions if a route_id cannot be parsed.
    let eui_values = euis
        .iter()
        .map(|eui_pair| eui_pair.try_into())
        .collect::<Result<Vec<(Uuid, i64, i64)>, _>>()?;

    const EUI_DELETE_VALS: &str =
        " delete from route_eui_pairs where (route_id, app_eui, dev_eui) in ";
    const EUI_DELETE_RETURNING: &str = " returning * ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(EUI_DELETE_VALS);
    query_builder
        .push_tuples(eui_values, |mut builder, (id, app_eui, dev_eui)| {
            builder.push_bind(id).push_bind(app_eui).push_bind(dev_eui);
        })
        .push(EUI_DELETE_RETURNING);

    Ok(query_builder
        .build_query_as::<EuiPair>()
        .fetch_all(db)
        .await?)
}

pub async fn update_euis(
    to_add: &[EuiPair],
    to_remove: &[EuiPair],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    signing_key: Arc<Keypair>,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> anyhow::Result<()> {
    let mut transaction = db.begin().await?;

    let added_euis: Vec<(EuiPair, proto::ActionV1)> = insert_euis(to_add, &mut transaction)
        .await?
        .into_iter()
        .map(|added_eui| (added_eui, proto::ActionV1::Add))
        .collect();

    let removed_euis: Vec<(EuiPair, proto::ActionV1)> = remove_euis(to_remove, &mut transaction)
        .await?
        .into_iter()
        .map(|removed_eui| (removed_eui, proto::ActionV1::Remove))
        .collect();

    transaction.commit().await?;

    tokio::spawn(async move {
        let timestamp = Utc::now().encode_timestamp();
        let signer: Vec<u8> = signing_key.public_key().into();
        stream::iter([added_euis, removed_euis].concat())
            .map(Ok)
            .try_for_each(|(update, action)| {
                let mut update_res = proto::RouteStreamResV1 {
                    action: i32::from(action),
                    data: Some(proto::route_stream_res_v1::Data::EuiPair(update.into())),
                    timestamp,
                    signer: signer.clone(),
                    signature: vec![],
                };
                futures::future::ready(signing_key.sign(&update_res.encode_to_vec()))
                    .map_err(|_| anyhow!("failed signing eui pair update"))
                    .and_then(|signature| {
                        update_res.signature = signature;
                        broadcast_update::<proto::RouteStreamResV1>(update_res, update_tx.clone())
                            .map_err(|_| anyhow!("failed broadcasting eui pair update"))
                    })
            })
            .await
    });

    Ok(())
}

async fn insert_devaddr_ranges(
    ranges: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<Vec<DevAddrRange>> {
    if ranges.is_empty() {
        return Ok(vec![]);
    }
    // We don't want to take any actions if a route_id cannot be parsed.
    let devaddr_values = ranges
        .iter()
        .map(|range| range.try_into())
        .collect::<Result<Vec<(Uuid, i32, i32)>, _>>()?;

    const DEVADDR_RANGE_INSERT_VALS: &str =
        " insert into route_devaddr_ranges (route_id, start_addr, end_addr) ";
    const DEVADDR_RANGE_INSERT_ON_CONF: &str =
        " on conflict (route_id, start_addr, end_addr) do nothing returning * ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(DEVADDR_RANGE_INSERT_VALS);
    query_builder
        .push_values(devaddr_values, |mut builder, (id, start, end)| {
            builder.push_bind(id).push_bind(start).push_bind(end);
        })
        .push(DEVADDR_RANGE_INSERT_ON_CONF);

    Ok(query_builder
        .build_query_as::<DevAddrRange>()
        .fetch_all(db)
        .await?)
}

async fn remove_devaddr_ranges(
    ranges: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<Vec<DevAddrRange>> {
    if ranges.is_empty() {
        return Ok(vec![]);
    }
    // We don't want to take any actions if a route_id cannot be parsed.
    let devaddr_values = ranges
        .iter()
        .map(|range| range.try_into())
        .collect::<Result<Vec<(Uuid, i32, i32)>, _>>()?;

    const DEVADDR_RANGE_DELETE_VALS: &str =
        " delete from route_devaddr_ranges where (route_id, start_addr, end_addr) in ";
    const DEVADDR_RANGE_DELETE_RETURNING: &str = " returning * ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(DEVADDR_RANGE_DELETE_VALS);

    query_builder
        .push_tuples(devaddr_values, |mut builder, (id, start, end)| {
            builder.push_bind(id).push_bind(start).push_bind(end);
        })
        .push(DEVADDR_RANGE_DELETE_RETURNING);

    Ok(query_builder
        .build_query_as::<DevAddrRange>()
        .fetch_all(db)
        .await?)
}

pub async fn update_devaddr_ranges(
    to_add: &[DevAddrRange],
    to_remove: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    signing_key: Arc<Keypair>,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> anyhow::Result<()> {
    let mut transaction = db.begin().await?;

    let added_devaddrs: Vec<(DevAddrRange, proto::ActionV1)> =
        insert_devaddr_ranges(to_add, &mut transaction)
            .await?
            .into_iter()
            .map(|added_range| (added_range, proto::ActionV1::Add))
            .collect();

    let removed_devaddrs: Vec<(DevAddrRange, proto::ActionV1)> =
        remove_devaddr_ranges(to_remove, &mut transaction)
            .await?
            .into_iter()
            .map(|removed_range| (removed_range, proto::ActionV1::Remove))
            .collect();

    transaction.commit().await?;

    tokio::spawn(async move {
        let timestamp = Utc::now().encode_timestamp();
        let signer: Vec<u8> = signing_key.public_key().into();
        stream::iter([added_devaddrs, removed_devaddrs].concat())
            .map(Ok)
            .try_for_each(|(update, action)| {
                let mut devaddr_res = proto::RouteStreamResV1 {
                    action: i32::from(action),
                    data: Some(proto::route_stream_res_v1::Data::DevaddrRange(
                        update.into(),
                    )),
                    timestamp,
                    signer: signer.clone(),
                    signature: vec![],
                };

                futures::future::ready(signing_key.sign(&devaddr_res.encode_to_vec()))
                    .map_err(|_| anyhow!("failed to sign devaddr range update"))
                    .and_then(|signature| {
                        devaddr_res.signature = signature;
                        broadcast_update::<proto::RouteStreamResV1>(devaddr_res, update_tx.clone())
                            .map_err(|_| anyhow!("failed to broadcast devaddr range update"))
                    })
            })
            .await
    });

    Ok(())
}

pub async fn list_routes(oui: u64, db: impl sqlx::PgExecutor<'_>) -> anyhow::Result<Vec<Route>> {
    Ok(sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts, r.active, r.ignore_empty_skf, o.locked
            from routes r
            join organizations o on r.oui = o.oui
            where o.oui = $1
            group by r.id, o.locked
        "#,
    )
    .bind(oui as i64)
    .fetch(db)
    .map_err(RouteStorageError::from)
    .and_then(|route| async move { Ok(Route {
            id: route.id.to_string(),
            net_id: route.net_id.into(),
            oui: route.oui as u64,
            server: RouteServer::new(route.server_host, route.server_port as u32, serde_json::from_value(route.server_protocol_opts)?),
            max_copies: route.max_copies as u32,
            active: route.active,
            locked: route.locked,
            ignore_empty_skf: route.ignore_empty_skf,
        })})
    .filter_map(|route| async move { route.ok() })
    .collect::<Vec<Route>>()
    .await)
}

pub fn list_euis_for_route<'a>(
    id: &str,
    db: impl sqlx::PgExecutor<'a> + 'a + Copy,
) -> Result<impl Stream<Item = Result<EuiPair, sqlx::Error>> + 'a, RouteStorageError> {
    let id = Uuid::try_parse(id)?;
    const EUI_SELECT_SQL: &str = r#"
    select eui.route_id, eui.app_eui, eui.dev_eui
        from route_eui_pairs eui
        where eui.route_id = $1
    "#;

    Ok(sqlx::query_as::<_, EuiPair>(EUI_SELECT_SQL)
        .bind(id)
        .fetch(db)
        .boxed())
}

pub fn list_devaddr_ranges_for_route<'a>(
    id: &str,
    db: impl sqlx::PgExecutor<'a> + 'a,
) -> Result<impl Stream<Item = Result<DevAddrRange, sqlx::Error>> + 'a, RouteStorageError> {
    let id = Uuid::try_parse(id)?;
    const DEVADDR_RANGE_SELECT_SQL: &str = r#"
    select devaddr.route_id, devaddr.start_addr, devaddr.end_addr
        from route_devaddr_ranges devaddr
        where devaddr.route_id = $1
    "#;

    Ok(sqlx::query_as::<_, DevAddrRange>(DEVADDR_RANGE_SELECT_SQL)
        .bind(id)
        .fetch(db)
        .boxed())
}

pub fn active_route_stream<'a>(
    db: impl sqlx::PgExecutor<'a> + 'a,
) -> impl Stream<Item = Route> + 'a {
    sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts, r.active, r.ignore_empty_skf, o.locked
            from routes r
            join organizations o on r.oui = o.oui
            group by r.id, o.locked
        "#,
    )
    .fetch(db)
    .map_err(RouteStorageError::from)
    .and_then(|route| async move { Ok(Route {
            id: route.id.to_string(),
            net_id: route.net_id.into(),
            oui: route.oui as u64,
            server: RouteServer::new(route.server_host, route.server_port as u32, serde_json::from_value(route.server_protocol_opts)?),
            max_copies: route.max_copies as u32,
            active: route.active,
            locked: route.locked,
            ignore_empty_skf: route.ignore_empty_skf,
        })})
    .filter_map(|route| async move { route.ok() })
    .boxed()
}

pub fn eui_stream<'a>(
    db: impl sqlx::PgExecutor<'a> + 'a + Copy,
) -> impl Stream<Item = EuiPair> + 'a {
    sqlx::query_as::<_, EuiPair>(
        r#"
        select eui.route_id, eui.app_eui, eui.dev_eui
        from route_eui_pairs eui
        "#,
    )
    .fetch(db)
    .map_err(sqlx::Error::from)
    .filter_map(|eui| async move { eui.ok() })
    .boxed()
}

pub fn devaddr_range_stream<'a>(
    db: impl sqlx::PgExecutor<'a> + 'a + Copy,
) -> impl Stream<Item = DevAddrRange> + 'a {
    sqlx::query_as::<_, DevAddrRange>(
        r#"
        select devaddr.route_id, devaddr.start_addr, devaddr.end_addr
        from route_devaddr_ranges devaddr
        "#,
    )
    .fetch(db)
    .map_err(sqlx::Error::from)
    .filter_map(|devaddr| async move { devaddr.ok() })
    .boxed()
}

pub fn skf_stream<'a>(db: impl sqlx::PgExecutor<'a> + 'a + Copy) -> impl Stream<Item = Skf> + 'a {
    sqlx::query_as::<_, Skf>(
        r#"
        select skf.route_id, skf.devaddr, skf.session_key, skf.max_copies
        from route_session_key_filters skf
        "#,
    )
    .fetch(db)
    .filter_map(|skf| async move { skf.ok() })
    .boxed()
}

pub async fn get_route(id: &str, db: impl sqlx::PgExecutor<'_>) -> anyhow::Result<Route> {
    let uuid = Uuid::try_parse(id)?;
    let route = sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts, r.active, r.ignore_empty_skf, o.locked
            from routes r
            join organizations o on r.oui = o.oui
            where r.id = $1
            group by r.id, o.locked
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await?;

    let server = RouteServer::new(
        route.server_host,
        route.server_port as u32,
        serde_json::from_value(route.server_protocol_opts)?,
    );

    Ok(Route {
        id: route.id.to_string(),
        net_id: route.net_id.into(),
        oui: route.oui as u64,
        server,
        max_copies: route.max_copies as u32,
        active: route.active,
        locked: route.locked,
        ignore_empty_skf: route.ignore_empty_skf,
    })
}

pub async fn delete_route(
    id: &str,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    signing_key: &Keypair,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> anyhow::Result<()> {
    let uuid = Uuid::try_parse(id)?;
    let mut transaction = db.begin().await?;

    let route = get_route(id, &mut transaction).await?;

    sqlx::query(
        r#"
        delete from routes
        where id = $1
        "#,
    )
    .bind(uuid)
    .execute(&mut transaction)
    .await?;

    transaction.commit().await?;

    let timestamp = Utc::now().encode_timestamp();
    let signer = signing_key.public_key().into();
    let mut delete_res = proto::RouteStreamResV1 {
        action: proto::ActionV1::Remove.into(),
        data: Some(proto::route_stream_res_v1::Data::Route(
            route.clone().into(),
        )),
        timestamp,
        signer,
        signature: vec![],
    };

    _ = signing_key
        .sign(&delete_res.encode_to_vec())
        .map_err(|_| anyhow!("failed to sign route delete update"))
        .and_then(|signature| {
            delete_res.signature = signature;
            update_tx
                .send(delete_res)
                .map_err(|_| anyhow!("failed to broadcast route delete update"))
        });

    Ok(())
}

pub fn list_skfs_for_route<'a>(
    id: &str,
    db: impl sqlx::PgExecutor<'a> + 'a + Copy,
) -> Result<impl Stream<Item = Result<Skf, sqlx::Error>> + 'a, RouteStorageError> {
    let id = Uuid::try_parse(id)?;
    const SKF_SELECT_SQL: &str = r#"
        select skf.route_id, skf.devaddr, skf.session_key, skf.max_copies
            from route_session_key_filters skf
            where skf.route_id = $1
    "#;

    Ok(sqlx::query_as::<_, Skf>(SKF_SELECT_SQL)
        .bind(id)
        .fetch(db)
        .boxed())
}

pub fn list_skfs_for_route_and_devaddr<'a>(
    id: &str,
    devaddr: DevAddrField,
    db: impl sqlx::PgExecutor<'a> + 'a + Copy,
) -> Result<impl Stream<Item = Result<Skf, sqlx::Error>> + 'a, RouteStorageError> {
    let id = Uuid::try_parse(id)?;

    Ok(sqlx::query_as::<_, Skf>(
        r#"
        select skf.route_id, skf.devaddr, skf.session_key, skf.max_copies
        from route_session_key_filters skf
        where skf.route_id = $1 and devaddr = $2
        "#,
    )
    .bind(id)
    .bind(i32::from(devaddr))
    .fetch(db)
    .boxed())
}

pub async fn update_skfs(
    to_add: &[Skf],
    to_remove: &[Skf],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    signing_key: Arc<Keypair>,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> anyhow::Result<()> {
    let mut transaction = db.begin().await?;

    // Always process removes before adds to ensure updating existing values doesn't result in
    // removing a value that was just added
    let removed_updates: Vec<(Skf, proto::ActionV1)> = remove_skfs(to_remove, &mut transaction)
        .await?
        .into_iter()
        .map(|removed_skf| (removed_skf, proto::ActionV1::Remove))
        .collect();

    let added_updates: Vec<(Skf, proto::ActionV1)> = insert_skfs(to_add, &mut transaction)
        .await?
        .into_iter()
        .map(|added_skf| (added_skf, proto::ActionV1::Add))
        .collect();

    transaction.commit().await?;

    tokio::spawn(async move {
        let timestamp = Utc::now().encode_timestamp();
        let signer: Vec<u8> = signing_key.public_key().into();
        stream::iter([added_updates, removed_updates].concat())
            .map(Ok)
            .try_for_each(|(update, action)| {
                let mut skf_update = proto::RouteStreamResV1 {
                    action: i32::from(action),
                    data: Some(proto::route_stream_res_v1::Data::Skf(update.into())),
                    timestamp,
                    signer: signer.clone(),
                    signature: vec![],
                };
                futures::future::ready(signing_key.sign(&skf_update.encode_to_vec()))
                    .map_err(|_| anyhow!("failed to sign session key filter update"))
                    .and_then(|signature| {
                        skf_update.signature = signature;
                        broadcast_update::<proto::RouteStreamResV1>(skf_update, update_tx.clone())
                            .map_err(|_| anyhow!("failed to broadcast session key filter update"))
                    })
            })
            .await
    });

    Ok(())
}

async fn insert_skfs(skfs: &[Skf], db: impl sqlx::PgExecutor<'_>) -> anyhow::Result<Vec<Skf>> {
    if skfs.is_empty() {
        return Ok(vec![]);
    }

    let skfs = skfs
        .iter()
        .map(|filter| filter.try_into())
        .collect::<Result<Vec<(Uuid, i32, String, i32)>, _>>()?;

    const SKF_INSERT_VALS: &str =
        " insert into route_session_key_filters (route_id, devaddr, session_key, max_copies) ";
    const SKF_INSERT_CONFLICT: &str =
        " on conflict (route_id, devaddr, session_key) update set max_copies = excluded.max_copies returning * ";

    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(SKF_INSERT_VALS);
    query_builder
        .push_values(
            skfs,
            |mut builder, (route_id, devaddr, session_key, max_copies)| {
                builder
                    .push_bind(route_id)
                    .push_bind(devaddr)
                    .push_bind(session_key)
                    .push_bind(max_copies);
            },
        )
        .push(SKF_INSERT_CONFLICT);

    Ok(query_builder.build_query_as::<Skf>().fetch_all(db).await?)
}

async fn remove_skfs(skfs: &[Skf], db: impl sqlx::PgExecutor<'_>) -> anyhow::Result<Vec<Skf>> {
    if skfs.is_empty() {
        return Ok(vec![]);
    }

    let skfs = skfs
        .iter()
        .map(|filter| filter.try_into())
        .collect::<Result<Vec<(Uuid, i32, String, i32)>, _>>()?;

    const SKF_DELETE_VALS: &str =
        " delete from route_session_key_filters where (route_id, devaddr, session_key) in ";
    const SKF_DELETE_RETURN: &str = " returning * ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(SKF_DELETE_VALS);
    query_builder
        .push_tuples(
            skfs,
            |mut builder, (route_id, devaddr, session_key, _max_copies)| {
                builder
                    .push_bind(route_id)
                    .push_bind(devaddr)
                    .push_bind(session_key);
            },
        )
        .push(SKF_DELETE_RETURN);

    Ok(query_builder.build_query_as::<Skf>().fetch_all(db).await?)
}

#[derive(Debug, Serialize)]
pub struct RouteList {
    routes: Vec<Route>,
}

impl RouteList {
    pub fn count(&self) -> usize {
        self.routes.len()
    }
}

impl From<proto::RouteV1> for Route {
    fn from(route: proto::RouteV1) -> Self {
        let net_id: NetIdField = route.net_id.into();
        Self {
            id: route.id,
            net_id,
            oui: route.oui,
            server: route.server.map_or_else(RouteServer::default, |s| s.into()),
            max_copies: route.max_copies,
            active: route.active,
            locked: route.locked,
            ignore_empty_skf: route.ignore_empty_skf,
        }
    }
}

impl From<Route> for proto::RouteV1 {
    fn from(route: Route) -> Self {
        Self {
            id: route.id,
            net_id: route.net_id.into(),
            oui: route.oui,
            server: Some(route.server.into()),
            max_copies: route.max_copies,
            active: route.active,
            locked: route.locked,
            ignore_empty_skf: route.ignore_empty_skf,
        }
    }
}

impl TryFrom<&EuiPair> for (Uuid, i64, i64) {
    type Error = sqlx::types::uuid::Error;

    fn try_from(eui: &EuiPair) -> Result<(Uuid, i64, i64), Self::Error> {
        let uuid = Uuid::try_parse(&eui.route_id)?;
        Ok((uuid, i64::from(eui.app_eui), i64::from(eui.dev_eui)))
    }
}

impl TryFrom<&DevAddrRange> for (Uuid, i32, i32) {
    type Error = sqlx::types::uuid::Error;

    fn try_from(devaddr: &DevAddrRange) -> Result<(Uuid, i32, i32), Self::Error> {
        let uuid = Uuid::try_parse(&devaddr.route_id)?;
        Ok((
            uuid,
            i32::from(devaddr.start_addr),
            i32::from(devaddr.end_addr),
        ))
    }
}

impl TryFrom<&Skf> for (Uuid, i32, String, i32) {
    type Error = sqlx::types::uuid::Error;

    fn try_from(skf: &Skf) -> Result<(Uuid, i32, String, i32), Self::Error> {
        let uuid = Uuid::try_parse(&skf.route_id)?;
        Ok((
            uuid,
            i32::from(skf.devaddr),
            skf.session_key.clone(),
            skf.max_copies as i32,
        ))
    }
}

pub type Port = u32;
pub type GwmpMap = BTreeMap<proto::Region, Port>;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteServer {
    pub host: String,
    pub port: Port,
    pub protocol: Option<Protocol>,
}

impl RouteServer {
    pub fn new(host: String, port: Port, protocol: Protocol) -> Self {
        Self {
            host,
            port,
            protocol: Some(protocol),
        }
    }

    pub fn gwmp_add_mapping(&mut self, map: GwmpMap) -> Result<(), RouteServerError> {
        if let Some(ref mut protocol) = self.protocol {
            return protocol.gwmp_add_mapping(map);
        }
        Err(RouteServerError::NoProtocol("gwmp".to_string()))
    }

    pub fn http_update(&mut self, http: Http) -> Result<(), RouteServerError> {
        if let Some(ref mut protocol) = self.protocol {
            return protocol.http_update(http);
        }
        Err(RouteServerError::NoProtocol("http".to_string()))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Protocol {
    Gwmp(Gwmp),
    Http(Http),
    PacketRouter,
}

#[derive(thiserror::Error, Debug)]
pub enum RouteServerError {
    #[error("route server has no protocol error: {0}")]
    NoProtocol(String),
    #[error("route server region mapping add incompatible: {0}")]
    RegionMappingFailure(String),
    #[error("route server http details update incompatible: {0}")]
    HttpDetailsFailure(String),
}

impl Protocol {
    pub fn default_gwmp() -> Self {
        Protocol::Gwmp(Gwmp::default())
    }

    pub fn default_http() -> Self {
        Protocol::Http(Http::default())
    }

    pub fn default_packet_router() -> Self {
        Protocol::PacketRouter
    }

    pub fn make_gwmp_mapping(region: proto::Region, port: Port) -> GwmpMap {
        BTreeMap::from([(region, port)])
    }

    pub fn make_http(
        flow_type: FlowType,
        dedupe_timeout: u32,
        path: String,
        auth_header: String,
        receiver_nsid: String,
    ) -> Self {
        Self::Http(Http {
            flow_type,
            dedupe_timeout,
            path,
            auth_header,
            receiver_nsid,
        })
    }

    pub fn make_gwmp(region: proto::Region, port: Port) -> Result<Self, RouteServerError> {
        let mut gwmp = Self::default_gwmp();
        gwmp.gwmp_add_mapping(Self::make_gwmp_mapping(region, port))?;
        Ok(gwmp)
    }

    fn gwmp_add_mapping(&mut self, map: GwmpMap) -> Result<(), RouteServerError> {
        match self {
            Protocol::Gwmp(Gwmp { ref mut mapping }) => {
                mapping.extend(map);
                Ok(())
            }
            Protocol::Http(_) => Err(RouteServerError::RegionMappingFailure("http".to_string())),
            Protocol::PacketRouter => Err(RouteServerError::RegionMappingFailure(
                "packet router".to_string(),
            )),
        }
    }

    fn http_update(&mut self, http: Http) -> Result<(), RouteServerError> {
        match self {
            Protocol::Http(_) => {
                *self = Protocol::Http(http);
                Ok(())
            }
            Protocol::Gwmp(_) => Err(RouteServerError::HttpDetailsFailure("gwmp".to_string())),
            Protocol::PacketRouter => Err(RouteServerError::HttpDetailsFailure(
                "packet router".to_string(),
            )),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Gwmp {
    pub mapping: GwmpMap,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Http {
    flow_type: FlowType,
    dedupe_timeout: u32,
    path: String,
    auth_header: String,
    receiver_nsid: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FlowType {
    #[default]
    Sync,
    Async,
}

#[derive(thiserror::Error, Debug)]
#[error("unsupported flow type {0}")]
pub struct UnsupportedFlowTypeError(i32);

impl FlowType {
    fn from_i32(v: i32) -> Result<Self, UnsupportedFlowTypeError> {
        proto::FlowTypeV1::from_i32(v)
            .map(|ft| ft.into())
            .ok_or(UnsupportedFlowTypeError(v))
    }
}

impl From<proto::FlowTypeV1> for FlowType {
    fn from(ft: proto::FlowTypeV1) -> Self {
        match ft {
            proto::FlowTypeV1::Sync => Self::Sync,
            proto::FlowTypeV1::Async => Self::Async,
        }
    }
}

impl From<FlowType> for proto::FlowTypeV1 {
    fn from(ft: FlowType) -> Self {
        match ft {
            FlowType::Sync => Self::Sync,
            FlowType::Async => Self::Async,
        }
    }
}

impl From<RouteServer> for proto::ServerV1 {
    fn from(server: RouteServer) -> Self {
        Self {
            host: server.host,
            port: server.port,
            protocol: server.protocol.map(|p| p.into()),
        }
    }
}

impl From<proto::ServerV1> for RouteServer {
    fn from(server: proto::ServerV1) -> Self {
        Self {
            host: server.host,
            port: server.port,
            protocol: server.protocol.map(|p| p.into()),
        }
    }
}

impl From<Protocol> for proto::Protocol {
    fn from(protocol: Protocol) -> Self {
        match protocol {
            Protocol::Gwmp(gwmp) => {
                let mut mapping = vec![];
                for (region, port) in gwmp.mapping.into_iter() {
                    mapping.push(proto::ProtocolGwmpMappingV1 {
                        region: region.into(),
                        port,
                    })
                }
                proto::Protocol::Gwmp(proto::ProtocolGwmpV1 { mapping })
            }
            Protocol::Http(http) => proto::Protocol::HttpRoaming(proto::ProtocolHttpRoamingV1 {
                flow_type: proto::FlowTypeV1::from(http.flow_type) as i32,
                dedupe_timeout: http.dedupe_timeout,
                path: http.path,
                auth_header: http.auth_header,
                receiver_nsid: http.receiver_nsid,
            }),
            Protocol::PacketRouter => {
                proto::Protocol::PacketRouter(proto::ProtocolPacketRouterV1 {})
            }
        }
    }
}

impl From<proto::Protocol> for Protocol {
    fn from(proto: proto::Protocol) -> Self {
        match proto {
            proto::Protocol::Gwmp(gwmp) => {
                let mut mapping = BTreeMap::new();
                for entry in gwmp.mapping {
                    let region =
                        proto::Region::from_i32(entry.region).unwrap_or(proto::Region::Us915);
                    mapping.insert(region, entry.port);
                }
                Protocol::Gwmp(Gwmp { mapping })
            }
            proto::Protocol::HttpRoaming(http) => Protocol::Http(Http {
                flow_type: FlowType::from_i32(http.flow_type).unwrap_or(FlowType::Sync),
                dedupe_timeout: http.dedupe_timeout,
                path: http.path,
                auth_header: http.auth_header,
                receiver_nsid: http.receiver_nsid,
            }),
            proto::Protocol::PacketRouter(_args) => Protocol::PacketRouter,
        }
    }
}
