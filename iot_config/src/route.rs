use crate::lora_field::{DevAddrRange, EuiPair, NetIdField};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{types::Uuid, Row};
use std::collections::BTreeMap;
use tokio::sync::broadcast::{self, Sender};

pub mod proto {
    pub use helium_proto::{
        services::iot_config::{
            protocol_http_roaming_v1::FlowTypeV1, route_stream_res_v1, server_v1::Protocol,
            ActionV1, ProtocolGwmpMappingV1, ProtocolGwmpV1, ProtocolHttpRoamingV1,
            ProtocolPacketRouterV1, RouteStreamResV1, RouteV1, ServerV1,
        },
        Region,
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
}

#[derive(thiserror::Error, Debug)]
pub enum RouteStorageError {
    #[error("db persist failed: {0}")]
    PersistError(#[from] sqlx::Error),
    #[error("uuid parse error: {0}")]
    UuidParse(#[from] sqlx::types::uuid::Error),
    #[error("protocol serialize error: {0}")]
    ProtocolSerde(#[from] serde_json::Error),
    #[error("protocol error: {0}")]
    ServerProtocol(String),
    #[error("stream update error: {0}")]
    StreamUpdate(#[from] Box<broadcast::error::SendError<proto::RouteStreamResV1>>),
}

pub async fn create_route(
    route: Route,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> Result<Route, RouteStorageError> {
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
            insert into routes (oui, net_id, max_copies, server_host, server_port, server_protocol_opts, active)
            values ($1, $2, $3, $4, $5, $6, $7)
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
        .fetch_one(&mut transaction)
        .await?;

    let route_id = row.get::<Uuid, &str>("id").to_string();

    let new_route = get_route(&route_id, &mut transaction).await?;

    transaction.commit().await?;

    if new_route.active && !new_route.locked {
        _ = update_tx.send(proto::RouteStreamResV1 {
            action: proto::ActionV1::Add.into(),
            data: Some(proto::route_stream_res_v1::Data::Route(
                new_route.clone().into(),
            )),
        });
    };

    Ok(new_route)
}

pub async fn update_route(
    route: Route,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> Result<Route, RouteStorageError> {
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
        set max_copies = $2, server_host = $3, server_port = $4, server_protocol_opts = $5, active = $6
        where id = $1
        "#,
    )
    .bind(uuid)
    .bind(route.max_copies as i32)
    .bind(&route.server.host)
    .bind(route.server.port as i32)
    .bind(json!(&protocol_opts))
    .bind(route.active)
    .execute(&mut transaction)
    .await?;

    let updated_route = get_route(&route.id, &mut transaction).await?;

    transaction.commit().await?;

    _ = update_tx.send(proto::RouteStreamResV1 {
        action: proto::ActionV1::Add.into(),
        data: Some(proto::route_stream_res_v1::Data::Route(
            updated_route.clone().into(),
        )),
    });

    Ok(updated_route)
}

async fn insert_euis(
    euis: &[EuiPair],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), RouteStorageError> {
    // We don't want to take any actions if a route_id cannot be parsed.
    let mut eui_values = vec![];
    for eui in euis.iter() {
        eui_values.push((
            Uuid::try_parse(&eui.route_id)?,
            i64::from(eui.app_eui),
            i64::from(eui.dev_eui),
        ));
    }

    const EUI_INSERT_VALS: &str = " insert into route_eui_pairs (route_id, app_eui, dev_eui) ";
    const EUI_INSERT_ON_CONF: &str = " on conflict (route_id, app_eui, dev_eui) do nothing ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(EUI_INSERT_VALS);
    query_builder
        .push_values(eui_values, |mut builder, (id, app_eui, dev_eui)| {
            builder.push_bind(id).push_bind(app_eui).push_bind(dev_eui);
        })
        .push(EUI_INSERT_ON_CONF);

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

async fn remove_euis(
    euis: &[EuiPair],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), RouteStorageError> {
    // We don't want to take any actions if a route_id cannot be parsed.
    let mut eui_values = vec![];
    for eui in euis.iter() {
        eui_values.push((
            Uuid::try_parse(&eui.route_id)?,
            i64::from(eui.app_eui),
            i64::from(eui.dev_eui),
        ));
    }

    const EUI_DELETE_SQL: &str =
        " delete from route_eui_pairs where (route_id, app_eui, dev_eui) in ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(EUI_DELETE_SQL);
    query_builder.push_tuples(eui_values, |mut builder, (id, app_eui, dev_eui)| {
        builder.push_bind(id).push_bind(app_eui).push_bind(dev_eui);
    });

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

pub async fn update_euis(
    to_add: &[EuiPair],
    to_remove: &[EuiPair],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> Result<(), RouteStorageError> {
    let mut transaction = db.begin().await?;

    if !to_add.is_empty() {
        insert_euis(to_add, &mut transaction).await?;
    }

    if !to_remove.is_empty() {
        remove_euis(to_remove, &mut transaction).await?;
    }

    transaction.commit().await?;

    for added in to_add {
        let update = proto::RouteStreamResV1 {
            action: proto::ActionV1::Add.into(),
            data: Some(proto::route_stream_res_v1::Data::EuiPair(added.into())),
        };
        if update_tx.send(update).is_err() {
            break;
        }
    }

    for removed in to_remove {
        let update = proto::RouteStreamResV1 {
            action: proto::ActionV1::Remove.into(),
            data: Some(proto::route_stream_res_v1::Data::EuiPair(removed.into())),
        };
        if update_tx.send(update).is_err() {
            break;
        }
    }

    Ok(())
}

async fn insert_devaddr_ranges(
    ranges: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), RouteStorageError> {
    // We don't want to take any actions if a route_id cannot be parsed.
    let mut devaddr_values = vec![];
    for devaddr in ranges {
        devaddr_values.push((
            Uuid::try_parse(&devaddr.route_id)?,
            i32::from(devaddr.start_addr),
            i32::from(devaddr.end_addr),
        ));
    }

    const DEVADDR_RANGE_INSERT_VALS: &str =
        " insert into route_devaddr_ranges (route_id, start_addr, end_addr) ";
    const DEVADDR_RANGE_INSERT_ON_CONF: &str =
        " on conflict (route_id, start_addr, end_addr) do nothing ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(DEVADDR_RANGE_INSERT_VALS);
    query_builder
        .push_values(devaddr_values, |mut builder, (id, start, end)| {
            builder.push_bind(id).push_bind(start).push_bind(end);
        })
        .push(DEVADDR_RANGE_INSERT_ON_CONF);

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

async fn remove_devaddr_ranges(
    ranges: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), RouteStorageError> {
    // We don't want to take any actions if a route_id cannot be parsed.
    let mut devaddr_values = vec![];
    for devaddr in ranges {
        devaddr_values.push((
            Uuid::try_parse(&devaddr.route_id)?,
            i32::from(devaddr.start_addr),
            i32::from(devaddr.end_addr),
        ));
    }

    const DEVADDR_RANGE_DELETE_SQL: &str =
        " delete from route_devaddr_ranges where (route_id, start_addr, end_addr) in ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(DEVADDR_RANGE_DELETE_SQL);

    query_builder.push_tuples(devaddr_values, |mut builder, (id, start, end)| {
        builder.push_bind(id).push_bind(start).push_bind(end);
    });

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

pub async fn update_devaddr_ranges(
    to_add: &[DevAddrRange],
    to_remove: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> Result<(), RouteStorageError> {
    let mut transaction = db.begin().await?;

    if !to_add.is_empty() {
        insert_devaddr_ranges(to_add, &mut transaction).await?;
    }

    if !to_remove.is_empty() {
        remove_devaddr_ranges(to_remove, &mut transaction).await?;
    }

    transaction.commit().await?;

    for added in to_add {
        let update = proto::RouteStreamResV1 {
            action: proto::ActionV1::Add.into(),
            data: Some(proto::route_stream_res_v1::Data::DevaddrRange(added.into())),
        };
        if update_tx.send(update).is_err() {
            break;
        }
    }

    for removed in to_remove {
        let update = proto::RouteStreamResV1 {
            action: proto::ActionV1::Remove.into(),
            data: Some(proto::route_stream_res_v1::Data::DevaddrRange(
                removed.into(),
            )),
        };
        if update_tx.send(update).is_err() {
            break;
        }
    }

    Ok(())
}

pub async fn list_routes(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<Route>, RouteStorageError> {
    Ok(sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts, r.active, o.locked
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
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts, r.active, o.locked
            from routes r
            join organizations o on r.oui = o.oui
            where o.locked = false and r.active = true
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

pub async fn get_route(
    id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Route, RouteStorageError> {
    let uuid = Uuid::try_parse(id)?;
    let route_row = sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts, r.active, o.locked
            from routes r
            join organizations o on r.oui = o.oui
            where r.id = $1
            group by r.id, o.locked
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await;

    tracing::debug!("Result Route Row: {route_row:?}");
    let route = route_row?;

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
    })
}

pub async fn delete_route(
    id: &str,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Sender<proto::RouteStreamResV1>,
) -> Result<(), RouteStorageError> {
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

    _ = update_tx.send(proto::RouteStreamResV1 {
        action: proto::ActionV1::Remove.into(),
        data: Some(proto::route_stream_res_v1::Data::Route(
            route.clone().into(),
        )),
    });

    Ok(())
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
        }
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
    ) -> Self {
        Self::Http(Http {
            flow_type,
            dedupe_timeout,
            path,
            auth_header,
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
            }),
            proto::Protocol::PacketRouter(_args) => Protocol::PacketRouter,
        }
    }
}
