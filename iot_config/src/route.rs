use crate::{
    lora_field::{DevAddrRange, Eui, NetIdField},
    org::OrgStatus,
};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use helium_proto::Region;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{types::Uuid, Row};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::broadcast::{self, Sender};

pub mod proto {
    pub use helium_proto::services::iot_config::{
        protocol_http_roaming_v1::FlowTypeV1, server_v1::Protocol, ActionV1, ProtocolGwmpMappingV1,
        ProtocolGwmpV1, ProtocolHttpRoamingV1, ProtocolPacketRouterV1, RouteDevaddrsActionV1,
        RouteEuisActionV1, RouteStreamResV1, RouteV1, ServerV1,
    };
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Route {
    pub id: String,
    pub net_id: NetIdField,
    pub devaddr_ranges: Vec<DevAddrRange>,
    pub euis: Vec<Eui>,
    pub oui: u64,
    pub server: RouteServer,
    pub max_copies: u32,
}

impl Route {
    pub fn new(net_id: NetIdField, oui: u64, max_copies: u32) -> Self {
        Self {
            id: "".into(),
            net_id,
            devaddr_ranges: vec![],
            euis: vec![],
            oui,
            server: RouteServer::default(),
            max_copies,
        }
    }

    pub fn add_eui(&mut self, eui: Eui) {
        self.euis.push(eui);
    }

    pub fn remove_eui(&mut self, eui: Eui) {
        self.euis.retain(|e| e != &eui);
    }

    pub fn add_devaddr(&mut self, range: DevAddrRange) {
        self.devaddr_ranges.push(range);
    }

    pub fn remove_devaddr(&mut self, range: DevAddrRange) {
        self.devaddr_ranges.retain(|dr| dr != &range);
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
    pub net_id: i64,
    pub max_copies: i32,
    pub server_host: String,
    pub server_port: i32,
    pub server_protocol_opts: serde_json::Value,
    pub devaddr_ranges: Vec<(i64, i64)>,
    pub eui_pairs: Vec<(i64, i64)>,
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
    StreamUpdate(#[from] broadcast::error::SendError<proto::RouteStreamResV1>),
}

pub async fn create_route(
    route: Route,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Arc<Sender<proto::RouteStreamResV1>>,
) -> Result<Route, RouteStorageError> {
    let net_id: i64 = route.net_id.into();
    let protocol_opts = route
        .server
        .protocol
        .as_ref()
        .ok_or("no protocol defined")
        .map_err(|e| RouteStorageError::ServerProtocol(e.to_string()))?;

    let mut transaction = db.begin().await?;

    let row = sqlx::query(
            r#"
            insert into routes (oui, net_id, max_copies, server_host, server_port, server_protocol_opts)
            values ($1, $2, $3, $4, $5, $6)
            returning id
            "#,
        )
        .bind(route.oui as i64)
        .bind(net_id)
        .bind(route.max_copies as i32)
        .bind(&route.server.host)
        .bind(route.server.port as i32)
        .bind(json!(&protocol_opts))
        .fetch_one(&mut transaction)
        .await?;

    let route_id = row.get::<Uuid, &str>("id").to_string();

    if !route.euis.is_empty() {
        insert_euis(&route_id, &route.euis, &mut transaction).await?;
    }

    if !route.devaddr_ranges.is_empty() {
        insert_devaddr_ranges(&route_id, &route.devaddr_ranges, &mut transaction).await?;
    }

    let new_route = get_route(&route_id, &mut transaction).await?;

    transaction.commit().await?;

    _ = update_tx.send(proto::RouteStreamResV1 {
        action: proto::ActionV1::Create.into(),
        route: Some(new_route.clone().into()),
    });

    Ok(new_route)
}

pub async fn update_route(
    route: Route,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Arc<Sender<proto::RouteStreamResV1>>,
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
        set max_copies = $2, server_host = $3, server_port = $4, server_protocol_opts = $5
        where id = $1
        "#,
    )
    .bind(uuid)
    .bind(route.max_copies as i32)
    .bind(&route.server.host)
    .bind(route.server.port as i32)
    .bind(json!(&protocol_opts))
    .execute(&mut transaction)
    .await?;

    sqlx::query(
        r#"
        delete from route_eui_pairs
        where route_id = $1
        "#,
    )
    .bind(uuid)
    .execute(&mut transaction)
    .await?;

    if !route.euis.is_empty() {
        insert_euis(&route.id, &route.euis, &mut transaction).await?;
    }

    sqlx::query(
        r#"
        delete from route_devaddr_ranges
        where route_id = $1
        "#,
    )
    .bind(uuid)
    .execute(&mut transaction)
    .await?;

    if !route.devaddr_ranges.is_empty() {
        insert_devaddr_ranges(&route.id, &route.devaddr_ranges, &mut transaction).await?;
    }

    let updated_route = get_route(&route.id, &mut transaction).await?;

    transaction.commit().await?;

    _ = update_tx.send(proto::RouteStreamResV1 {
        action: proto::ActionV1::Update.into(),
        route: Some(updated_route.clone().into()),
    });

    Ok(updated_route)
}

async fn insert_euis(
    id: &str,
    euis: &[Eui],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), RouteStorageError> {
    let id = Uuid::try_parse(id)?;
    let euis = euis.iter().map(|eui| (id, eui));

    const EUI_INSERT_SQL: &str = " insert into route_eui_pairs (route_id, app_eui, dev_eui) ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(EUI_INSERT_SQL);
    query_builder.push_values(euis, |mut builder, (id, eui)| {
        builder
            .push_bind(id)
            .push_bind(i64::from(eui.app_eui))
            .push_bind(i64::from(eui.dev_eui));
    });

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

pub async fn modify_euis(
    id: &str,
    action: proto::RouteEuisActionV1,
    euis: &[Eui],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Arc<Sender<proto::RouteStreamResV1>>,
) -> Result<(), RouteStorageError> {
    let mut transaction = db.begin().await?;

    match action {
        proto::RouteEuisActionV1::AddEuis => {
            insert_euis(id, euis, &mut transaction).await?;
        }
        proto::RouteEuisActionV1::RemoveEuis => {
            let uuid = Uuid::try_parse(id)?;
            const EUI_REMOVE_PAIRS_SNIPPET: &str =
                " delete from route_eui_pairs where (app_eui, dev_eui) in ";
            const EUI_FILTER_ID_SNIPPET: &str = " and (route_id) = ";
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new(EUI_REMOVE_PAIRS_SNIPPET);
            query_builder
                .push_tuples(euis, |mut builder, eui| {
                    builder
                        .push_bind(i64::from(eui.app_eui))
                        .push_bind(i64::from(eui.dev_eui));
                })
                .push(EUI_FILTER_ID_SNIPPET)
                .push_bind(uuid);

            query_builder
                .build()
                .execute(&mut transaction)
                .await
                .map(|_| ())?;
        }
        proto::RouteEuisActionV1::UpdateEuis => {
            sqlx::query(
                r#"
                delete from route_eui_pairs where route_id = $1
                "#,
            )
            .bind(id)
            .execute(&mut transaction)
            .await?;

            insert_euis(id, euis, &mut transaction).await?;
        }
    }

    let route = get_route(id, &mut transaction).await?;

    transaction.commit().await?;

    _ = update_tx.send(proto::RouteStreamResV1 {
        action: proto::ActionV1::Update.into(),
        route: Some(route.clone().into()),
    });

    Ok(())
}

async fn insert_devaddr_ranges(
    id: &str,
    ranges: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), RouteStorageError> {
    let id = Uuid::try_parse(id)?;
    let ranges = ranges.iter().map(|range| (id, range));

    const DEVADDR_RANGE_INSERT_SQL: &str =
        "insert into route_devaddr_ranges (route_id, start_addr, end_addr) ";
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
        sqlx::QueryBuilder::new(DEVADDR_RANGE_INSERT_SQL);
    query_builder.push_values(ranges, |mut builder, (id, range)| {
        builder
            .push_bind(id)
            .push_bind(i64::from(range.start_addr))
            .push_bind(i64::from(range.end_addr));
    });

    query_builder.build().execute(db).await.map(|_| ())?;

    Ok(())
}

pub async fn modify_devaddr_ranges(
    id: &str,
    action: proto::RouteDevaddrsActionV1,
    devaddr_ranges: &[DevAddrRange],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Arc<Sender<proto::RouteStreamResV1>>,
) -> Result<(), RouteStorageError> {
    let mut transaction = db.begin().await?;

    match action {
        proto::RouteDevaddrsActionV1::AddDevaddrs => {
            insert_devaddr_ranges(id, devaddr_ranges, &mut transaction).await?;
        }
        proto::RouteDevaddrsActionV1::RemoveDevaddrs => {
            let uuid = Uuid::try_parse(id)?;
            const DEVADDR_REMOVE_RANGE_SNIPPET: &str =
                " delete from route_devaddr_ranges where (start_addr, end_addr) in ";
            const DEVADDR_FILTER_ID_SNIPPET: &str = " and (route_id) = ";
            let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
                sqlx::QueryBuilder::new(DEVADDR_REMOVE_RANGE_SNIPPET);
            query_builder
                .push_tuples(devaddr_ranges, |mut builder, range| {
                    builder
                        .push_bind(i64::from(range.start_addr))
                        .push_bind(i64::from(range.end_addr));
                })
                .push(DEVADDR_FILTER_ID_SNIPPET)
                .push_bind(uuid);

            query_builder
                .build()
                .execute(&mut transaction)
                .await
                .map(|_| ())?;
        }
        proto::RouteDevaddrsActionV1::UpdateDevaddrs => {
            sqlx::query(
                r#"
                delete from route_devaddr_ranges where route_id = $1
                "#,
            )
            .bind(id)
            .execute(&mut transaction)
            .await?;

            insert_devaddr_ranges(id, devaddr_ranges, &mut transaction).await?;
        }
    }

    let route = get_route(id, &mut transaction).await?;

    transaction.commit().await?;

    _ = update_tx.send(proto::RouteStreamResV1 {
        action: proto::ActionV1::Update.into(),
        route: Some(route.clone().into()),
    });

    Ok(())
}

pub async fn list_routes(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<Route>, RouteStorageError> {
    Ok(sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts,
               array(select distinct row(app_eui, dev_eui) from route_eui_pairs e where e.route_id = r.id) as eui_pairs,
               array(select distinct row(start_addr, end_addr) from route_devaddr_ranges d where d.route_id = r.id) as devaddr_ranges
            from routes r
            where r.oui = $1
            group by r.id
        "#,
    )
    .bind(oui as i64)
    .fetch(db)
    .map_err(RouteStorageError::from)
    .and_then(|route| async move { Ok(Route {
            id: route.id.to_string(),
            net_id: route.net_id.into(),
            devaddr_ranges: route.devaddr_ranges.into_iter().map(|(start, end)| DevAddrRange {start_addr: start.into(), end_addr: end.into()}).collect(),
            euis: route.eui_pairs
                      .into_iter()
                      .map(|(app, dev)| Eui::new(app.into(), dev.into()))
                      .collect(),
            oui: route.oui as u64,
            server: RouteServer::new(route.server_host, route.server_port as u32, serde_json::from_value(route.server_protocol_opts)?),
            max_copies: route.max_copies as u32,
        })})
    .filter_map(|route| async move { route.ok() })
    .collect::<Vec<Route>>()
    .await)
}

pub async fn route_stream_by_status<'a>(
    status: OrgStatus,
    db: impl sqlx::PgExecutor<'a> + 'a + Copy,
) -> impl Stream<Item = Route> + 'a {
    sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts,
            array(select distinct row(app_eui, dev_eui) from route_eui_pairs e where e.route_id = r.id) as eui_pairs,
            array(select distinct row(start_addr, end_addr) from route_devaddr_ranges d where d.route_id = r.id) as devaddr_ranges
            from routes r
            join organizations o on r.oui = o.oui
            where o.status = $1
            group by r.id
        "#,
    )
    .bind(status)
    .fetch(db)
    .map_err(RouteStorageError::from)
    .and_then(|route| async move { Ok(Route {
            id: route.id.to_string(),
            net_id: route.net_id.into(),
            devaddr_ranges: route.devaddr_ranges.into_iter().map(|(start, end)| DevAddrRange {start_addr: start.into(), end_addr: end.into()}).collect(),
            euis: route.eui_pairs.into_iter().map(|(app, dev)| Eui::new(app.into(), dev.into())).collect(),
            oui: route.oui as u64,
            server: RouteServer::new(route.server_host, route.server_port as u32, serde_json::from_value(route.server_protocol_opts)?),
            max_copies: route.max_copies as u32,
        })})
    .filter_map(|route| async move { route.ok() })
}

pub async fn get_route(
    id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Route, RouteStorageError> {
    let uuid = Uuid::try_parse(id)?;
    let route_row = sqlx::query_as::<_, StorageRoute>(
        r#"
        select r.id, r.oui, r.net_id, r.max_copies, r.server_host, r.server_port, r.server_protocol_opts,
            array(select distinct row(app_eui, dev_eui) from route_eui_pairs e where e.route_id = r.id) as eui_pairs,
            array(select distinct row(start_addr, end_addr) from route_devaddr_ranges d where d.route_id = r.id) as devaddr_ranges
            from routes r
            where r.id = $1
            group by r.id
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await;

    tracing::info!("Result Route Row: {route_row:?}");
    let route = route_row?;
    tracing::info!("The Server Protocol Json: {:?}", route.server_protocol_opts);

    let server = RouteServer::new(
        route.server_host,
        route.server_port as u32,
        serde_json::from_value(route.server_protocol_opts)?,
    );
    tracing::info!("Successfully created new RouteServer");

    Ok(Route {
        id: route.id.to_string(),
        net_id: route.net_id.into(),
        devaddr_ranges: route
            .devaddr_ranges
            .into_iter()
            .map(|(start, end)| DevAddrRange {
                start_addr: start.into(),
                end_addr: end.into(),
            })
            .collect(),
        euis: route
            .eui_pairs
            .into_iter()
            .map(|(app, dev)| Eui::new(app.into(), dev.into()))
            .collect(),
        oui: route.oui as u64,
        server,
        max_copies: route.max_copies as u32,
    })
}

pub async fn delete_route(
    id: &str,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
    update_tx: Arc<Sender<proto::RouteStreamResV1>>,
) -> Result<(), RouteStorageError> {
    let uuid = Uuid::try_parse(id)?;
    let mut transaction = db.begin().await?;

    sqlx::query(
        r#"
        delete from routes
        where id = $1
        "#,
    )
    .bind(uuid)
    .execute(&mut transaction)
    .await?;

    let route = get_route(id, &mut transaction).await?;

    transaction.commit().await?;

    _ = update_tx.send(proto::RouteStreamResV1 {
        action: proto::ActionV1::Delete.into(),
        route: Some(route.clone().into()),
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
            devaddr_ranges: route
                .devaddr_ranges
                .into_iter()
                .map(|dr| dr.into())
                .collect(),
            euis: route.euis.into_iter().map(|e| e.into()).collect(),
            oui: route.oui,
            server: route.server.map_or_else(RouteServer::default, |s| s.into()),
            max_copies: route.max_copies,
        }
    }
}

#[allow(deprecated)]
impl From<Route> for proto::RouteV1 {
    fn from(route: Route) -> Self {
        Self {
            id: route.id,
            net_id: route.net_id.into(),
            devaddr_ranges: route
                .devaddr_ranges
                .into_iter()
                .map(|dr| dr.into())
                .collect(),
            euis: route.euis.into_iter().map(|e| e.into()).collect(),
            oui: route.oui,
            server: Some(route.server.into()),
            max_copies: route.max_copies,
            // Deprecated proto field; flagged above to avoid compiler warning
            nonce: 0,
        }
    }
}

pub type Port = u32;
pub type GwmpMap = BTreeMap<Region, Port>;

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

    pub fn make_gwmp_mapping(region: Region, port: Port) -> GwmpMap {
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

    pub fn make_gwmp(region: Region, port: Port) -> Result<Self, RouteServerError> {
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
                    let region = Region::from_i32(entry.region).unwrap_or(Region::Us915);
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
