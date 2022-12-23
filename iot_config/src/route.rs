use crate::{
    lora_field::{DevAddrRange, Eui, NetIdField},
    region::Region,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod proto {
    pub use helium_proto::services::iot_config::{
        protocol_http_roaming_v1::FlowTypeV1, server_v1::Protocol, ProtocolGwmpMappingV1,
        ProtocolGwmpV1, ProtocolHttpRoamingV1, ProtocolPacketRouterV1, RouteV1, ServerV1,
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
    pub nonce: u64,
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
            nonce: 1,
        }
    }

    pub fn inc_nonce(&mut self) {
        self.nonce += 1;
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
            nonce: route.nonce,
        }
    }
}

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
            nonce: route.nonce,
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
