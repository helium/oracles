use crate::{
    api::{gateway::Gateway, internal_error, DatabaseConnection},
    Error, EventId, PublicKey, Result,
};
use axum::{http::StatusCode, Json};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::CellHeartbeatReqV1;
use serde::Deserialize;
use serde_json::{json, Value};

pub async fn create_cell_heartbeat(
    Json(event): Json<CellHeartbeat>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    Gateway::update_last_heartbeat(&mut conn, &event.pubkey, &event.timestamp)
        .await
        .and_then(|_| EventId::try_from(event))
        .map(|id| json!({ "id": id }))
        .map(Json)
        .map_err(internal_error)
}

#[derive(Deserialize)]
pub struct CellHeartbeat {
    #[serde(alias = "pubKey")]
    pub pubkey: PublicKey,
    pub hotspot_type: String,
    pub cell_id: u32,
    pub timestamp: DateTime<Utc>,
    #[serde(alias = "longitude")]
    pub lon: f64,
    #[serde(alias = "latitude")]
    pub lat: f64,
    pub operation_mode: bool,
    pub cbsd_category: String,
    pub cbsd_id: String,
}

impl TryInto<CellHeartbeatReqV1> for CellHeartbeat {
    type Error = Error;
    fn try_into(self) -> Result<CellHeartbeatReqV1> {
        Ok(CellHeartbeatReqV1 {
            pub_key: self.pubkey.to_vec(),
            hotspot_type: self.hotspot_type,
            cell_id: self.cell_id,
            timestamp: self.timestamp.timestamp() as u64,
            lon: self.lon,
            lat: self.lat,
            operation_mode: self.operation_mode,
            cbsd_category: self.cbsd_category,
            cbsd_id: self.cbsd_id,
            signature: vec![],
        })
    }
}
