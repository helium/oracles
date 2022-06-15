use crate::{
    api::{internal_error, not_found_error, DatabaseConnection},
    gateway::After,
    Gateway, PublicKey,
};
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Json,
};
use serde_json::Value;

pub async fn get_gateway(
    Path(pubkey): Path<PublicKey>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let event = Gateway::get(&mut conn, &pubkey)
        .await
        .map_err(internal_error)?;
    if let Some(event) = event {
        let json = serde_json::to_value(event).map_err(internal_error)?;
        Ok(Json(json))
    } else {
        Err(not_found_error())
    }
}

pub async fn get_gateways(
    Query(after): Query<After>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let gateways = Gateway::list(&mut conn, &after)
        .await
        .map_err(internal_error)?;
    let json = serde_json::to_value(gateways).map_err(internal_error)?;
    Ok(Json(json))
}
