use crate::{
    api::{internal_error, not_found_error, DatabaseConnection},
    pagination::Since,
    CellSpeedtest, Uuid,
};
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Json,
};
use serde_json::{json, Value};

pub async fn create_cell_speedtest(
    Json(event): Json<CellSpeedtest>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    event
        .insert_into(&mut conn)
        .await
        .map(|id: Uuid| {
            json!({
                "id": id,
            })
        })
        .map(Json)
        .map_err(internal_error)
}

pub async fn get_cell_speedtest(
    Path(id): Path<Uuid>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let event = CellSpeedtest::get(&mut conn, &id)
        .await
        .map_err(internal_error)?;
    if let Some(event) = event {
        let json = serde_json::to_value(event).map_err(internal_error)?;
        Ok(Json(json))
    } else {
        Err(not_found_error())
    }
}

pub async fn get_hotspot_cell_speedtests(
    Path(id): Path<String>,
    Query(since): Query<Since>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let heartbeats = CellSpeedtest::for_hotspot_since(&mut conn, &id, &since)
        .await
        .map_err(internal_error)?;
    let json = serde_json::to_value(heartbeats).map_err(internal_error)?;
    Ok(Json(json))
}

pub async fn get_hotspot_last_cell_speedtest(
    Path(id): Path<String>,
    DatabaseConnection(mut conn): DatabaseConnection,
) -> std::result::Result<Json<Value>, (StatusCode, String)> {
    let heartbeat = CellSpeedtest::for_hotspot_last(&mut conn, &id)
        .await
        .map_err(internal_error)?;
    if let Some(heartbeat) = heartbeat {
        let json = serde_json::to_value(heartbeat).map_err(internal_error)?;
        Ok(Json(json))
    } else {
        Err(not_found_error())
    }
}
