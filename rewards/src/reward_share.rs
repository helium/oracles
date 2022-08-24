use futures::stream::StreamExt;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use poc_store::BytesMutStream;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{CellType, PublicKey, Result};
use std::collections::HashMap;

// key: cbsd_id, val: share
pub type Shares = HashMap<String, Share>;

#[derive(Debug, Serialize)]
pub struct Share {
    pub timestamp: u64,
    pub pub_key: PublicKey,
    pub weight: Decimal,
    pub cell_type: CellType,
}

impl Share {
    pub fn new(timestamp: u64, pub_key: PublicKey, weight: Decimal, cell_type: CellType) -> Self {
        Self {
            timestamp,
            pub_key,
            weight,
            cell_type,
        }
    }
}

pub async fn gather_shares(
    stream: &mut BytesMutStream,
    after_utc: u64,
    before_utc: u64,
) -> Result<Shares> {
    let mut shares = Shares::new();

    while let Some(Ok(msg)) = stream.next().await {
        let CellHeartbeatReqV1 {
            pub_key,
            cbsd_id,
            timestamp,
            ..
        } = CellHeartbeatReqV1::decode(msg)?;

        if timestamp < after_utc || timestamp >= before_utc {
            continue;
        }

        if let Some(cell_type) = CellType::from_cbsd_id(&cbsd_id) {
            if let Ok(gw_pubkey) = PublicKey::try_from(pub_key) {
                let share = Share::new(timestamp, gw_pubkey, cell_type.reward_weight(), cell_type);
                shares.insert(cbsd_id, share);
            }
        }
    }

    Ok(shares)
}
