use crate::{traits::MsgDecode, Error};
use chrono::{DateTime, TimeZone, Utc};
use helium_proto as proto;

#[derive(Clone, Debug)]
pub struct RewardManifest {
    pub written_files: Vec<String>,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
}

impl MsgDecode for RewardManifest {
    type Msg = proto::RewardManifest;
}

impl TryFrom<proto::RewardManifest> for RewardManifest {
    type Error = Error;

    fn try_from(value: proto::RewardManifest) -> Result<Self, Self::Error> {
        Ok(RewardManifest {
            written_files: value.written_files,
            start_timestamp: Utc.timestamp(value.start_timestamp as i64, 0),
            end_timestamp: Utc.timestamp(value.end_timestamp as i64, 0),
        })
    }
}
