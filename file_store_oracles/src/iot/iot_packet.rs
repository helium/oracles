use blake3::Hasher;
use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::{
        packet_verifier::ValidPacket,
        router::{packet_router_packet_report_v1::PacketType, PacketRouterPacketReportV1},
    },
    DataRate, Region,
};
use serde::Serialize;

use crate::{prost_enum, traits::MsgTimestamp};

#[derive(thiserror::Error, Debug)]
pub enum IotPacketError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("unsupported datarate: {0}")]
    DataRate(prost::UnknownEnumValue),

    #[error("unsupported region: {0}")]
    Region(prost::UnknownEnumValue),

    #[error("unsupported packet type: {0}")]
    PacketType(prost::UnknownEnumValue),
}

#[derive(Serialize, Clone)]
pub struct PacketRouterPacketReport {
    pub oui: u64,
    pub net_id: u32,
    pub rssi: i32,
    pub free: bool,
    /// Frequency in Hz
    pub frequency: u32,
    pub snr: f32,
    pub data_rate: DataRate,
    pub region: Region,
    pub gateway: PublicKeyBinary,
    pub payload_hash: Vec<u8>,
    pub payload_size: u32,
    pub packet_type: PacketType,
    pub received_timestamp: DateTime<Utc>,
}

#[derive(Serialize, Clone)]
pub struct IotValidPacket {
    pub payload_size: u32,
    pub gateway: PublicKeyBinary,
    pub payload_hash: Vec<u8>,
    pub num_dcs: u32,
    pub packet_timestamp: DateTime<Utc>,
}

impl MsgTimestamp<u64> for PacketRouterPacketReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for PacketRouterPacketReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotValidPacket {
    fn timestamp(&self) -> u64 {
        self.packet_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for ValidPacket {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.packet_timestamp.to_timestamp_millis()
    }
}

impl MsgDecode for PacketRouterPacketReport {
    type Msg = PacketRouterPacketReportV1;
}

impl MsgDecode for IotValidPacket {
    type Msg = ValidPacket;
}

impl TryFrom<PacketRouterPacketReportV1> for PacketRouterPacketReport {
    type Error = IotPacketError;

    fn try_from(v: PacketRouterPacketReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            oui: v.oui,
            net_id: v.net_id,
            rssi: v.rssi,
            free: v.free,
            frequency: v.frequency,
            snr: v.snr,
            data_rate: prost_enum(v.datarate, IotPacketError::DataRate)?,
            region: prost_enum(v.region, IotPacketError::Region)?,
            gateway: v.gateway.into(),
            payload_hash: v.payload_hash,
            payload_size: v.payload_size,
            packet_type: prost_enum(v.r#type, IotPacketError::PacketType)?,
        })
    }
}

impl TryFrom<ValidPacket> for IotValidPacket {
    type Error = IotPacketError;

    fn try_from(v: ValidPacket) -> Result<Self, Self::Error> {
        let ts = v.timestamp()?;
        Ok(Self {
            gateway: v.gateway.into(),
            payload_hash: v.payload_hash,
            payload_size: v.payload_size,
            num_dcs: v.num_dcs,
            packet_timestamp: ts,
        })
    }
}

impl From<IotValidPacket> for ValidPacket {
    fn from(v: IotValidPacket) -> Self {
        let ts = v.timestamp();
        Self {
            gateway: v.gateway.into(),
            payload_hash: v.payload_hash,
            payload_size: v.payload_size,
            num_dcs: v.num_dcs,
            packet_timestamp: ts,
        }
    }
}

impl IotValidPacket {
    pub fn packet_id(&self) -> Vec<u8> {
        let mut hasher = Hasher::new();
        let now = Utc::now().timestamp_micros() as u64;
        hasher.update(self.gateway.as_ref());
        hasher.update(self.payload_hash.as_ref());
        hasher.update(
            &self
                .packet_timestamp
                .encode_timestamp_millis()
                .to_le_bytes(),
        );
        hasher.update(&now.to_le_bytes());
        hasher.finalize().as_bytes().to_vec()
    }
}
