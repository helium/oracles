use crate::{
    error::DecodeError,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use blake3::Hasher;
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::ValidPacket;
use helium_proto::{
    services::router::{packet_router_packet_report_v1::PacketType, PacketRouterPacketReportV1},
    DataRate, Region,
};
use serde::Serialize;

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

impl MsgTimestamp<Result<DateTime<Utc>>> for PacketRouterPacketReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for IotValidPacket {
    fn timestamp(&self) -> u64 {
        self.packet_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for ValidPacket {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
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
    type Error = Error;

    fn try_from(v: PacketRouterPacketReportV1) -> Result<Self> {
        let data_rate = DataRate::from_i32(v.datarate).ok_or_else(|| {
            DecodeError::unsupported_datarate("iot_packet_router_packet_report_v1", v.datarate)
        })?;
        let region = Region::from_i32(v.region).ok_or_else(|| {
            DecodeError::unsupported_region("iot_packet_router_packet_report_v1", v.region)
        })?;
        let packet_type = PacketType::from_i32(v.r#type).ok_or_else(|| {
            DecodeError::unsupported_packet_type("iot_packet_router_packet_report_v1", v.r#type)
        })?;
        let received_timestamp = v.timestamp()?;
        Ok(Self {
            received_timestamp,
            oui: v.oui,
            net_id: v.net_id,
            rssi: v.rssi,
            free: v.free,
            frequency: v.frequency,
            snr: v.snr,
            data_rate,
            region,
            gateway: v.gateway.into(),
            payload_hash: v.payload_hash,
            payload_size: v.payload_size,
            packet_type,
        })
    }
}

impl TryFrom<ValidPacket> for IotValidPacket {
    type Error = Error;
    fn try_from(v: ValidPacket) -> Result<Self> {
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
