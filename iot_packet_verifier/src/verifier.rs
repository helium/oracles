use crate::pending_burns::PendingBurns;
use async_trait::async_trait;
use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient, iot_packet::PacketRouterPacketReport, traits::MsgTimestamp,
};
use futures::{Stream, StreamExt};
use helium_crypto::{Keypair, PublicKeyBinary, Sign};
use helium_proto::services::{
    iot_config::OrgGetReqV1,
    packet_verifier::{InvalidPacket, InvalidPacketReason, ValidPacket},
};
use helium_proto::{
    services::{
        iot_config::{config_org_client::OrgClient, OrgDisableReqV1, OrgEnableReqV1},
        Channel,
    },
    Message,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    convert::Infallible,
    fmt::Debug,
    mem,
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct Verifier<D, C> {
    pub debiter: D,
    pub config_server: C,
}

#[derive(thiserror::Error, Debug)]
pub enum VerificationError<DE, CE, BE, VPE, IPE> {
    #[error("Debit error: {0}")]
    DebitError(DE),
    #[error("Config server error: {0}")]
    ConfigError(CE),
    #[error("Burn error: {0}")]
    BurnError(BE),
    #[error("Valid packet writer error: {0}")]
    ValidPacketWriterError(VPE),
    #[error("Invalid packet writer error: {0}")]
    InvalidPacketWriterError(IPE),
}

impl<D, C> Verifier<D, C>
where
    D: Debiter,
    C: ConfigServer,
{
    /// Verify a stream of packet reports. Writes out `valid_packets` and `invalid_packets`.
    pub async fn verify<B, R, VP, IP>(
        &mut self,
        mut pending_burns: B,
        reports: R,
        mut valid_packets: VP,
        mut invalid_packets: IP,
    ) -> Result<(), VerificationError<D::Error, C::Error, B::Error, VP::Error, IP::Error>>
    where
        B: PendingBurns,
        R: Stream<Item = PacketRouterPacketReport>,
        VP: PacketWriter<ValidPacket>,
        IP: PacketWriter<InvalidPacket>,
    {
        let mut org_cache = HashMap::<u64, PublicKeyBinary>::new();

        tokio::pin!(reports);

        while let Some(report) = reports.next().await {
            let debit_amount = payload_size_to_dc(report.payload_size as u64);

            let payer = self
                .config_server
                .fetch_org(report.oui, &mut org_cache)
                .await
                .map_err(VerificationError::ConfigError)?;
            if self
                .debiter
                .debit_if_sufficient(&payer, debit_amount)
                .await
                .map_err(VerificationError::DebitError)?
            {
                pending_burns
                    .add_burned_amount(&payer, debit_amount)
                    .await
                    .map_err(VerificationError::BurnError)?;
                valid_packets
                    .write(ValidPacket {
                        packet_timestamp: report.timestamp(),
                        payload_size: report.payload_size,
                        gateway: report.gateway.into(),
                        payload_hash: report.payload_hash,
                        num_dcs: debit_amount as u32,
                    })
                    .await
                    .map_err(VerificationError::ValidPacketWriterError)?;
                self.config_server
                    .enable_org(report.oui)
                    .await
                    .map_err(VerificationError::ConfigError)?;
            } else {
                invalid_packets
                    .write(InvalidPacket {
                        payload_size: report.payload_size,
                        gateway: report.gateway.into(),
                        payload_hash: report.payload_hash,
                        reason: InvalidPacketReason::InsufficientBalance as i32,
                    })
                    .await
                    .map_err(VerificationError::InvalidPacketWriterError)?;
                self.config_server
                    .disable_org(report.oui)
                    .await
                    .map_err(VerificationError::ConfigError)?;
            }
        }

        Ok(())
    }
}

pub const BYTES_PER_DC: u64 = 24;

pub fn payload_size_to_dc(payload_size: u64) -> u64 {
    let payload_size = payload_size.max(BYTES_PER_DC);
    // perform a div_ciel function:
    (payload_size + BYTES_PER_DC - 1) / BYTES_PER_DC
}

#[async_trait]
pub trait Debiter {
    type Error;

    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<bool, Self::Error>;
}

#[async_trait]
impl Debiter for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = Infallible;

    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<bool, Infallible> {
        let map = self.lock().await;
        let balance = map.get(payer).unwrap();
        // Don't debit the amount if we're mocking. That is a job for the burner.
        Ok(*balance >= amount)
    }
}

#[async_trait]
pub trait ConfigServer {
    type Error;

    async fn fetch_org(
        &mut self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, Self::Error>;

    async fn enable_org(&mut self, oui: u64) -> Result<(), Self::Error>;

    async fn disable_org(&mut self, oui: u64) -> Result<(), Self::Error>;
}

// TODO: Move this somewhere else

// Probably should change name to something like OrgClientCache to be more
// consistent with BalanceCache
pub struct CachedOrgClient {
    pub keypair: Keypair,
    pub enabled_clients: HashMap<u64, bool>,
    pub client: OrgClient<Channel>,
}

impl CachedOrgClient {
    pub fn new(client: OrgClient<Channel>, keypair: Keypair) -> Self {
        CachedOrgClient {
            keypair,
            enabled_clients: HashMap::new(),
            client,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum OrgClientError {
    #[error("Rpc error: {0}")]
    RpcError(#[from] tonic::Status),
    #[error("Crypto error: {0}")]
    CryptoError(#[from] helium_crypto::Error),
}

#[async_trait]
impl ConfigServer for CachedOrgClient {
    type Error = OrgClientError;

    async fn fetch_org(
        &mut self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, Self::Error> {
        if let Entry::Vacant(e) = cache.entry(oui) {
            let req = OrgGetReqV1 { oui };
            let pubkey =
                PublicKeyBinary::from(self.client.get(req).await?.into_inner().org.unwrap().owner);
            e.insert(pubkey);
        }
        Ok(cache.get(&oui).unwrap().clone())
    }

    async fn enable_org(&mut self, oui: u64) -> Result<(), Self::Error> {
        if !mem::replace(self.enabled_clients.entry(oui).or_insert(false), true) {
            let mut req = OrgEnableReqV1 {
                oui,
                timestamp: Utc::now().timestamp_millis() as u64,
                signer: self.keypair.public_key().into(),
                signature: vec![],
            };
            let signature = self.keypair.sign(&req.encode_to_vec())?;
            req.signature = signature;
            let _ = self.client.enable(req).await?;
        }
        Ok(())
    }

    async fn disable_org(&mut self, oui: u64) -> Result<(), Self::Error> {
        if mem::replace(self.enabled_clients.entry(oui).or_insert(true), false) {
            let mut req = OrgDisableReqV1 {
                oui,
                timestamp: Utc::now().timestamp_millis() as u64,
                signer: self.keypair.public_key().into(),
                signature: vec![],
            };
            let signature = self.keypair.sign(&req.encode_to_vec())?;
            req.signature = signature;
            let _ = self.client.disable(req).await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait PacketWriter<T> {
    type Error;

    // The redundant &mut receivers we see for PacketWriter and Burner are so
    // that we are able to resolve to either a mutable or immutable ref without
    // having to take ownership of the mutable reference.
    async fn write(&mut self, packet: T) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T: prost::Message + 'static> PacketWriter<T> for &'_ FileSinkClient {
    type Error = file_store::Error;

    async fn write(&mut self, packet: T) -> Result<(), Self::Error> {
        (*self).write(packet, []).await?;
        Ok(())
    }
}

#[async_trait]
impl<T: Send> PacketWriter<T> for &'_ mut Vec<T> {
    type Error = ();

    async fn write(&mut self, packet: T) -> Result<(), ()> {
        (*self).push(packet);
        Ok(())
    }
}
