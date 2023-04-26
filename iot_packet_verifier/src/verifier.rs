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
        iot_config::{config_org_client::OrgClient, OrgDisableReqV1, OrgEnableReqV1, OrgListReqV1},
        Channel,
    },
    Message,
};
use solana::SolanaNetwork;
use std::{
    collections::{hash_map::Entry, HashMap},
    convert::Infallible,
    fmt::Debug,
    sync::Arc,
};
use tokio::{
    sync::Mutex,
    task::JoinError,
    time::{sleep_until, Duration, Instant},
};

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
        minimum_allowed_balance: u64,
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
            let remaining_balance = self
                .debiter
                .debit_if_sufficient(&payer, debit_amount)
                .await
                .map_err(VerificationError::DebitError)?;

            if let Some(remaining_balance) = remaining_balance {
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

                if remaining_balance < minimum_allowed_balance {
                    self.config_server
                        .disable_org(report.oui)
                        .await
                        .map_err(VerificationError::ConfigError)?;
                }
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
            }
        }

        Ok(())
    }
}

pub const BYTES_PER_DC: u64 = 24;

pub fn payload_size_to_dc(payload_size: u64) -> u64 {
    let payload_size = payload_size.max(BYTES_PER_DC);
    // Integer div/ceil from: https://stackoverflow.com/a/2745086
    (payload_size + BYTES_PER_DC - 1) / BYTES_PER_DC
}

#[async_trait]
pub trait Debiter {
    type Error;

    /// Debit the balance from the account. If the debit was successful,
    /// return the remaining amount.
    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Option<u64>, Self::Error>;
}

#[async_trait]
impl Debiter for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = Infallible;

    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Option<u64>, Infallible> {
        let map = self.lock().await;
        let balance = map.get(payer).unwrap();
        // Don't debit the amount if we're mocking. That is a job for the burner.
        Ok((*balance >= amount).then(|| balance.saturating_sub(amount)))
    }
}

// TODO: Move these to a separate module

pub struct Org {
    pub oui: u64,
    pub payer: PublicKeyBinary,
    pub locked: bool,
}

#[async_trait]
pub trait ConfigServer: Sized + Send + Sync + 'static {
    type Error: Send + Sync + 'static;

    async fn fetch_org(
        &self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, Self::Error>;

    async fn disable_org(&self, oui: u64) -> Result<(), Self::Error>;

    async fn enable_org(&self, oui: u64) -> Result<(), Self::Error>;

    async fn list_orgs(&self) -> Result<Vec<Org>, Self::Error>;

    async fn monitor_funds<S, B>(
        self,
        solana: S,
        balances: B,
        minimum_allowed_balance: u64,
        monitor_period: Duration,
        shutdown: triggered::Listener,
    ) -> Result<(), MonitorError<S::Error, Self::Error>>
    where
        S: SolanaNetwork,
        B: BalanceStore,
    {
        let join_handle = tokio::spawn(async move {
            loop {
                tracing::info!("Checking if any orgs need to be re-enabled");

                for Org { locked, payer, oui } in self
                    .list_orgs()
                    .await
                    .map_err(MonitorError::ConfigClientError)?
                    .into_iter()
                {
                    if locked {
                        let balance = solana
                            .payer_balance(&payer)
                            .await
                            .map_err(MonitorError::SolanaError)?;
                        if balance >= minimum_allowed_balance {
                            balances.set_balance(&payer, balance).await;
                            self.enable_org(oui)
                                .await
                                .map_err(MonitorError::ConfigClientError)?;
                        }
                    }
                }
                // Sleep until we should re-check the monitor
                sleep_until(Instant::now() + monitor_period).await;
            }
        });
        tokio::select! {
            result = join_handle => match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err),
                Err(err) => Err(MonitorError::from(err)),
            },
            _ = shutdown => Ok(())
        }
    }
}

#[async_trait]
pub trait BalanceStore: Send + Sync + 'static {
    async fn set_balance(&self, payer: &PublicKeyBinary, balance: u64);
}

#[async_trait]
impl BalanceStore for crate::balances::BalanceStore {
    async fn set_balance(&self, payer: &PublicKeyBinary, balance: u64) {
        self.lock().await.entry(payer.clone()).or_default().balance = balance;
    }
}

#[async_trait]
// differs from the BalanceStore in the value stored in the contained HashMap; a u64 here instead of a Balance {} struct
impl BalanceStore for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    async fn set_balance(&self, payer: &PublicKeyBinary, balance: u64) {
        *self.lock().await.entry(payer.clone()).or_default() = balance;
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MonitorError<S, E> {
    #[error("Join error: {0}")]
    JoinError(#[from] JoinError),
    #[error("Config client error: {0}")]
    ConfigClientError(E),
    #[error("Solana error: {0}")]
    SolanaError(S),
}

// Probably should change name to something like OrgClientCache to be more
// consistent with BalanceCache
pub struct CachedOrgClient {
    pub keypair: Keypair,
    pub client: OrgClient<Channel>,
}

impl CachedOrgClient {
    pub fn new(client: OrgClient<Channel>, keypair: Keypair) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(CachedOrgClient { keypair, client }))
    }

    async fn enable_org(&mut self, oui: u64) -> Result<(), OrgClientError> {
        tracing::info!(%oui, "enabling org");

        let mut req = OrgEnableReqV1 {
            oui,
            timestamp: Utc::now().timestamp_millis() as u64,
            signer: self.keypair.public_key().into(),
            signature: vec![],
        };
        let signature = self.keypair.sign(&req.encode_to_vec())?;
        req.signature = signature;
        let _ = self.client.enable(req).await?;
        Ok(())
    }

    async fn disable_org(&mut self, oui: u64) -> Result<(), OrgClientError> {
        tracing::info!(%oui, "disabling org");

        let mut req = OrgDisableReqV1 {
            oui,
            timestamp: Utc::now().timestamp_millis() as u64,
            signer: self.keypair.public_key().into(),
            signature: vec![],
        };
        let signature = self.keypair.sign(&req.encode_to_vec())?;
        req.signature = signature;
        let _ = self.client.disable(req).await?;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum OrgClientError {
    #[error("Rpc error: {0}")]
    RpcError(#[from] tonic::Status),
    #[error("Crypto error: {0}")]
    CryptoError(#[from] helium_crypto::Error),
    #[error("No org")]
    NoOrg,
}

#[async_trait]
impl ConfigServer for Arc<Mutex<CachedOrgClient>> {
    type Error = OrgClientError;

    async fn fetch_org(
        &self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, Self::Error> {
        if let Entry::Vacant(e) = cache.entry(oui) {
            let req = OrgGetReqV1 { oui };
            let pubkey = PublicKeyBinary::from(
                self.lock()
                    .await
                    .client
                    .get(req)
                    .await?
                    .into_inner()
                    .org
                    .ok_or(OrgClientError::NoOrg)?
                    .payer,
            );
            e.insert(pubkey);
        }
        Ok(cache.get(&oui).unwrap().clone())
    }

    async fn disable_org(&self, oui: u64) -> Result<(), Self::Error> {
        self.lock().await.disable_org(oui).await
    }

    async fn enable_org(&self, oui: u64) -> Result<(), Self::Error> {
        self.lock().await.enable_org(oui).await
    }

    async fn list_orgs(&self) -> Result<Vec<Org>, Self::Error> {
        Ok(self
            .lock()
            .await
            .client
            .list(OrgListReqV1 {})
            .await
            .map_err(OrgClientError::RpcError)?
            .into_inner()
            .orgs
            .into_iter()
            .map(|org| Org {
                oui: org.oui,
                payer: PublicKeyBinary::from(org.payer),
                locked: org.locked,
            })
            .collect())
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
