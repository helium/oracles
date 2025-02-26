use crate::pending::AddPendingBurn;
use async_trait::async_trait;
use file_store::{
    file_sink::FileSinkClient,
    iot_packet::PacketRouterPacketReport,
    traits::{MsgBytes, MsgTimestamp},
};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    packet_verifier::{InvalidPacket, InvalidPacketReason, ValidPacket},
    router::packet_router_packet_report_v1::PacketType,
};
use iot_config::client::{org_client::Orgs, ClientError};
use solana::{burn::SolanaNetwork, SolanaRpcError};
use std::{
    collections::{hash_map::Entry, HashMap},
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
pub enum VerificationError {
    #[error("Debit error: {0}")]
    DebitError(#[from] SolanaRpcError),
    #[error("Config server error: {0}")]
    ConfigError(#[from] ConfigServerError),
    #[error("Burn error: {0}")]
    BurnError(#[from] sqlx::Error),
    #[error("Valid packet writer error: {0}")]
    ValidPacketWriterError(file_store::Error),
    #[error("Invalid packet writer error: {0}")]
    InvalidPacketWriterError(file_store::Error),
}

impl<D, C> Verifier<D, C>
where
    D: Debiter,
    C: ConfigServer,
{
    /// Verify a stream of packet reports. Writes out `valid_packets` and `invalid_packets`.
    pub async fn verify(
        &mut self,
        minimum_allowed_balance: u64,
        pending_burns: &mut impl AddPendingBurn,
        reports: impl Stream<Item = PacketRouterPacketReport>,
        valid_packets: &mut impl PacketWriter<ValidPacket>,
        invalid_packets: &mut impl PacketWriter<InvalidPacket>,
    ) -> Result<(), VerificationError> {
        let mut org_cache = HashMap::<u64, PublicKeyBinary>::new();

        tokio::pin!(reports);

        while let Some(report) = reports.next().await {
            if PacketType::Uplink != report.packet_type {
                continue;
            }

            let debit_amount = if report.free {
                0
            } else {
                payload_size_to_dc(report.payload_size as u64)
            };

            let payer = self
                .config_server
                .fetch_org(report.oui, &mut org_cache)
                .await?;

            if let Some(remaining_balance) = self
                .debiter
                .debit_if_sufficient(&payer, debit_amount, minimum_allowed_balance)
                .await?
            {
                pending_burns
                    .add_burned_amount(&payer, debit_amount)
                    .await?;

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
                    self.config_server.disable_org(report.oui).await?;
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

                self.config_server.disable_org(report.oui).await?;
            }
        }

        Ok(())
    }
}

pub const BYTES_PER_DC: u64 = 24;

pub fn payload_size_to_dc(payload_size: u64) -> u64 {
    let payload_size = payload_size.max(BYTES_PER_DC);
    payload_size.div_ceil(BYTES_PER_DC)
}

#[async_trait]
pub trait Debiter {
    /// Debit the balance from the account. If the debit was successful,
    /// return the remaining amount.
    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        trigger_balance_check_threshold: u64,
    ) -> Result<Option<u64>, SolanaRpcError>;
}

#[async_trait]
impl Debiter for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        _trigger_balance_check_threshold: u64,
    ) -> Result<Option<u64>, SolanaRpcError> {
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
    async fn fetch_org(
        &self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, ConfigServerError>;

    async fn disable_org(&self, oui: u64) -> Result<(), ConfigServerError>;

    async fn enable_org(&self, oui: u64) -> Result<(), ConfigServerError>;

    async fn list_orgs(&self) -> Result<Vec<Org>, ConfigServerError>;

    async fn monitor_funds<S, B>(
        self,
        solana: S,
        balances: B,
        minimum_allowed_balance: u64,
        monitor_period: Duration,
        shutdown: triggered::Listener,
    ) -> Result<(), MonitorError>
    where
        S: SolanaNetwork,
        B: BalanceStore,
    {
        let join_handle = tokio::spawn(async move {
            loop {
                tracing::info!("Checking if any orgs need to be re-enabled");

                for Org { locked, payer, oui } in self.list_orgs().await?.into_iter() {
                    if locked {
                        let balance = solana.payer_balance(&payer).await?;
                        if balance >= minimum_allowed_balance {
                            balances.set_balance(&payer, balance).await;
                            self.enable_org(oui).await?;
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
pub enum MonitorError {
    #[error("Join error: {0}")]
    JoinError(#[from] JoinError),
    #[error("Config client error: {0}")]
    ConfigClientError(#[from] ConfigServerError),
    #[error("Solana error: {0}")]
    SolanaError(#[from] SolanaRpcError),
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigServerError {
    #[error("orgs  error: {0}")]
    OrgError(#[from] ClientError),
    #[error("not found: {0}")]
    NotFound(u64),
}

pub struct CachedOrgClient<O> {
    orgs: O,
    locked_cache: HashMap<u64, bool>,
}

impl<O> CachedOrgClient<O> {
    pub fn new(orgs: O) -> Self {
        Self {
            orgs,
            locked_cache: HashMap::new(),
        }
    }
}

#[async_trait]
impl<O> ConfigServer for Arc<Mutex<CachedOrgClient<O>>>
where
    O: Orgs,
{
    async fn fetch_org(
        &self,
        oui: u64,
        oui_cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, ConfigServerError> {
        if let Entry::Vacant(e) = oui_cache.entry(oui) {
            let pubkey = PublicKeyBinary::from(
                self.lock()
                    .await
                    .orgs
                    .get(oui)
                    .await?
                    .org
                    .ok_or(ConfigServerError::NotFound(oui))?
                    .payer,
            );
            e.insert(pubkey);
        }
        Ok(oui_cache.get(&oui).unwrap().clone())
    }

    async fn disable_org(&self, oui: u64) -> Result<(), ConfigServerError> {
        let mut cached_client = self.lock().await;
        if *cached_client.locked_cache.entry(oui).or_insert(true) {
            cached_client.orgs.disable(oui).await?;
            *cached_client.locked_cache.get_mut(&oui).unwrap() = false;
        }
        Ok(())
    }

    async fn enable_org(&self, oui: u64) -> Result<(), ConfigServerError> {
        let mut cached_client = self.lock().await;
        if !*cached_client.locked_cache.entry(oui).or_insert(false) {
            cached_client.orgs.enable(oui).await?;
            *cached_client.locked_cache.get_mut(&oui).unwrap() = true;
        }
        Ok(())
    }

    async fn list_orgs(&self) -> Result<Vec<Org>, ConfigServerError> {
        Ok(self
            .lock()
            .await
            .orgs
            .list()
            .await?
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
    // The redundant &mut receivers we see for PacketWriter and Burner are so
    // that we are able to resolve to either a mutable or immutable ref without
    // having to take ownership of the mutable reference.
    async fn write(&mut self, packet: T) -> Result<(), file_store::Error>;
}

#[async_trait]
impl<T: MsgBytes + Send + Sync + 'static> PacketWriter<T> for FileSinkClient<T> {
    async fn write(&mut self, packet: T) -> Result<(), file_store::Error> {
        (*self).write(packet, []).await?;
        Ok(())
    }
}

#[async_trait]
impl<T: Send> PacketWriter<T> for Vec<T> {
    async fn write(&mut self, packet: T) -> Result<(), file_store::Error> {
        (*self).push(packet);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_size_to_dc() {
        assert_eq!(1, payload_size_to_dc(1));
        assert_eq!(1, payload_size_to_dc(24));
        assert_eq!(2, payload_size_to_dc(25));
    }
}
