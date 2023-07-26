use crate::pending_burns::PendingBurns;
use async_trait::async_trait;
use file_store::{
    file_sink::FileSinkClient, iot_packet::PacketRouterPacketReport, traits::MsgTimestamp,
};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::packet_verifier::{InvalidPacket, InvalidPacketReason, ValidPacket};
use iot_config::client::{ClientError, OrgClient};
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
use tracing::debug;

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
            debug!(%report.received_timestamp, "Processing packet report");

            let debit_amount = payload_size_to_dc(report.payload_size as u64);

            debug!(%report.oui, "Fetching payer");
            let payer = self
                .config_server
                .fetch_org(report.oui, &mut org_cache)
                .await
                .map_err(VerificationError::ConfigError)?;
            debug!(%payer, "Debiting payer");
            let remaining_balance = self
                .debiter
                .debit_if_sufficient(&payer, debit_amount)
                .await
                .map_err(VerificationError::DebitError)?;

            if let Some(remaining_balance) = remaining_balance {
                debug!(%debit_amount, "Adding debit amount to pending burns");

                pending_burns
                    .add_burned_amount(&payer, debit_amount)
                    .await
                    .map_err(VerificationError::BurnError)?;

                debug!("Writing valid packet report");
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
                    debug!(%report.oui, "Disabling org");
                    self.config_server
                        .disable_org(report.oui)
                        .await
                        .map_err(VerificationError::ConfigError)?;
                }
            } else {
                debug!("Writing invalid packet report");
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

#[derive(thiserror::Error, Debug)]
pub enum ConfigServerError {
    #[error("org client error: {0}")]
    Client(#[from] ClientError),
    #[error("not found: {0}")]
    NotFound(u64),
}

#[async_trait]
impl ConfigServer for Arc<Mutex<OrgClient>> {
    type Error = ConfigServerError;

    async fn fetch_org(
        &self,
        oui: u64,
        cache: &mut HashMap<u64, PublicKeyBinary>,
    ) -> Result<PublicKeyBinary, Self::Error> {
        if let Entry::Vacant(e) = cache.entry(oui) {
            let pubkey = PublicKeyBinary::from(
                self.lock()
                    .await
                    .get(oui)
                    .await?
                    .org
                    .ok_or(ConfigServerError::NotFound(oui))?
                    .payer,
            );
            e.insert(pubkey);
        }
        Ok(cache.get(&oui).unwrap().clone())
    }

    async fn disable_org(&self, oui: u64) -> Result<(), Self::Error> {
        self.lock().await.disable(oui).await?;
        Ok(())
    }

    async fn enable_org(&self, oui: u64) -> Result<(), Self::Error> {
        self.lock().await.enable(oui).await?;
        Ok(())
    }

    async fn list_orgs(&self) -> Result<Vec<Org>, Self::Error> {
        Ok(self
            .lock()
            .await
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
