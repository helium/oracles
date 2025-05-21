use crate::{send_with_retry, GetSignature, SolanaRpcError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_lib::{
    anchor_client::RequestBuilder,
    anchor_lang::{AccountDeserialize, ToAccountMetas},
    programs::{data_credits, helium_sub_daos},
    solana_client::{
        self, client_error::ClientError, nonblocking::rpc_client::RpcClient, rpc_response::Response,
    },
    solana_program,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        program_pack::Pack,
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair, Signature},
        signer::Signer,
        transaction::Transaction,
    },
};
use itertools::Itertools;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use std::{sync::Arc, time::SystemTime};
use tokio::sync::Mutex;

#[async_trait]
pub trait SolanaNetwork: Clone + Send + Sync + 'static {
    type Transaction: GetSignature + Send + Sync + 'static;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError>;

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, SolanaRpcError>;

    async fn submit_transaction(
        &self,
        transaction: &Self::Transaction,
    ) -> Result<(), SolanaRpcError>;

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, SolanaRpcError>;
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    rpc_url: String,
    cluster: String,
    burn_keypair: String,
    dc_mint: String,
    dnt_mint: String,
    #[serde(default)]
    payers_to_monitor: Vec<String>,
    #[serde(default = "min_priority_fee")]
    min_priority_fee: u64,
}

fn min_priority_fee() -> u64 {
    1
}

impl Settings {
    pub fn payers_to_monitor(&self) -> Result<Vec<PublicKeyBinary>, SolanaRpcError> {
        self.payers_to_monitor
            .iter()
            .map(|payer| PublicKeyBinary::from_str(payer))
            .collect::<Result<_, _>>()
            .map_err(SolanaRpcError::from)
    }
}

#[derive(Clone)]
pub struct SolanaRpc {
    provider: Arc<RpcClient>,
    program_cache: BurnProgramCache,
    cluster: String,
    keypair: [u8; 64],
    payers_to_monitor: Vec<PublicKeyBinary>,
    priority_fee: PriorityFee,
    min_priority_fee: u64,
}

impl SolanaRpc {
    pub async fn new(settings: &Settings) -> Result<Arc<Self>, SolanaRpcError> {
        let dc_mint = settings.dc_mint.parse()?;
        let dnt_mint = settings.dnt_mint.parse()?;
        let Ok(keypair) = read_keypair_file(&settings.burn_keypair) else {
            return Err(SolanaRpcError::FailedToReadKeypairError(
                settings.burn_keypair.to_owned(),
            ));
        };
        let provider =
            RpcClient::new_with_commitment(settings.rpc_url.clone(), CommitmentConfig::finalized());
        let program_cache = BurnProgramCache::new(&provider, dc_mint, dnt_mint).await?;
        if program_cache.dc_burn_authority != keypair.pubkey() {
            return Err(SolanaRpcError::InvalidKeypair);
        }
        Ok(Arc::new(Self {
            cluster: settings.cluster.clone(),
            provider: Arc::new(provider),
            program_cache,
            keypair: keypair.to_bytes(),
            payers_to_monitor: settings.payers_to_monitor()?,
            priority_fee: PriorityFee::default(),
            min_priority_fee: settings.min_priority_fee,
        }))
    }
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    type Transaction = Transaction;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
        let ddc_key = delegated_data_credits(&self.program_cache.sub_dao, payer);
        let (escrow_account, _) = Pubkey::find_program_address(
            &["escrow_dc_account".as_bytes(), &ddc_key.to_bytes()],
            &data_credits::ID,
        );
        let account_data = match self
            .provider
            .get_account_with_commitment(&escrow_account, CommitmentConfig::finalized())
            .await?
        {
            Response { value: None, .. } => {
                tracing::info!(%payer, "Account not found, therefore no balance");
                return Ok(0);
            }
            Response {
                value: Some(account),
                ..
            } => account.data,
        };
        let account_layout = spl_token::state::Account::unpack(account_data.as_slice())?;

        if self.payers_to_monitor.contains(payer) {
            metrics::gauge!(
                "balance",
                "payer" => payer.to_string()
            )
            .set(account_layout.amount as f64);
        }

        Ok(account_layout.amount)
    }

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, SolanaRpcError> {
        // Fetch the sub dao epoch info:
        const EPOCH_LENGTH: u64 = 60 * 60 * 24;
        let epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs()
            / EPOCH_LENGTH;
        let (sub_dao_epoch_info, _) = Pubkey::find_program_address(
            &[
                "sub_dao_epoch_info".as_bytes(),
                self.program_cache.sub_dao.as_ref(),
                &epoch.to_le_bytes(),
            ],
            &helium_sub_daos::ID,
        );

        // Fetch escrow account
        let ddc_key = delegated_data_credits(&self.program_cache.sub_dao, payer);
        let (escrow_account, _) = Pubkey::find_program_address(
            &["escrow_dc_account".as_bytes(), &ddc_key.to_bytes()],
            &data_credits::ID,
        );

        let accounts = data_credits::client::accounts::BurnDelegatedDataCreditsV0 {
            sub_dao_epoch_info,
            dao: self.program_cache.dao,
            sub_dao: self.program_cache.sub_dao,
            account_payer: self.program_cache.account_payer,
            data_credits: self.program_cache.data_credits,
            delegated_data_credits: delegated_data_credits(&self.program_cache.sub_dao, payer),
            token_program: spl_token::id(),
            helium_sub_daos_program: helium_sub_daos::ID,
            system_program: solana_program::system_program::id(),
            dc_burn_authority: self.program_cache.dc_burn_authority,
            dc_mint: self.program_cache.dc_mint,
            escrow_account,
            registrar: self.program_cache.registrar,
        };

        let priority_fee_accounts: Vec<_> = accounts
            .to_account_metas(None)
            .into_iter()
            .map(|x| x.pubkey)
            .unique()
            .take(MAX_RECENT_PRIORITY_FEE_ACCOUNTS)
            .collect();

        // Get a new priority fee. Can't be done in Sync land
        let priority_fee = self
            .priority_fee
            .get_estimate(
                &self.provider,
                &priority_fee_accounts,
                self.min_priority_fee,
            )
            .await?;

        tracing::info!(%priority_fee);

        // This is Sync land: anything async in here will error.
        let instructions = {
            let request = RequestBuilder::from(
                data_credits::ID,
                &self.cluster,
                std::rc::Rc::new(Keypair::from_bytes(&self.keypair).unwrap()),
                Some(CommitmentConfig::finalized()),
                &self.provider,
            );

            let args = data_credits::client::args::BurnDelegatedDataCreditsV0 {
                args: data_credits::types::BurnDelegatedDataCreditsArgsV0 { amount },
            };

            // As far as I can tell, the instructions function does not actually have any
            // error paths.
            request
                // Set priority fees:
                .instruction(ComputeBudgetInstruction::set_compute_unit_limit(300_000))
                .instruction(ComputeBudgetInstruction::set_compute_unit_price(
                    priority_fee,
                ))
                // Create burn transaction
                .accounts(accounts)
                .args(args)
                .instructions()
                .unwrap()
        };

        let blockhash = self.provider.get_latest_blockhash().await?;
        let signer = Keypair::from_bytes(&self.keypair).unwrap();

        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&signer.pubkey()),
            &[&signer],
            blockhash,
        ))
    }

    async fn submit_transaction(&self, tx: &Self::Transaction) -> Result<(), SolanaRpcError> {
        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };
        match send_with_retry!(self
            .provider
            .send_and_confirm_transaction_with_spinner_and_config(
                tx,
                CommitmentConfig::finalized(),
                config,
            )) {
            Ok(signature) => {
                tracing::info!(
                    transaction = %signature,
                    "Data credit burn successful",
                );
                Ok(())
            }
            Err(err) => {
                let signature = tx.get_signature();
                tracing::error!(
                    transaction = %signature,
                    "Data credit burn failed: {err:?}"
                );
                Err(err.into())
            }
        }
    }

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, SolanaRpcError> {
        Ok(matches!(
            self.provider
                .get_signature_status_with_commitment_and_history(
                    txn,
                    CommitmentConfig::finalized(),
                    true,
                )
                .await?,
            Some(Ok(()))
        ))
    }
}

#[derive(Default, Clone)]
pub struct PriorityFee {
    last_estimate: Arc<Mutex<LastEstimate>>,
}

pub const MAX_RECENT_PRIORITY_FEE_ACCOUNTS: usize = 128;

impl PriorityFee {
    pub async fn get_estimate(
        &self,
        provider: &RpcClient,
        accounts: &[Pubkey],
        min_priority_fee: u64,
    ) -> Result<u64, ClientError> {
        let mut last_estimate = self.last_estimate.lock().await;
        match last_estimate.time_taken {
            Some(time_taken) if (Utc::now() - time_taken) < chrono::Duration::minutes(15) => {
                return Ok(last_estimate.fee_estimate)
            }
            _ => (),
        }
        // Find a new estimate
        let time_taken = Utc::now();
        let recent_fees = provider.get_recent_prioritization_fees(accounts).await?;
        let mut max_per_slot = Vec::new();
        for (slot, fees) in &recent_fees.into_iter().chunk_by(|x| x.slot) {
            let Some(maximum) = fees.map(|x| x.prioritization_fee).max() else {
                continue;
            };
            max_per_slot.push((slot, maximum));
        }
        // Only take the most recent 20 maximum fees:
        max_per_slot.sort_by(|a, b| a.0.cmp(&b.0).reverse());
        let mut max_per_slot: Vec<_> = max_per_slot.into_iter().take(20).map(|x| x.1).collect();
        max_per_slot.sort();
        // Get the median:
        let num_recent_fees = max_per_slot.len();
        let mid = num_recent_fees / 2;
        let estimate = if num_recent_fees == 0 {
            min_priority_fee
        } else if num_recent_fees % 2 == 0 {
            // If the number of samples is even, taken the mean of the two median fees
            (max_per_slot[mid - 1] + max_per_slot[mid]) / 2
        } else {
            max_per_slot[mid]
        }
        .max(min_priority_fee);
        *last_estimate = LastEstimate::new(time_taken, estimate);
        Ok(estimate)
    }
}

#[derive(Copy, Clone, Default)]
pub struct LastEstimate {
    time_taken: Option<DateTime<Utc>>,
    fee_estimate: u64,
}

impl LastEstimate {
    fn new(time_taken: DateTime<Utc>, fee_estimate: u64) -> Self {
        Self {
            time_taken: Some(time_taken),
            fee_estimate,
        }
    }
}

/// Cached pubkeys for the burn program
#[derive(Clone)]
pub struct BurnProgramCache {
    pub account_payer: Pubkey,
    pub data_credits: Pubkey,
    pub sub_dao: Pubkey,
    pub dao: Pubkey,
    pub dc_mint: Pubkey,
    pub dc_burn_authority: Pubkey,
    pub registrar: Pubkey,
}

impl BurnProgramCache {
    pub async fn new(
        provider: &RpcClient,
        dc_mint: Pubkey,
        dnt_mint: Pubkey,
    ) -> Result<Self, SolanaRpcError> {
        let (account_payer, _) =
            Pubkey::find_program_address(&["account_payer".as_bytes()], &data_credits::ID);
        let (data_credits, _) =
            Pubkey::find_program_address(&["dc".as_bytes(), dc_mint.as_ref()], &data_credits::ID);
        let (sub_dao, _) = Pubkey::find_program_address(
            &["sub_dao".as_bytes(), dnt_mint.as_ref()],
            &helium_sub_daos::ID,
        );
        let (dao, dc_burn_authority) = {
            let account_data = provider.get_account_data(&sub_dao).await?;
            let mut account_data = account_data.as_ref();
            let sub_dao = helium_sub_daos::accounts::SubDaoV0::try_deserialize(&mut account_data)?;
            (sub_dao.dao, sub_dao.dc_burn_authority)
        };
        let registrar = {
            let account_data = provider.get_account_data(&dao).await?;
            let mut account_data = account_data.as_ref();
            helium_sub_daos::accounts::DaoV0::try_deserialize(&mut account_data)?.registrar
        };
        Ok(Self {
            account_payer,
            data_credits,
            sub_dao,
            dao,
            dc_mint,
            dc_burn_authority,
            registrar,
        })
    }
}

pub enum PossibleTransaction {
    NoTransaction(Signature),
    Transaction(Transaction),
}

impl GetSignature for PossibleTransaction {
    fn get_signature(&self) -> &Signature {
        match self {
            Self::NoTransaction(ref sig) => sig,
            Self::Transaction(ref txn) => txn.get_signature(),
        }
    }
}

#[async_trait]
impl SolanaNetwork for Option<Arc<SolanaRpc>> {
    type Transaction = PossibleTransaction;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
        if let Some(ref rpc) = self {
            rpc.payer_balance(payer).await
        } else {
            Ok(u64::MAX)
        }
    }

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<Self::Transaction, SolanaRpcError> {
        if let Some(ref rpc) = self {
            Ok(PossibleTransaction::Transaction(
                rpc.make_burn_transaction(payer, amount).await?,
            ))
        } else {
            Ok(PossibleTransaction::NoTransaction(Signature::new_unique()))
        }
    }

    async fn submit_transaction(
        &self,
        transaction: &Self::Transaction,
    ) -> Result<(), SolanaRpcError> {
        match (self, transaction) {
            (Some(ref rpc), PossibleTransaction::Transaction(ref txn)) => {
                rpc.submit_transaction(txn).await?
            }
            (None, PossibleTransaction::NoTransaction(_)) => (),
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, SolanaRpcError> {
        if let Some(ref rpc) = self {
            rpc.confirm_transaction(txn).await
        } else {
            panic!("We will not confirm transactions when Solana is disabled");
        }
    }
}

pub struct MockTransaction {
    pub signature: Signature,
    pub payer: PublicKeyBinary,
    pub amount: u64,
}

impl GetSignature for MockTransaction {
    fn get_signature(&self) -> &Signature {
        &self.signature
    }
}

#[derive(Clone)]
pub struct TestSolanaClientMap {
    payer_balances: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
    confirm_all_txns: Arc<Mutex<bool>>,
    confirmed_txns: Arc<Mutex<HashSet<Signature>>>,
}

impl Default for TestSolanaClientMap {
    fn default() -> Self {
        Self {
            payer_balances: Default::default(),
            confirm_all_txns: Arc::new(Mutex::new(true)),
            confirmed_txns: Default::default(),
        }
    }
}

impl TestSolanaClientMap {
    pub async fn insert(&self, payer: &PublicKeyBinary, amount: u64) {
        self.payer_balances
            .lock()
            .await
            .insert(payer.clone(), amount);
    }

    pub async fn get_payer_balance(&self, payer: &PublicKeyBinary) -> u64 {
        self.payer_balances
            .lock()
            .await
            .get(payer)
            .cloned()
            .unwrap_or_default()
    }

    pub async fn add_confirmed(&self, signature: Signature) {
        *self.confirm_all_txns.lock().await = false;
        self.confirmed_txns.lock().await.insert(signature);
    }
}

#[async_trait]
impl SolanaNetwork for TestSolanaClientMap {
    type Transaction = MockTransaction;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, SolanaRpcError> {
        Ok(*self.payer_balances.lock().await.get(payer).unwrap())
    }

    async fn make_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<MockTransaction, SolanaRpcError> {
        Ok(MockTransaction {
            signature: Signature::new_unique(),
            payer: payer.clone(),
            amount,
        })
    }

    async fn submit_transaction(&self, txn: &MockTransaction) -> Result<(), SolanaRpcError> {
        *self
            .payer_balances
            .lock()
            .await
            .get_mut(&txn.payer)
            .unwrap() -= txn.amount;
        Ok(())
    }

    async fn confirm_transaction(&self, signature: &Signature) -> Result<bool, SolanaRpcError> {
        if *self.confirm_all_txns.lock().await {
            return Ok(true);
        }

        Ok(self.confirmed_txns.lock().await.contains(signature))
    }
}

/// Returns the PDA for the Delegated Data Credits of the given `payer`.
pub fn delegated_data_credits(sub_dao: &Pubkey, payer: &PublicKeyBinary) -> Pubkey {
    let mut hasher = Sha256::new();
    hasher.update(payer.to_string());
    let sha_digest = hasher.finalize();
    let (ddc_key, _) = Pubkey::find_program_address(
        &[
            "delegated_data_credits".as_bytes(),
            sub_dao.as_ref(),
            &sha_digest,
        ],
        &data_credits::ID,
    );
    ddc_key
}
