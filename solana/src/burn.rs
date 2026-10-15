use crate::{send_with_retry, GetLastValidBlockHeight, GetSignature, SolanaRpcError};
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
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::{Transaction, TransactionError},
    },
};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::{sync::Arc, time::SystemTime};
use tokio::sync::Mutex;

/// Blocks past a burn's `last_valid_block_height` before a missing finalized
/// status is taken to mean it never landed.
///
/// A transaction can be included up to one block past its last valid height,
/// and the status read may be served by an RPC node that is behind the one
/// that answered the block height. 150 blocks, about a minute, covers both.
pub const BLOCKHASH_EXPIRY_MARGIN: u64 = 150;

/// What a finalized read of the chain says about one signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Finalized without error: the DC were burned.
    Succeeded,
    /// Finalized with an error: nothing was burned, only the fee was paid.
    Failed(TransactionError),
    /// No finalized status. The transaction never landed, or it landed and
    /// has not finalized yet; this read alone cannot tell which.
    NotFinalized,
}

/// Whether a submitted burn spent the payer's DC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BurnOutcome {
    /// The DC were burned.
    Burned,
    /// The DC were not burned, and this transaction can never burn them.
    Failed,
    /// The transaction may still land. Ask again later.
    Pending {
        /// The finalized block height this answer was based on. It is
        /// released once this passes its expiry plus
        /// [`BLOCKHASH_EXPIRY_MARGIN`].
        finalized_block_height: u64,
    },
}

#[async_trait]
pub trait SolanaNetwork: Clone + Send + Sync + 'static {
    type Transaction: GetSignature + GetLastValidBlockHeight + Send + Sync + 'static;

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

    /// The signature's status at finalized commitment, searching history.
    async fn transaction_status(
        &self,
        signature: &Signature,
    ) -> Result<TransactionStatus, SolanaRpcError>;

    /// The chain's block height at finalized commitment.
    async fn finalized_block_height(&self) -> Result<u64, SolanaRpcError>;

    /// Settle a burn that was signed with `last_valid_block_height`.
    ///
    /// [`BurnOutcome::Failed`] is only returned when the transaction can never
    /// land: it finalized with an error, or the finalized block height is past
    /// its expiry (plus [`BLOCKHASH_EXPIRY_MARGIN`]) and it has no finalized
    /// status.
    ///
    /// The block height is read before the status. Any block at or below that
    /// height is already final, so a burn that landed before its expiry shows
    /// up in the status read that follows. Reading them the other way round,
    /// a burn finalizing between the two reads would look like it never
    /// landed.
    ///
    /// This trusts the RPC: a node that reports no status for a transaction
    /// that did land makes a burned transaction look failed.
    async fn burn_outcome(
        &self,
        signature: &Signature,
        last_valid_block_height: u64,
    ) -> Result<BurnOutcome, SolanaRpcError> {
        let finalized_height = self.finalized_block_height().await?;
        let expired =
            finalized_height > last_valid_block_height.saturating_add(BLOCKHASH_EXPIRY_MARGIN);

        Ok(match self.transaction_status(signature).await? {
            TransactionStatus::Succeeded => BurnOutcome::Burned,
            TransactionStatus::Failed(_) => BurnOutcome::Failed,
            TransactionStatus::NotFinalized if expired => BurnOutcome::Failed,
            TransactionStatus::NotFinalized => BurnOutcome::Pending {
                finalized_block_height: finalized_height,
            },
        })
    }
}

/// A signed burn, and the block height its blockhash expires at.
pub struct BurnTransaction {
    pub transaction: Transaction,
    pub last_valid_block_height: u64,
}

impl GetSignature for BurnTransaction {
    fn get_signature(&self) -> &Signature {
        self.transaction.get_signature()
    }
}

impl GetLastValidBlockHeight for BurnTransaction {
    fn last_valid_block_height(&self) -> u64 {
        self.last_valid_block_height
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    #[serde(skip_serializing)]
    rpc_url: String,
    cluster: String,
    /// JSON encoded array of solana keypair
    #[serde(
        deserialize_with = "crate::deserialize_solana_keypair",
        skip_serializing
    )]
    burn_keypair: Keypair,
    dc_mint: String,
    dnt_mint: String,
    #[serde(default, deserialize_with = "from_json_or_struct")]
    payers_to_monitor: Vec<PublicKeyBinary>,
    #[serde(default = "min_priority_fee")]
    min_priority_fee: u64,
}

fn min_priority_fee() -> u64 {
    1
}

fn from_json_or_struct<'de, D, T>(de: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let v = serde_json::Value::deserialize(de)?;
    match v {
        // Environment source provides a *string*; parse that string as JSON
        serde_json::Value::String(s) => serde_json::from_str(&s).map_err(serde::de::Error::custom),

        // Already a map/array/etc.; just deserialize it normally
        other => serde_json::from_value(other).map_err(serde::de::Error::custom),
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
        let provider =
            RpcClient::new_with_commitment(settings.rpc_url.clone(), CommitmentConfig::finalized());
        let program_cache = BurnProgramCache::new(&provider, dc_mint, dnt_mint).await?;
        if program_cache.dc_burn_authority != settings.burn_keypair.pubkey() {
            return Err(SolanaRpcError::InvalidKeypair);
        }
        Ok(Arc::new(Self {
            cluster: settings.cluster.clone(),
            provider: Arc::new(provider),
            program_cache,
            keypair: settings.burn_keypair.to_bytes(),
            payers_to_monitor: settings.payers_to_monitor.clone(),
            priority_fee: PriorityFee::default(),
            min_priority_fee: settings.min_priority_fee,
        }))
    }
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    type Transaction = BurnTransaction;

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
                std::rc::Rc::new(Keypair::try_from(&self.keypair[..]).unwrap()),
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

        let (blockhash, last_valid_block_height) = self
            .provider
            .get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
            .await?;
        let signer = Keypair::try_from(&self.keypair[..]).unwrap();

        Ok(BurnTransaction {
            transaction: Transaction::new_signed_with_payer(
                &instructions,
                Some(&signer.pubkey()),
                &[&signer],
                blockhash,
            ),
            last_valid_block_height,
        })
    }

    async fn submit_transaction(&self, tx: &Self::Transaction) -> Result<(), SolanaRpcError> {
        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            ..Default::default()
        };
        match send_with_retry!(self
            .provider
            .send_and_confirm_transaction_with_spinner_and_config(
                &tx.transaction,
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

    async fn transaction_status(
        &self,
        signature: &Signature,
    ) -> Result<TransactionStatus, SolanaRpcError> {
        // The client drops a status that hasn't reached the requested
        // commitment, so `None` covers both "not found" and "not finalized".
        let status = self
            .provider
            .get_signature_status_with_commitment_and_history(
                signature,
                CommitmentConfig::finalized(),
                true,
            )
            .await?;

        Ok(match status {
            Some(Ok(())) => TransactionStatus::Succeeded,
            Some(Err(err)) => TransactionStatus::Failed(err),
            None => TransactionStatus::NotFinalized,
        })
    }

    async fn finalized_block_height(&self) -> Result<u64, SolanaRpcError> {
        Ok(self
            .provider
            .get_block_height_with_commitment(CommitmentConfig::finalized())
            .await?)
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
    Transaction(BurnTransaction),
}

impl GetSignature for PossibleTransaction {
    fn get_signature(&self) -> &Signature {
        match self {
            Self::NoTransaction(ref sig) => sig,
            Self::Transaction(ref txn) => txn.get_signature(),
        }
    }
}

impl GetLastValidBlockHeight for PossibleTransaction {
    fn last_valid_block_height(&self) -> u64 {
        match self {
            // There is no blockhash to expire. Nothing reads this: resolving a
            // burn panics when Solana is disabled.
            Self::NoTransaction(_) => 0,
            Self::Transaction(ref txn) => txn.last_valid_block_height(),
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

    async fn transaction_status(
        &self,
        signature: &Signature,
    ) -> Result<TransactionStatus, SolanaRpcError> {
        if let Some(ref rpc) = self {
            rpc.transaction_status(signature).await
        } else {
            panic!("We will not confirm transactions when Solana is disabled");
        }
    }

    async fn finalized_block_height(&self) -> Result<u64, SolanaRpcError> {
        if let Some(ref rpc) = self {
            rpc.finalized_block_height().await
        } else {
            panic!("We will not confirm transactions when Solana is disabled");
        }
    }
}

/// How many blocks a mock burn's blockhash lives, matching Solana's
/// `MAX_PROCESSING_AGE`.
const MOCK_BLOCKHASH_LIFETIME: u64 = 150;

pub struct MockTransaction {
    pub signature: Signature,
    pub payer: PublicKeyBinary,
    pub amount: u64,
    pub last_valid_block_height: u64,
}

impl GetSignature for MockTransaction {
    fn get_signature(&self) -> &Signature {
        &self.signature
    }
}

impl GetLastValidBlockHeight for MockTransaction {
    fn last_valid_block_height(&self) -> u64 {
        self.last_valid_block_height
    }
}

#[derive(Clone)]
pub struct TestSolanaClientMap {
    payer_balances: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
    confirm_all_txns: Arc<Mutex<bool>>,
    confirmed_txns: Arc<Mutex<HashSet<Signature>>>,
    failed_txns: Arc<Mutex<HashSet<Signature>>>,
    block_height: Arc<Mutex<u64>>,
    block_height_step: Arc<Mutex<u64>>,
    failing_submissions: Arc<Mutex<bool>>,
    failing_height_reads: Arc<Mutex<usize>>,
}

impl Default for TestSolanaClientMap {
    fn default() -> Self {
        Self {
            payer_balances: Default::default(),
            confirm_all_txns: Arc::new(Mutex::new(true)),
            confirmed_txns: Default::default(),
            failed_txns: Default::default(),
            block_height: Default::default(),
            block_height_step: Default::default(),
            failing_submissions: Default::default(),
            failing_height_reads: Default::default(),
        }
    }
}

impl TestSolanaClientMap {
    pub async fn with(items: &[(&PublicKeyBinary, u64)]) -> Self {
        let me = Self::default();

        for (payer, amount) in items {
            me.insert(payer, *amount).await;
        }

        me
    }

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

    /// Mark a transaction as finalized with an error.
    pub async fn add_failed(&self, signature: Signature) {
        *self.confirm_all_txns.lock().await = false;
        self.failed_txns.lock().await.insert(signature);
    }

    /// Move the finalized block height. New burns expire
    /// [`MOCK_BLOCKHASH_LIFETIME`] blocks after it.
    pub async fn set_block_height(&self, height: u64) {
        *self.block_height.lock().await = height;
    }

    /// Advance the finalized block height by `step` every time it is read, so
    /// a burn that never finalizes eventually expires.
    pub async fn advance_block_height_by(&self, step: u64) {
        *self.block_height_step.lock().await = step;
    }

    /// Make every later submission return an error. Whether the burn landed
    /// anyway is up to [`Self::add_confirmed`] and [`Self::add_failed`]; with
    /// neither called, every burn reads as succeeded.
    pub async fn fail_submissions(&self) {
        *self.failing_submissions.lock().await = true;
    }

    /// Make the next `count` block height reads return an RPC error.
    pub async fn fail_height_reads(&self, count: usize) {
        *self.failing_height_reads.lock().await = count;
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
            last_valid_block_height: *self.block_height.lock().await + MOCK_BLOCKHASH_LIFETIME,
        })
    }

    async fn submit_transaction(&self, txn: &MockTransaction) -> Result<(), SolanaRpcError> {
        if *self.failing_submissions.lock().await {
            return Err(SolanaRpcError::Test("submission failed".to_string()));
        }

        *self
            .payer_balances
            .lock()
            .await
            .get_mut(&txn.payer)
            .unwrap() -= txn.amount;
        Ok(())
    }

    async fn transaction_status(
        &self,
        signature: &Signature,
    ) -> Result<TransactionStatus, SolanaRpcError> {
        if *self.confirm_all_txns.lock().await
            || self.confirmed_txns.lock().await.contains(signature)
        {
            return Ok(TransactionStatus::Succeeded);
        }

        if self.failed_txns.lock().await.contains(signature) {
            return Ok(TransactionStatus::Failed(
                TransactionError::InsufficientFundsForFee,
            ));
        }

        Ok(TransactionStatus::NotFinalized)
    }

    async fn finalized_block_height(&self) -> Result<u64, SolanaRpcError> {
        let mut failing = self.failing_height_reads.lock().await;
        if *failing > 0 {
            *failing -= 1;
            return Err(SolanaRpcError::Test("block height read failed".to_string()));
        }

        let mut height = self.block_height.lock().await;
        let current = *height;
        *height += *self.block_height_step.lock().await;
        Ok(current)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn payer() -> PublicKeyBinary {
        PublicKeyBinary::from(vec![1])
    }

    #[tokio::test]
    async fn burn_outcome_pending_until_past_expiry_margin() {
        let solana = TestSolanaClientMap::default();
        solana.add_confirmed(Signature::new_unique()).await;
        let txn = solana.make_burn_transaction(&payer(), 1).await.unwrap();
        let expires_at = txn.last_valid_block_height + BLOCKHASH_EXPIRY_MARGIN;

        solana.set_block_height(expires_at).await;
        let outcome = solana
            .burn_outcome(&txn.signature, txn.last_valid_block_height)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            BurnOutcome::Pending {
                finalized_block_height: expires_at
            }
        );

        solana.set_block_height(expires_at + 1).await;
        let outcome = solana
            .burn_outcome(&txn.signature, txn.last_valid_block_height)
            .await
            .unwrap();
        assert_eq!(outcome, BurnOutcome::Failed);
    }

    #[tokio::test]
    async fn burn_outcome_finalized_error_is_failed_before_expiry() {
        let solana = TestSolanaClientMap::default();
        let txn = solana.make_burn_transaction(&payer(), 1).await.unwrap();
        solana.add_failed(txn.signature).await;

        let outcome = solana
            .burn_outcome(&txn.signature, txn.last_valid_block_height)
            .await
            .unwrap();
        assert_eq!(outcome, BurnOutcome::Failed);
    }

    #[tokio::test]
    async fn burn_outcome_succeeded_is_burned_after_expiry() {
        let solana = TestSolanaClientMap::default();
        let txn = solana.make_burn_transaction(&payer(), 1).await.unwrap();
        solana.add_confirmed(txn.signature).await;
        solana
            .set_block_height(txn.last_valid_block_height + BLOCKHASH_EXPIRY_MARGIN + 1)
            .await;

        let outcome = solana
            .burn_outcome(&txn.signature, txn.last_valid_block_height)
            .await
            .unwrap();
        assert_eq!(outcome, BurnOutcome::Burned);
    }
}
