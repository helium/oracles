pub mod balance_monitor;

use anchor_client::{RequestBuilder, RequestNamespace};
use anchor_lang::AccountDeserialize;
use async_trait::async_trait;
use data_credits::{accounts, instruction};
use helium_crypto::PublicKeyBinary;
use helium_sub_daos::{DaoV0, SubDaoV0};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use solana_client::{
    client_error::ClientError, nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    program_pack::Pack,
    pubkey::{ParsePubkeyError, Pubkey},
    signature::{read_keypair_file, Keypair, ParseSignatureError, Signature},
    signer::Signer,
    transaction::Transaction,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
    EncodedTransactionWithStatusMeta, UiInstruction, UiParsedInstruction, UiTransactionStatusMeta,
};
use std::{collections::HashMap, str::FromStr};
use std::{
    sync::Arc,
    time::{SystemTime, SystemTimeError},
};
use tokio::sync::Mutex;

#[async_trait]
pub trait SolanaNetwork: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error>;

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error>;

    async fn latest_transaction(&self) -> Result<Signature, Self::Error>;

    async fn has_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        since: &Signature,
    ) -> Result<bool, Self::Error>;
}

#[derive(thiserror::Error, Debug)]
pub enum SolanaRpcError {
    #[error("Solana rpc error: {0}")]
    RpcClientError(#[from] ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(Box<anchor_lang::error::Error>),
    #[error("Solana program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
    #[error("Parse pubkey error: {0}")]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("Parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Parse signature error: {0}")]
    ParseSignatureError(#[from] ParseSignatureError),
    #[error("DC burn authority does not match keypair")]
    InvalidKeypair,
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("Failed to read keypair file")]
    FailedToReadKeypairError,
    #[error("Crypto error: {0}")]
    Crypto(#[from] helium_crypto::Error),
    #[error("No transactions found")]
    NoTransactionsFound,
}

impl From<anchor_lang::error::Error> for SolanaRpcError {
    fn from(err: anchor_lang::error::Error) -> Self {
        Self::AnchorError(Box::new(err))
    }
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

pub struct SolanaRpc {
    provider: RpcClient,
    program_cache: BurnProgramCache,
    cluster: String,
    keypair: [u8; 64],
    payers_to_monitor: Vec<PublicKeyBinary>,
}

impl SolanaRpc {
    pub async fn new(settings: &Settings) -> Result<Arc<Self>, SolanaRpcError> {
        let dc_mint = settings.dc_mint.parse()?;
        let dnt_mint = settings.dnt_mint.parse()?;
        let Ok(keypair) = read_keypair_file(&settings.burn_keypair) else {
            return Err(SolanaRpcError::FailedToReadKeypairError);
        };
        let provider =
            RpcClient::new_with_commitment(settings.rpc_url.clone(), CommitmentConfig::finalized());
        let program_cache = BurnProgramCache::new(&provider, dc_mint, dnt_mint).await?;
        if program_cache.dc_burn_authority != keypair.pubkey() {
            return Err(SolanaRpcError::InvalidKeypair);
        }
        Ok(Arc::new(Self {
            cluster: settings.cluster.clone(),
            provider,
            program_cache,
            keypair: keypair.to_bytes(),
            payers_to_monitor: settings.payers_to_monitor()?,
        }))
    }
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    type Error = SolanaRpcError;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        let ddc_key = delegated_data_credits(&self.program_cache.sub_dao, payer);
        let (escrow_account, _) = Pubkey::find_program_address(
            &["escrow_dc_account".as_bytes(), &ddc_key.to_bytes()],
            &data_credits::ID,
        );
        let Ok(account_data) = self.provider.get_account_data(&escrow_account).await else {
            // If the account is empty, it has no DC
            tracing::info!(%payer, "Account not found, therefore no balance");
            return Ok(0);
        };
        let account_layout = spl_token::state::Account::unpack(account_data.as_slice())?;

        if self.payers_to_monitor.contains(payer) {
            metrics::gauge!(
                "balance",
                account_layout.amount as f64,
                "payer" => payer.to_string()
            );
        }

        Ok(account_layout.amount)
    }

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
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

        let instructions = {
            let request = RequestBuilder::from(
                data_credits::id(),
                &self.cluster,
                std::rc::Rc::new(Keypair::from_bytes(&self.keypair).unwrap()),
                Some(CommitmentConfig::confirmed()),
                RequestNamespace::Global,
            );

            let accounts = accounts::BurnDelegatedDataCreditsV0 {
                sub_dao_epoch_info,
                dao: self.program_cache.dao,
                sub_dao: self.program_cache.sub_dao,
                account_payer: self.program_cache.account_payer,
                data_credits: self.program_cache.data_credits,
                delegated_data_credits: delegated_data_credits(&self.program_cache.sub_dao, payer),
                token_program: spl_token::id(),
                helium_sub_daos_program: helium_sub_daos::id(),
                system_program: solana_program::system_program::id(),
                dc_burn_authority: self.program_cache.dc_burn_authority,
                dc_mint: self.program_cache.dc_mint,
                escrow_account,
                registrar: self.program_cache.registrar,
            };
            let args = instruction::BurnDelegatedDataCreditsV0 {
                args: data_credits::BurnDelegatedDataCreditsArgsV0 { amount },
            };

            // As far as I can tell, the instructions function does not actually have any
            // error paths.
            request
                .accounts(accounts)
                .args(args)
                .instructions()
                .unwrap()
        };

        let blockhash = self.provider.get_latest_blockhash().await?;
        let signer = Keypair::from_bytes(&self.keypair).unwrap();

        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&signer.pubkey()),
            &[&signer],
            blockhash,
        );

        let signature = self.provider.send_and_confirm_transaction(&tx).await?;

        tracing::info!(
            transaction = %signature,
            "Successfully burned data credits",
        );

        Ok(())
    }

    async fn latest_transaction(&self) -> Result<Signature, Self::Error> {
        let signer = Keypair::from_bytes(&self.keypair).unwrap();
        let mut signatures = self
            .provider
            .get_signatures_for_address_with_config(
                &signer.pubkey(),
                GetConfirmedSignaturesForAddress2Config {
                    before: None,
                    until: None,
                    limit: Some(1),
                    commitment: Some(CommitmentConfig::confirmed()),
                },
            )
            .await?;
        Ok(signatures
            .pop()
            .ok_or(SolanaRpcError::NoTransactionsFound)?
            .signature
            .parse()?)
    }

    async fn has_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        since: &Signature,
    ) -> Result<bool, Self::Error> {
        let signer = Keypair::from_bytes(&self.keypair).unwrap();
        let ddc_key = delegated_data_credits(&self.program_cache.sub_dao, payer);
        let (escrow_account, _) = Pubkey::find_program_address(
            &["escrow_dc_account".as_bytes(), &ddc_key.to_bytes()],
            &data_credits::ID,
        );
        let signatures = self
            .provider
            .get_signatures_for_address_with_config(
                &signer.pubkey(),
                GetConfirmedSignaturesForAddress2Config {
                    before: None,
                    until: Some(*since),
                    limit: None,
                    commitment: Some(CommitmentConfig::confirmed()),
                },
            )
            .await?;
        for signature in signatures.into_iter() {
            let signature: Signature = signature.signature.parse()?;
            if signature == *since {
                continue;
            }
            let transaction = self
                .provider
                .get_transaction(
                    &signature,
                    solana_transaction_status::UiTransactionEncoding::JsonParsed,
                )
                .await?;
            let Some(burn_info) = extract_burn_info(transaction)? else {
                continue;
            };
            if burn_info.account == escrow_account && burn_info.amount == amount {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Cached pubkeys for the burn program
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
            let sub_dao = SubDaoV0::try_deserialize(&mut account_data)?;
            (sub_dao.dao, sub_dao.dc_burn_authority)
        };
        let registrar = {
            let account_data = provider.get_account_data(&dao).await?;
            let mut account_data = account_data.as_ref();
            DaoV0::try_deserialize(&mut account_data)?.registrar
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

const FIXED_BALANCE: u64 = 1_000_000_000;

#[async_trait]
impl SolanaNetwork for Option<Arc<SolanaRpc>> {
    type Error = SolanaRpcError;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.payer_balance(payer).await
        } else {
            Ok(FIXED_BALANCE)
        }
    }

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        if let Some(ref rpc) = self {
            rpc.burn_data_credits(payer, amount).await
        } else {
            Ok(())
        }
    }

    async fn latest_transaction(&self) -> Result<Signature, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.latest_transaction().await
        } else {
            Ok(Signature::new_unique())
        }
    }

    async fn has_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        since: &Signature,
    ) -> Result<bool, Self::Error> {
        if let Some(ref rpc) = self {
            rpc.has_burn_transaction(payer, amount, since).await
        } else {
            Ok(false)
        }
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

#[derive(Debug)]
struct BurnInfo {
    account: Pubkey,
    amount: u64,
}

#[derive(Debug, Deserialize)]
struct Instruction {
    #[serde(rename = "type")]
    instr_type: String,
    info: InstrInfo,
}

#[derive(Debug, Deserialize)]
struct InstrInfo {
    account: String,
    amount: String,
}

fn extract_burn_info(
    transaction: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<Option<BurnInfo>, SolanaRpcError> {
    match transaction {
        EncodedConfirmedTransactionWithStatusMeta {
            transaction:
                EncodedTransactionWithStatusMeta {
                    meta:
                        Some(UiTransactionStatusMeta {
                            inner_instructions: OptionSerializer::Some(instructions),
                            ..
                        }),
                    ..
                },
            ..
        } => {
            for instruction in instructions {
                for instruction in instruction.instructions {
                    if let UiInstruction::Parsed(UiParsedInstruction::Parsed(parsed_instruction)) =
                        instruction
                    {
                        let Ok(instruction) = serde_json::from_value::<Instruction>(parsed_instruction.parsed) else {
                                continue;
                            };
                        if instruction.instr_type != "burn" {
                            continue;
                        }
                        let amount: u64 = instruction.info.amount.parse()?;
                        let account: Pubkey = instruction.info.account.parse()?;
                        return Ok(Some(BurnInfo { account, amount }));
                    }
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

#[derive(Clone)]
pub struct MockSolanaNetwork {
    pub ledger: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
    pub transactions: Arc<Mutex<Vec<MockTransaction>>>,
}

impl MockSolanaNetwork {
    pub fn new(ledger: HashMap<PublicKeyBinary, u64>) -> Self {
        Self {
            ledger: Arc::new(Mutex::new(ledger)),
            transactions: Arc::new(Mutex::new(vec![MockTransaction {
                amount: 0,
                account: PublicKeyBinary::from(Vec::new()),
                signature: Signature::new_unique(),
            }])),
        }
    }
}

#[derive(Clone)]
pub struct MockTransaction {
    pub amount: u64,
    pub account: PublicKeyBinary,
    pub signature: Signature,
}

#[derive(thiserror::Error, Debug)]
pub enum MockSolanaError {
    #[error("No transactions found")]
    NoTransactionsFound,
}

#[async_trait]
impl SolanaNetwork for MockSolanaNetwork {
    type Error = MockSolanaError;

    async fn payer_balance(&self, payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
        Ok(*self.ledger.lock().await.get(payer).unwrap())
    }

    async fn burn_data_credits(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        self.transactions.lock().await.push(MockTransaction {
            amount,
            account: payer.clone(),
            signature: Signature::new_unique(),
        });
        *self.ledger.lock().await.get_mut(payer).unwrap() -= amount;
        Ok(())
    }

    async fn latest_transaction(&self) -> Result<Signature, Self::Error> {
        Ok(self
            .transactions
            .lock()
            .await
            .last()
            .ok_or(MockSolanaError::NoTransactionsFound)?
            .signature)
    }

    async fn has_burn_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        since: &Signature,
    ) -> Result<bool, Self::Error> {
        let transactions = self.transactions.lock().await;
        let transaction_iter = transactions.iter();
        for transaction in transaction_iter.as_ref() {
            if transaction.signature == *since {
                break;
            }
        }
        for transaction in transaction_iter {
            if transaction.account == *payer && transaction.amount == amount {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
