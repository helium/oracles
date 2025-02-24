use std::str::FromStr;

use crate::settings::Settings;
use anyhow::{bail, Context, Result};
use helium_crypto::PublicKeyBinary;
use solana::{
    burn::{SolanaNetwork, SolanaRpc},
    sender::{SenderError, SenderResult, TxnStore},
    Transaction,
};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Router key from which to burn delegated DC from.
    payer: String,
    /// Use a dummy TxnStore for sending
    #[arg(long)]
    use_txn_store: bool,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let Some(ref solana_settings) = settings.solana else {
            bail!("Missing solana section in settings");
        };
        let solana = SolanaRpc::new(solana_settings, solana::SubDao::Mobile).await?;

        let payer = PublicKeyBinary::from_str(&self.payer).context("Payer -> PublicKeyBinary")?;
        let txn = solana
            .make_burn_transaction(&payer, 1)
            .await
            .context("making burn txn")?;

        if self.use_txn_store {
            println!("sending with txn store...");
            let store = DummyTxnStore;
            let res = solana.submit_transaction(&txn, &store).await;
            println!("txn store: {res:?}");
        } else {
            println!("sending with send_and_confirm_transaction");
            let res = solana.provider.send_and_confirm_transaction(&txn).await;
            println!("send_and_confirm_transaction: {res:?}");
        }

        Ok(())
    }
}

struct DummyTxnStore;

#[async_trait::async_trait]
impl TxnStore for DummyTxnStore {
    async fn on_prepared(&self, txn: &Transaction) -> SenderResult<()> {
        let signature = txn.get_signature();
        println!("prepared... {signature}");
        Ok(())
    }

    async fn on_sent(&self, txn: &Transaction) {
        let signature = txn.get_signature();
        println!("sent... {signature}");
    }

    async fn on_sent_retry(&self, txn: &Transaction, attempt: usize) {
        let signature = txn.get_signature();
        println!("retrying ({attempt})... {signature}");
    }

    async fn on_finalized(&self, txn: &Transaction) {
        let signature = txn.get_signature();
        println!("finalized... {signature}");
    }

    async fn on_error(&self, txn: &Transaction, err: SenderError) {
        let signature = txn.get_signature();
        println!("txn error: {signature}\n{err:?}");
    }
}
