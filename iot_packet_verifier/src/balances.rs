use anchor_client::Client;
use anchor_lang::AccountDeserialize;
use data_credits::{
    accounts::BurnDelegatedDataCreditsV0, instructions::burn_delegated_data_credits_v0,
    DelegatedDataCreditsV0,
};
use helium_crypto::PublicKey;
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_program::{program_error::ProgramError, program_pack::Pack};
use solana_sdk::{pubkey::Pubkey, signature::Keypair};

pub struct Balances {
    pub pid: Pubkey,
    pub provider: RpcClient,
}

const SUB_DAO: Pubkey = Pubkey::new_from_array([0; 32]);

#[derive(thiserror::Error, Debug)]
pub enum DebitError {
    #[error("solana rpc error: {0}")]
    RpcClientError(#[from] ClientError),
    #[error("anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("solana program error: {0}")]
    ProgramError(#[from] ProgramError),
}

impl Balances {
    pub fn new(url: &str, pid: Pubkey) -> Self {
        Self {
            pid,
            provider: RpcClient::new(url.to_string()),
        }
    }

    /// Debit the balance by a given amount
    pub async fn debit(&self, gateway: &PublicKey, amount: u64) -> Result<bool, DebitError> {
        let ddc_pda_key = "delegated_data_credits";
        let sha_digest = sha256::digest(gateway.to_vec().as_slice());
        let (ddc_key, _) = Pubkey::find_program_address(
            &[
                ddc_pda_key.as_bytes(),
                &SUB_DAO.to_bytes(),
                sha_digest.as_bytes(),
            ],
            &data_credits::ID,
        );

        let ddc = self.delegated_data_credits(&ddc_key).await?;
        let account_balance = self.account_balance(&ddc).await?;
        let suficient = account_balance >= amount;

        if suficient {
            // Burn the credits:
            let counter = Keypair::new();
            let authority = todo!();

            let accounts = BurnDelegatedDataCreditsV0 { ..todo!() };

            let args = burn_delegated_data_credits_v0::BurnDelegatedDataCreditsArgsV0 { amount };

            // We have to manually create and send the program in order for it to be async...
            // todo...
        }

        Ok(suficient)
    }

    pub async fn delegated_data_credits(
        &self,
        ddkey: &Pubkey,
    ) -> Result<DelegatedDataCreditsV0, DebitError> {
        let account_data = self.provider.get_account_data(ddkey).await?;
        let mut account_data = account_data.as_ref();
        Ok(DelegatedDataCreditsV0::try_deserialize(&mut account_data)?)
    }

    pub async fn account_balance(&self, ddc: &DelegatedDataCreditsV0) -> Result<u64, DebitError> {
        let account_data = self.provider.get_account_data(&ddc.escrow_account).await?;
        let account_layout = spl_token::state::Account::unpack(account_data.as_slice())?;
        Ok(account_layout.amount)
    }
}
