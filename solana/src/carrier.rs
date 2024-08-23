use crate::SolanaRpcError;
use async_trait::async_trait;
use helium_anchor_gen::{
    anchor_lang::AccountDeserialize,
    helium_sub_daos,
    mobile_entity_manager::{self, CarrierV0},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

#[async_trait]
pub trait SolanaNetwork {
    async fn fetch_incentive_escrow_fund_percent(
        &self,
        network_name: &str,
    ) -> Result<Decimal, SolanaRpcError>;
}

#[async_trait]
impl SolanaNetwork for SolanaRpc {
    async fn fetch_incentive_escrow_fund_percent(
        &self,
        network_name: &str,
    ) -> Result<Decimal, SolanaRpcError> {
        let (carrier_pda, _) = Pubkey::find_program_address(
            &[
                "carrier".as_bytes(),
                self.sub_dao.as_ref(),
                network_name.as_bytes(),
            ],
            &mobile_entity_manager::ID,
        );
        let carrier_data = self.provider.get_account_data(&carrier_pda).await?;
        let mut carrier_data = carrier_data.as_ref();
        let _carrier = CarrierV0::try_deserialize(&mut carrier_data)?;
        let bps: u16 = 0; // carrier.incentive_escrow_fund_bps (not available right now)
        let percent = Decimal::from(bps) / dec!(10_000);
        Ok(percent)
    }
}

pub struct SolanaRpc {
    provider: RpcClient,
    sub_dao: Pubkey,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    rpc_url: String,
    dnt_mint: String,
}

impl SolanaRpc {
    pub fn new(settings: &Settings) -> Result<Self, SolanaRpcError> {
        let dnt_mint: Pubkey = settings.dnt_mint.parse()?;
        let (sub_dao, _) = Pubkey::find_program_address(
            &["sub_dao".as_bytes(), dnt_mint.as_ref()],
            &helium_sub_daos::ID,
        );
        let provider =
            RpcClient::new_with_commitment(settings.rpc_url.clone(), CommitmentConfig::finalized());
        Ok(Self { provider, sub_dao })
    }
}
