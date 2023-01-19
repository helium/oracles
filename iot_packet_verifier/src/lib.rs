use solana_sdk::pubkey::Pubkey;

pub mod balances;
pub mod burner;
pub mod daemon;
pub mod ingest;
pub mod pdas;
pub mod settings;

pub const SUB_DAO: Pubkey = Pubkey::new_from_array([0; 32]);
pub const DNT_MINT: Pubkey = Pubkey::new_from_array([0; 32]);
pub const HNT_MINT: Pubkey = Pubkey::new_from_array([0; 32]);
pub const DC_MINT: Pubkey = Pubkey::new_from_array([0; 32]);
