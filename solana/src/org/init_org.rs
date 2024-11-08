use helium_anchor_gen::{
    anchor_lang::{AccountDeserialize, ToAccountMetas},
    iot_routing_manager::{self, accounts, instruction},
};
use serde::Deserialize;
use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey,
    signature::Signature,
    transaction::{Transaction, TransactionError},
};
use std::str::FromStr;

pub fn make_init_org_txn(
    oui: u64,
    owner: &Pubkey,
    fee_payer: &Pubkey,
    recent_blockhash: &Hash,
) -> Result<Vec<u8>, SolanaOrgError> {
    let program_id = iot_routing_manager::id();

    // Define seeds for deriving the organization account PDA
    let org_account_seed = format!("org_{}", oui);
    let seeds = &[org_account_seed.as_bytes()];

    // Derive the organization account PDA
    let (org_account, _bump) = Pubkey::find_program_address(seeds, &program_id);

    // Create the InitializeOrgV0 instruction
    let init_org_instruction =
        InitializeOrgV0 { oui, owner: *owner }.instruction(fee_payer, &org_account);

    // Assemble the transaction with the instruction
    let mut transaction = Transaction::new_unsigned(&[init_org_instruction]);

    // Set the recent blockhash and fee payer
    transaction.recent_blockhash = *recent_blockhash;
    transaction.fee_payer = Some(*fee_payer);

    // Serialize the transaction without signatures
    let serialized_tx = bincode::serialize(&transaction)?;

    Ok(serialized_tx)
}
