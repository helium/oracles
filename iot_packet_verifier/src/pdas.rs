//! Functions for returning Program Derived Addresses in order to complete
//! actions on the Solana chain.

use helium_crypto::PublicKeyBinary;
use sha2::{Digest, Sha256};
use solana_sdk::pubkey::Pubkey;

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
