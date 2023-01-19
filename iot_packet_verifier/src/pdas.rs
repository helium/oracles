use helium_crypto::PublicKey;
use solana_sdk::pubkey::Pubkey;

pub fn delegated_data_credits(gateway: &PublicKey) -> Pubkey {
    let sha_digest = sha256::digest(gateway.to_vec().as_slice());
    let (ddc_key, _) = Pubkey::find_program_address(
        &[
            "delegated_data_credits".as_bytes(),
            &crate::SUB_DAO.to_bytes(),
            sha_digest.as_bytes(),
        ],
        &data_credits::ID,
    );
    ddc_key
}
