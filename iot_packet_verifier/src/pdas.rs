use helium_crypto::PublicKey;
use solana_sdk::pubkey::Pubkey;

pub fn delegated_data_credits(sub_dao: &Pubkey, payer: &PublicKey) -> Pubkey {
    let sha_digest = sha256::digest(payer.to_vec().as_slice());
    let (ddc_key, _) = Pubkey::find_program_address(
        &[
            "delegated_data_credits".as_bytes(),
            sub_dao.as_ref(),
            sha_digest.as_bytes(),
        ],
        &data_credits::ID,
    );
    ddc_key
}
