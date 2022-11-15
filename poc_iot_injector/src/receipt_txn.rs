use crate::{Error, Result};
use file_store::{
    lora_valid_poc::{LoraValidBeaconReport, LoraValidPoc, LoraValidWitnessReport},
    traits::MsgDecode,
};
use helium_crypto::{Keypair, Sign};
use helium_proto::{
    blockchain_txn::Txn, BlockchainPocPathElementV1, BlockchainPocReceiptV1,
    BlockchainPocWitnessV1, BlockchainTxn, BlockchainTxnPocReceiptsV2, Message,
};
use rust_decimal::{prelude::ToPrimitive, Decimal, MathematicalOps};
use rust_decimal_macros::dec;
use sha2::{Digest, Sha256};
use std::sync::Arc;

// TODO: These should ideally be coming in from some configuration service
const WITNESS_REDUNDANCY: u32 = 4;
const POC_REWARD_DECAY_RATE: Decimal = dec!(0.8);
const HIP15_TX_REWARD_UNIT_CAP: Decimal = Decimal::TWO;

type PocPath = Vec<BlockchainPocPathElementV1>;

#[derive(Clone, Debug)]
pub struct TxnDetails {
    pub txn: BlockchainTxn,
    pub hash: Vec<u8>,
    pub hash_b64_url: String,
}

pub fn handle_report_msg(
    msg: prost::bytes::BytesMut,
    keypair: Arc<Keypair>,
    timestamp: i64,
) -> Result<TxnDetails> {
    // Path is always single element, till we decide to change it at some point.
    let mut path: PocPath = Vec::with_capacity(1);

    match LoraValidPoc::decode(msg) {
        Ok(lora_valid_poc) => {
            let poc_witnesses = construct_poc_witnesses(lora_valid_poc.witness_reports)?;

            let poc_receipt =
                construct_poc_receipt(lora_valid_poc.beacon_report, poc_witnesses.len() as u32)?;

            // TODO: Double check whether the gateway in the poc_receipt is challengee?
            let path_element =
                construct_path_element(poc_receipt.clone().gateway, poc_receipt, poc_witnesses)?;

            path.push(path_element);

            let (bare_txn, hash, hash_b64_url) = construct_bare_txn(path, timestamp, &keypair)?;
            Ok(TxnDetails {
                txn: wrap_txn(bare_txn),
                hash,
                hash_b64_url,
            })
        }
        Err(e) => {
            tracing::error!("error: {:?}", e);
            Err(Error::TxnConstruction)
        }
    }
}

fn wrap_txn(txn: BlockchainTxnPocReceiptsV2) -> BlockchainTxn {
    BlockchainTxn {
        txn: Some(Txn::PocReceiptsV2(txn)),
    }
}

fn construct_bare_txn(
    path: PocPath,
    timestamp: i64,
    keypair: &Keypair,
) -> Result<(BlockchainTxnPocReceiptsV2, Vec<u8>, String)> {
    let mut txn = BlockchainTxnPocReceiptsV2 {
        challenger: keypair.public_key().to_vec(),
        secret: vec![],
        onion_key_hash: vec![],
        path,
        fee: 0,
        signature: vec![],
        block_hash: vec![],
        timestamp: timestamp as u64,
    };

    txn.signature = sign_txn(&txn, keypair)?;
    let (txn_hash, txn_hash_b64url) = hash_txn(&txn);
    Ok((txn, txn_hash, txn_hash_b64url))
}

fn construct_path_element(
    challengee: Vec<u8>,
    poc_receipt: BlockchainPocReceiptV1,
    poc_witnesses: Vec<BlockchainPocWitnessV1>,
) -> Result<BlockchainPocPathElementV1> {
    let path_element = BlockchainPocPathElementV1 {
        challengee,
        receipt: Some(poc_receipt),
        witnesses: poc_witnesses,
    };
    Ok(path_element)
}

fn construct_poc_witnesses(
    witness_reports: Vec<LoraValidWitnessReport>,
) -> Result<Vec<BlockchainPocWitnessV1>> {
    let num_witnesses = witness_reports.len();
    let mut poc_witnesses: Vec<BlockchainPocWitnessV1> = Vec::with_capacity(num_witnesses);
    for witness_report in witness_reports {
        let reward_unit = poc_challengee_reward_unit(num_witnesses as u32)?;
        let reward_shares = witness_report.hex_scale * reward_unit;
        let reward_shares = reward_shares.to_u32().unwrap_or_default();

        // NOTE: channel is irrelevant now
        let poc_witness = BlockchainPocWitnessV1 {
            gateway: witness_report.report.pub_key.to_vec(),
            timestamp: witness_report.report.timestamp.timestamp() as u64,
            signal: witness_report.report.signal,
            packet_hash: witness_report.report.data,
            signature: witness_report.report.signature,
            snr: witness_report.report.snr as f32,
            frequency: hz_to_mhz(witness_report.report.frequency),
            datarate: witness_report.report.datarate.to_string(),
            channel: 0,
            reward_shares,
        };

        poc_witnesses.push(poc_witness)
    }
    Ok(poc_witnesses)
}

fn hz_to_mhz(freq_hz: u64) -> f32 {
    // MHZTOHZ = 1000000;
    let freq_mhz = Decimal::from(freq_hz) / Decimal::from(1000000);
    freq_mhz.to_f32().unwrap_or_default()
}

fn construct_poc_receipt(
    beacon_report: LoraValidBeaconReport,
    num_witnesses: u32,
) -> Result<BlockchainPocReceiptV1> {
    let reward_unit = poc_challengee_reward_unit(num_witnesses)?;
    let reward_shares = (beacon_report.hex_scale * reward_unit)
        .to_u32()
        .unwrap_or_default();

    // NOTE: signal, origin, snr and addr_hash are irrelevant now
    Ok(BlockchainPocReceiptV1 {
        gateway: beacon_report.report.pub_key.to_vec(),
        timestamp: beacon_report.report.timestamp.timestamp() as u64,
        signal: 0,
        data: beacon_report.report.data,
        origin: 0,
        signature: beacon_report.report.signature,
        snr: 0.0,
        frequency: hz_to_mhz(beacon_report.report.frequency),
        channel: beacon_report.report.channel,
        datarate: beacon_report.report.datarate.to_string(),
        tx_power: beacon_report.report.tx_power,
        addr_hash: vec![],
        reward_shares,
    })
}

fn hash_txn(txn: &BlockchainTxnPocReceiptsV2) -> (Vec<u8>, String) {
    let mut txn = txn.clone();
    txn.signature = vec![];
    let digest = Sha256::digest(txn.encode_to_vec()).to_vec();
    (
        digest.clone(),
        base64::encode_config(&digest, base64::URL_SAFE_NO_PAD),
    )
}

fn sign_txn(txn: &BlockchainTxnPocReceiptsV2, keypair: &Keypair) -> Result<Vec<u8>> {
    let mut txn = txn.clone();
    txn.signature = vec![];
    Ok(keypair.sign(&txn.encode_to_vec())?)
}

// TODO: Arguably this functionality should live in the poc-iot-verifier.
// Once the hex_scale calculations are done, poc-iot-verifier should also attach the actual reward
// share in the generated report.
fn poc_challengee_reward_unit(num_witnesses: u32) -> Result<Decimal> {
    if num_witnesses == 0 {
        Ok(Decimal::ZERO)
    } else if num_witnesses < WITNESS_REDUNDANCY {
        Ok(Decimal::from(WITNESS_REDUNDANCY / num_witnesses))
    } else {
        let exp = WITNESS_REDUNDANCY - num_witnesses;
        if let Some(to_sub) = POC_REWARD_DECAY_RATE.checked_powu(exp as u64) {
            let unnormalized = Decimal::TWO - to_sub;
            let normalized_unit = normalize_reward_unit(unnormalized);
            Ok(normalized_unit)
        } else {
            Err(Error::InvalidExponent(exp.to_string()))
        }
    }
}

fn normalize_reward_unit(unnormalized: Decimal) -> Decimal {
    if unnormalized >= HIP15_TX_REWARD_UNIT_CAP {
        HIP15_TX_REWARD_UNIT_CAP
    } else {
        unnormalized
    }
}
