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
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
use std::sync::Arc;

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

            let poc_receipt = construct_poc_receipt(lora_valid_poc.beacon_report)?;

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
    let mut poc_witnesses: Vec<BlockchainPocWitnessV1> = Vec::with_capacity(witness_reports.len());
    for witness_report in witness_reports {
        let reward_shares = (witness_report.hex_scale * witness_report.reward_unit)
            .to_u32()
            .unwrap_or_default();

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

fn construct_poc_receipt(beacon_report: LoraValidBeaconReport) -> Result<BlockchainPocReceiptV1> {
    let reward_shares = (beacon_report.hex_scale * beacon_report.reward_unit)
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
