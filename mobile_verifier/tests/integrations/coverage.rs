use std::str::FromStr;

use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use mobile_verifier::{
    coverage::{CoveredHexStream, HexCoverage},
    heartbeats::KeyType,
    seniority::Seniority,
};
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test(fixtures("covered_stream"))]
async fn test_covered_hex_stream(pool: PgPool) {
    let mut txn = pool.begin().await.unwrap();

    let wifi_pub_key: PublicKeyBinary = "1trSuseow771kqR8Muvj8rK3SbM26jN3o8GuDEjuUMEWZp7WzvexMtZwNP1jH7BMvaUgpb2fWQCxBgCm4UCFbHn6x6ApFzXoaUTb6SMSYYc6uwUQiHsa9vFC8LpPEwo6bv7rjKddgSxxtRhNojuck5dAXkAuWaxW9fW1vxwSqAq7WKEMnRMfjMzbpC1yKVA9iBd3m7s6V9KqLLCBaG4BdYszS3cbsQY92d9BkTapkLfbFrVEaLTeF5ETT7eewTGYQwY2h8knk9x9e84idnNVUKTiJs34AvSaAXkbRehzJpAjQ2skHXb1PtS7FU6TVgmQpW1tykJ9qJkDzDf9JWiHSvupkxvmK6MT2Aqkvc1owy2Q7i"
        .parse()
        .expect("failed gw1 parse");

    // WIFI should work
    let wifi_key = KeyType::Wifi(&wifi_pub_key);
    let seniority = Seniority::fetch_latest(wifi_key, &mut txn)
        .await
        .unwrap()
        .unwrap();
    let co_uuid = Uuid::from_str("019516a5-2a8c-7480-b319-8b4f801ebe6c").unwrap();
    let stream = pool
        .covered_hex_stream(wifi_key, &co_uuid, &seniority)
        .await
        .unwrap();
    let hexes: Vec<HexCoverage> = stream.try_collect().await.unwrap();
    assert_eq!(hexes.len(), 1);
}
