use futures::stream::TryStreamExt;
use helium_proto::{BlockchainRegionParamsV1, Message, Region};
use hextree::{
    compaction::EqCompactor,
    h3ron::{FromH3Index, H3Cell},
    HexTreeMap,
};
use libflate::gzip::Decoder;
use std::{io::Read, sync::Arc};
use tokio::sync::RwLock;

pub struct RegionMap(Arc<RwLock<HexTreeMap<(Region, BlockchainRegionParamsV1), EqCompactor>>>);

impl RegionMap {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HexTreeMap::with_compactor(
            EqCompactor,
        ))))
    }

    pub async fn get(&self, cell: H3Cell) -> Option<(Region, BlockchainRegionParamsV1)> {
        self.0.read().await.get(cell).cloned()
    }

    pub async fn take(&self) -> HexTreeMap<(Region, BlockchainRegionParamsV1), EqCompactor> {
        self.0.read().await.clone()
    }

    pub async fn swap(&self, new_map: HexTreeMap<(Region, BlockchainRegionParamsV1), EqCompactor>) {
        *self.0.write().await = new_map
    }
}

impl Default for RegionMap {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RegionMapError {
    #[error("region map database error")]
    DbFetch(#[from] sqlx::Error),
    #[error("params decode error")]
    ParamsDecode(#[from] helium_proto::DecodeError),
    #[error("indices gunzip error")]
    IdxGunzip(#[from] std::io::Error),
    #[error("unsupported region error: {0}")]
    UnsupportedRegion(i32),
}

#[derive(sqlx::FromRow)]
pub struct HexRegion {
    pub region: i32,
    pub params: Vec<u8>,
    pub indexes: Vec<u8>,
}

pub async fn build_region_map(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<HexTreeMap<(Region, BlockchainRegionParamsV1), EqCompactor>, RegionMapError> {
    let mut region_map = HexTreeMap::with_compactor(EqCompactor);

    let mut regions = sqlx::query_as::<_, HexRegion>("select * from regions").fetch(db);

    while let Some(region_row) = regions.try_next().await? {
        let region = region_row.region;
        let region = Region::from_i32(region).ok_or(RegionMapError::UnsupportedRegion(region))?;
        let params = BlockchainRegionParamsV1::decode(region_row.params.as_slice())?;
        let encoded_idxs = region_row.indexes;
        let mut idx_decoder = Decoder::new(&encoded_idxs[..])?;
        let mut buf = Vec::new();
        idx_decoder.read_to_end(&mut buf)?;

        let mut idx_buf = [0_u8; 8];
        for chunk in buf.chunks(8) {
            idx_buf.as_mut_slice().copy_from_slice(chunk);
            let idx = u64::from_le_bytes(idx_buf);
            region_map.insert(H3Cell::from_h3index(idx), (region, params.clone()));
        }
    }

    Ok(region_map)
}

pub async fn update_region(
    region: i32,
    params: BlockchainRegionParamsV1,
    indexes: &[u8],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
) -> Result<HexRegion, sqlx::Error> {
    let mut transaction = db.begin().await?;

    sqlx::query(
        r#"
        insert into regions (region, params)
        values ($1, $2)
        on conflict (region) do update set params = excluded.params
        "#,
    )
    .bind(region)
    .bind(params.encode_to_vec())
    .execute(&mut transaction)
    .await?;

    if !indexes.is_empty() {
        sqlx::query(
            r#"
            insert into regions (region, indexes)
            values ($1, $2)
            on conflict (region) do update set indexes = excluded.indexes
            "#,
        )
        .bind(region)
        .bind(indexes)
        .execute(&mut transaction)
        .await?;
    }

    let updated_region = sqlx::query_as::<_, HexRegion>(
        r#"
        select * from regions where region = $1
        "#,
    )
    .bind(region)
    .fetch_one(&mut transaction)
    .await?;

    transaction.commit().await?;

    Ok(updated_region)
}
