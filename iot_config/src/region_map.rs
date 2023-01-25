use futures::stream::TryStreamExt;
use helium_proto::{BlockchainRegionParamsV1, Message, Region};
use hextree::{
    compaction::EqCompactor,
    h3ron::{FromH3Index, H3Cell},
    HexTreeMap,
};
use libflate::gzip::Decoder;
use std::{collections::HashMap, io::Read, sync::Arc};
use tokio::sync::RwLock;

pub struct RegionMap {
    region_hextree: Arc<RwLock<HexTreeMap<Region, EqCompactor>>>,
    params_hashmap: Arc<RwLock<HashMap<Region, BlockchainRegionParamsV1>>>,
}

impl RegionMap {
    pub async fn new(db: impl sqlx::PgExecutor<'_> + Copy) -> Result<Self, RegionMapError> {
        let region_hextree = build_region_tree(db).await?;
        let params_map = build_params_map(db).await?;
        Ok(Self {
            region_hextree: Arc::new(RwLock::new(region_hextree)),
            params_hashmap: Arc::new(RwLock::new(params_map)),
        })
    }

    pub async fn get_region(&self, location: u64) -> Option<Region> {
        self.region_hextree
            .read()
            .await
            .get(H3Cell::from_h3index(location))
            .cloned()
    }

    pub async fn get_params(&self, region: &Region) -> Option<BlockchainRegionParamsV1> {
        self.params_hashmap.read().await.get(region).cloned()
    }

    pub async fn insert_params(&self, region: Region, params: BlockchainRegionParamsV1) {
        _ = self.params_hashmap.write().await.insert(region, params)
    }

    pub async fn swap_tree(&self, new_map: HexTreeMap<Region, EqCompactor>) {
        *self.region_hextree.write().await = new_map
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

pub async fn build_region_tree(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<HexTreeMap<Region, EqCompactor>, RegionMapError> {
    let mut region_tree = HexTreeMap::with_compactor(EqCompactor);

    let mut regions = sqlx::query_as::<_, HexRegion>("select * from regions").fetch(db);

    while let Some(region_row) = regions.try_next().await? {
        let region = Region::from_i32(region_row.region)
            .ok_or(RegionMapError::UnsupportedRegion(region_row.region))?;
        let mut idx_decoder = Decoder::new(&region_row.indexes[..])?;
        let mut buf = Vec::new();
        idx_decoder.read_to_end(&mut buf)?;

        let mut idx_buf = [0_u8; 8];
        for chunk in buf.chunks(8) {
            idx_buf.as_mut_slice().copy_from_slice(chunk);
            let idx = u64::from_le_bytes(idx_buf);
            region_tree.insert(H3Cell::from_h3index(idx), region);
        }
    }

    Ok(region_tree)
}

pub async fn build_params_map(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<HashMap<Region, BlockchainRegionParamsV1>, RegionMapError> {
    let mut params_map: HashMap<Region, BlockchainRegionParamsV1> = HashMap::new();

    let mut regions = sqlx::query_as::<_, HexRegion>("select * from regions").fetch(db);

    while let Some(region_row) = regions.try_next().await? {
        let region = Region::from_i32(region_row.region)
            .ok_or(RegionMapError::UnsupportedRegion(region_row.region))?;
        let params = BlockchainRegionParamsV1::decode(region_row.params.as_slice())?;
        params_map.insert(region, params);
    }
    Ok(params_map)
}

pub async fn update_region(
    region: i32,
    params: &BlockchainRegionParamsV1,
    indexes: &[u8],
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
) -> Result<Option<HexTreeMap<Region, EqCompactor>>, RegionMapError> {
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

    let updated_region = if !indexes.is_empty() {
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

        Some(build_region_tree(&mut transaction).await?)
    } else {
        None
    };

    transaction.commit().await?;

    Ok(updated_region)
}
