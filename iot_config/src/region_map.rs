use futures::stream::TryStreamExt;
use helium_proto::{BlockchainRegionParamsV1, Message, Region};
use hextree::{compaction::EqCompactor, Cell, HexTreeMap};
use libflate::gzip::Decoder;
use std::{collections::HashMap, io::Read, str::FromStr, sync::Arc};
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

    pub async fn get_region(&self, location: Cell) -> Option<Region> {
        self.region_hextree.read().await.get(location).cloned()
    }

    pub async fn get_params(&self, region: &Region) -> Option<BlockchainRegionParamsV1> {
        self.params_hashmap.read().await.get(region).cloned()
    }

    pub async fn insert_params(&self, region: Region, params: BlockchainRegionParamsV1) {
        _ = self.params_hashmap.write().await.insert(region, params)
    }

    pub async fn replace_tree(&self, new_map: HexTreeMap<Region, EqCompactor>) {
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
    #[error("malformed region h3 index list")]
    MalformedH3Indexes,
    #[error("unsupported region error: {0}")]
    UnsupportedRegion(i32),
}

#[derive(sqlx::FromRow)]
pub struct HexRegion {
    pub region: String,
    pub params: Vec<u8>,
    pub indexes: Option<Vec<u8>>,
}

pub async fn build_region_tree(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<HexTreeMap<Region, EqCompactor>, RegionMapError> {
    let mut region_tree = HexTreeMap::with_compactor(EqCompactor);

    let mut regions = sqlx::query_as::<_, HexRegion>("select * from regions").fetch(db);

    while let Some(region_row) = regions.try_next().await? {
        if let Some(indexes) = region_row.indexes {
            let region = Region::from_str(&region_row.region)?;
            let mut h3_idx_decoder = Decoder::new(&indexes[..])?;
            let mut raw_h3_indices = Vec::new();
            h3_idx_decoder.read_to_end(&mut raw_h3_indices)?;

            if raw_h3_indices.len() % std::mem::size_of::<u64>() != 0 {
                tracing::error!("h3 index list malformed; indices are not an index-byte-size multiple; region: {region}");
                return Err(RegionMapError::MalformedH3Indexes);
            }

            let mut h3_idx_buf = [0_u8; 8];
            for (chunk_num, chunk) in raw_h3_indices.chunks(8).enumerate() {
                h3_idx_buf.as_mut_slice().copy_from_slice(chunk);
                let h3_idx = u64::from_le_bytes(h3_idx_buf);
                match Cell::from_raw(h3_idx) {
                    Ok(cell) => region_tree.insert(cell, region),
                    Err(_) => {
                        tracing::error!(
                            "h3 index list malformed; region, chunk, bits: {region}, {chunk_num}, {h3_idx:x}"
                        );
                        return Err(RegionMapError::MalformedH3Indexes);
                    }
                }
            }
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
        let region = Region::from_str(&region_row.region)?;
        let params = BlockchainRegionParamsV1::decode(region_row.params.as_slice())?;
        params_map.insert(region, params);
    }
    Ok(params_map)
}

pub async fn update_region(
    region: Region,
    params: &BlockchainRegionParamsV1,
    indexes: Option<&[u8]>,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
) -> Result<Option<HexTreeMap<Region, EqCompactor>>, RegionMapError> {
    let mut transaction = db.begin().await?;

    sqlx::query(
        r#"
        insert into regions (region, params, indexes)
        values ($1, $2, $3)
        on conflict (region) do update set
            params = excluded.params,
            indexes = case when excluded.indexes is not null
                          then excluded.indexes
                          else regions.indexes
                      end
        "#,
    )
    .bind(region.to_string())
    .bind(params.encode_to_vec())
    .bind(indexes)
    .execute(&mut transaction)
    .await?;

    let updated_region = if indexes.is_some() {
        Some(build_region_tree(&mut transaction).await?)
    } else {
        tracing::debug!("h3 region index update skipped");
        None
    };

    transaction.commit().await?;

    Ok(updated_region)
}
