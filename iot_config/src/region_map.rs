use anyhow::anyhow;
use futures::stream::TryStreamExt;
use helium_proto::{BlockchainRegionParamsV1, Message, Region};
use hextree::{compaction::EqCompactor, Cell, HexTreeMap};
use libflate::gzip::Decoder;
use std::{collections::HashMap, io::Read, str::FromStr};
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct RegionMap {
    region_hextree: HexTreeMap<Region, EqCompactor>,
    params_map: HashMap<Region, BlockchainRegionParamsV1>,
}

#[derive(Clone, Debug)]
pub struct RegionMapReader {
    map_receiver: watch::Receiver<RegionMap>,
}

impl RegionMapReader {
    pub async fn new(
        db: impl sqlx::PgExecutor<'_> + Copy,
    ) -> anyhow::Result<(watch::Sender<RegionMap>, Self)> {
        let region_map = RegionMap::new(db).await?;
        let (map_sender, map_receiver) = watch::channel(region_map);
        Ok((map_sender, Self { map_receiver }))
    }

    pub fn get_region(&self, location: Cell) -> Option<Region> {
        self.map_receiver.borrow().get_region(location)
    }

    pub fn get_params(&self, region: &Region) -> Option<BlockchainRegionParamsV1> {
        self.map_receiver.borrow().get_params(region)
    }
}

impl RegionMap {
    pub async fn new(db: impl sqlx::PgExecutor<'_> + Copy) -> anyhow::Result<Self> {
        let region_hextree = build_region_tree(db).await?;
        let params_map = build_params_map(db).await?;
        Ok(Self {
            region_hextree,
            params_map,
        })
    }

    pub fn get_region(&self, location: Cell) -> Option<Region> {
        self.region_hextree.get(location).unzip().1.copied()
    }

    pub fn get_params(&self, region: &Region) -> Option<BlockchainRegionParamsV1> {
        self.params_map.get(region).cloned()
    }

    pub fn insert_params(&mut self, region: Region, params: BlockchainRegionParamsV1) {
        _ = self.params_map.insert(region, params)
    }

    pub fn replace_tree(&mut self, new_map: HexTreeMap<Region, EqCompactor>) {
        self.region_hextree = new_map
    }
}

#[derive(sqlx::FromRow)]
pub struct HexRegion {
    pub region: String,
    pub params: Vec<u8>,
    pub indexes: Option<Vec<u8>>,
}

pub async fn build_region_tree(
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<HexTreeMap<Region, EqCompactor>> {
    let mut region_tree = HexTreeMap::with_compactor(EqCompactor);

    let mut regions =
        sqlx::query_as::<_, HexRegion>("select * from regions order by region desc").fetch(db);

    while let Some(region_row) = regions.try_next().await? {
        if let Some(indexes) = region_row.indexes {
            let region = Region::from_str(&region_row.region)?;
            let mut h3_idx_decoder = Decoder::new(&indexes[..])?;
            let mut raw_h3_indices = Vec::new();
            h3_idx_decoder.read_to_end(&mut raw_h3_indices)?;

            if raw_h3_indices.len() % std::mem::size_of::<u64>() != 0 {
                tracing::error!("h3 index list malformed; indices are not an index-byte-size multiple; region: {region}");
                return Err(anyhow!("malformed h3 indices"));
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
                        return Err(anyhow!("malformed h3 indices"));
                    }
                }
            }
        }
    }

    Ok(region_tree)
}

pub async fn build_params_map(
    db: impl sqlx::PgExecutor<'_>,
) -> anyhow::Result<HashMap<Region, BlockchainRegionParamsV1>> {
    let mut params_map: HashMap<Region, BlockchainRegionParamsV1> = HashMap::new();

    let mut regions = sqlx::query_as::<_, HexRegion>("select * from regions").fetch(db);

    while let Some(region_row) = regions.try_next().await? {
        let region = Region::from_str(&region_row.region)?;
        let params = BlockchainRegionParamsV1::decode(region_row.params.as_slice())?;
        params_map.insert(region, params);
    }

    // insert the Unknown region with Empty params
    params_map.insert(
        Region::Unknown,
        BlockchainRegionParamsV1 {
            region_params: vec![],
        },
    );
    Ok(params_map)
}

pub async fn update_region(
    region: Region,
    params: &BlockchainRegionParamsV1,
    indexes: Option<&[u8]>,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
) -> anyhow::Result<Option<HexTreeMap<Region, EqCompactor>>> {
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
