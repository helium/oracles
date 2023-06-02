use crate::lora_field::{self, DevAddrConstraint, LoraField, NetIdField};
use helium_proto::services::iot_config::org_create_helium_req_v1::HeliumNetId as ProtoNetId;
use std::{collections::HashSet, ops::RangeInclusive};

const TYPE_0_ID: NetIdField = LoraField(0x00003c);
const TYPE_3_ID: NetIdField = LoraField(0x60002d);
const TYPE_6_ID: NetIdField = LoraField(0xc00053);
const TYPE_0_RANGE: RangeInclusive<u32> = 2_013_265_920..=2_046_820_351;
const TYPE_3_RANGE: RangeInclusive<u32> = 3_763_994_624..=3_764_125_695;
const TYPE_6_RANGE: RangeInclusive<u32> = 4_227_943_424..=4_227_944_447;

#[derive(Clone, Copy)]
pub enum HeliumNetId {
    Type0_0x00003c,
    Type3_0x60002d,
    Type6_0xc00053,
}

impl HeliumNetId {
    pub fn id(&self) -> NetIdField {
        match *self {
            HeliumNetId::Type0_0x00003c => TYPE_0_ID,
            HeliumNetId::Type3_0x60002d => TYPE_3_ID,
            HeliumNetId::Type6_0xc00053 => TYPE_6_ID,
        }
    }

    pub fn addr_range(&self) -> RangeInclusive<u32> {
        match *self {
            HeliumNetId::Type0_0x00003c => TYPE_0_RANGE,
            HeliumNetId::Type3_0x60002d => TYPE_3_RANGE,
            HeliumNetId::Type6_0xc00053 => TYPE_6_RANGE,
        }
    }
}

impl TryFrom<NetIdField> for HeliumNetId {
    type Error = &'static str;

    fn try_from(field: NetIdField) -> Result<Self, Self::Error> {
        let id = match field {
            TYPE_0_ID => HeliumNetId::Type0_0x00003c,
            TYPE_3_ID => HeliumNetId::Type3_0x60002d,
            TYPE_6_ID => HeliumNetId::Type6_0xc00053,
            _ => return Err("not a helium id"),
        };
        Ok(id)
    }
}

#[async_trait::async_trait]
pub trait AddressStore {
    type Error;

    async fn get_used_addrs(&mut self, net_id: HeliumNetId) -> Result<Vec<u32>, Self::Error>;
    async fn claim_addrs(
        &mut self,
        net_id: HeliumNetId,
        new_addrs: &[u32],
    ) -> Result<(), Self::Error>;
    async fn release_addrs(
        &mut self,
        net_id: HeliumNetId,
        released_addrs: &[u32],
    ) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
impl AddressStore for sqlx::Transaction<'_, sqlx::Postgres> {
    type Error = sqlx::Error;

    async fn get_used_addrs(&mut self, net_id: HeliumNetId) -> Result<Vec<u32>, Self::Error> {
        Ok(sqlx::query_scalar::<_, i32>(
            " select devaddr from helium_used_devaddrs where net_id = $1 order by devaddr asc ",
        )
        .bind(i32::from(net_id.id()))
        .fetch_all(self)
        .await?
        .into_iter()
        .map(|addr| addr as u32)
        .collect::<Vec<u32>>())
    }

    async fn claim_addrs(
        &mut self,
        net_id: HeliumNetId,
        new_addrs: &[u32],
    ) -> Result<(), Self::Error> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(" insert into helium_used_devaddrs (devaddr, net_id) ");
        query_builder.push_values(new_addrs, |mut builder, addr| {
            builder
                .push_bind(*addr as i32)
                .push_bind(i32::from(net_id.id()));
        });
        Ok(query_builder.build().execute(self).await.map(|_| ())?)
    }

    async fn release_addrs(
        &mut self,
        net_id: HeliumNetId,
        released_addrs: &[u32],
    ) -> Result<(), Self::Error> {
        let net_id = i32::from(net_id.id());
        let released_addrs = released_addrs
            .iter()
            .map(|addr| (*addr, net_id))
            .collect::<Vec<(u32, i32)>>();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            " delete from helium_used_devaddrs where (devaddr, net_id) in ",
        );
        query_builder.push_tuples(released_addrs, |mut builder, (addr, id)| {
            builder.push_bind(addr as i32).push_bind(id);
        });
        Ok(query_builder.build().execute(self).await.map(|_| ())?)
    }
}

pub fn is_helium_netid(net_id: &NetIdField) -> bool {
    [TYPE_0_ID, TYPE_3_ID, TYPE_6_ID].contains(net_id)
}

pub async fn checkout_devaddr_constraints<S>(
    addr_store: &mut S,
    count: u64,
    net_id: HeliumNetId,
) -> Result<Vec<DevAddrConstraint>, DevAddrConstraintsError<S::Error>>
where
    S: AddressStore,
{
    let addr_range = net_id.addr_range();
    let used_addrs = addr_store
        .get_used_addrs(net_id)
        .await
        .map_err(DevAddrConstraintsError::AddressStore)?;

    let range_start = *addr_range.start();
    let range_end = *addr_range.end();
    let last_used = used_addrs.last().copied().unwrap_or(range_start);
    let used_range = (range_start..=last_used).collect::<HashSet<u32>>();
    let used_addrs = used_addrs.into_iter().collect::<HashSet<u32>>();

    let mut available_diff = used_range
        .difference(&used_addrs)
        .copied()
        .collect::<Vec<_>>();
    available_diff.sort();

    let mut claimed_addrs = available_diff
        .drain(0..(count as usize).min(available_diff.len()))
        .collect::<Vec<_>>();

    let mut next_addr = last_used + 1;
    while claimed_addrs.len() < count as usize {
        if next_addr <= range_end {
            claimed_addrs.push(next_addr);
            next_addr += 1
        } else {
            return Err(DevAddrConstraintsError::NoAvailableAddrs);
        }
    }

    addr_store
        .claim_addrs(net_id, &claimed_addrs)
        .await
        .map_err(DevAddrConstraintsError::AddressStore)?;

    let new_constraints = constraints_from_addrs(claimed_addrs)?;
    Ok(new_constraints)
}

pub async fn checkout_specified_devaddr_constraint<S>(
    addr_store: &mut S,
    net_id: HeliumNetId,
    requested_constraint: &DevAddrConstraint,
) -> Result<(), DevAddrConstraintsError<S::Error>>
where
    S: AddressStore,
{
    let used_addrs = addr_store
        .get_used_addrs(net_id)
        .await
        .map_err(DevAddrConstraintsError::AddressStore)?;
    let request_addrs = (requested_constraint.start_addr.into()
        ..=requested_constraint.end_addr.into())
        .collect::<Vec<u32>>();
    if request_addrs.iter().any(|&addr| used_addrs.contains(&addr)) {
        return Err(DevAddrConstraintsError::ConstraintAddrInUse(format!(
            "{request_addrs:?}"
        )));
    };
    addr_store
        .claim_addrs(net_id, &request_addrs)
        .await
        .map_err(DevAddrConstraintsError::AddressStore)
}

#[derive(thiserror::Error, Debug)]
pub enum DevAddrConstraintsError<AS> {
    #[error("AddressStore error: {0}")]
    AddressStore(AS),
    #[error("No devaddrs available for NetId")]
    NoAvailableAddrs,
    #[error("Error building constraint")]
    InvalidConstraint(#[from] ConstraintsBuildError),
    #[error("Requested constraint in use {0}")]
    ConstraintAddrInUse(String),
}

fn constraints_from_addrs(
    addrs: Vec<u32>,
) -> Result<Vec<DevAddrConstraint>, ConstraintsBuildError> {
    let mut constraints = Vec::new();
    let mut start_addr: Option<u32> = None;
    let mut end_addr: Option<u32> = None;
    for addr in addrs {
        match (start_addr, end_addr) {
            (None, None) => start_addr = Some(addr),
            (Some(_), None) => end_addr = Some(addr),
            (Some(prev_addr), Some(next_addr)) => match addr {
                addr if addr == next_addr + 1 => end_addr = Some(addr),
                addr if addr > next_addr + 1 => {
                    constraints.push(DevAddrConstraint::new(prev_addr.into(), next_addr.into())?);
                    start_addr = Some(addr);
                    end_addr = None
                }
                _ => return Err(ConstraintsBuildError::EndAddr),
            },
            _ => return Err(ConstraintsBuildError::StartAddr),
        }
    }
    match (start_addr, end_addr) {
        (Some(remaining_start), Some(remaining_end)) => constraints.push(DevAddrConstraint::new(
            remaining_start.into(),
            remaining_end.into(),
        )?),
        _ => return Err(ConstraintsBuildError::EndAddr),
    }
    Ok(constraints)
}

#[derive(thiserror::Error, Debug)]
pub enum ConstraintsBuildError {
    #[error("Constraint missing or invalid start addr")]
    StartAddr,
    #[error("Constraint missing or invalid end addr")]
    EndAddr,
    #[error("invalid constraint: {0}")]
    InvalidConstraint(#[from] lora_field::DevAddrRangeError),
}

impl From<ProtoNetId> for HeliumNetId {
    fn from(pni: ProtoNetId) -> Self {
        match pni {
            ProtoNetId::Type00x00003c => Self::Type0_0x00003c,
            ProtoNetId::Type30x60002d => Self::Type3_0x60002d,
            ProtoNetId::Type60xc00053 => Self::Type6_0xc00053,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[async_trait::async_trait]
    impl AddressStore for HashMap<NetIdField, Vec<u32>> {
        type Error = &'static str;

        async fn get_used_addrs(&mut self, net_id: HeliumNetId) -> Result<Vec<u32>, Self::Error> {
            let mut result = self.get(&net_id.id()).cloned().unwrap_or_default();
            result.sort();
            Ok(result)
        }

        async fn claim_addrs(
            &mut self,
            net_id: HeliumNetId,
            new_addrs: &[u32],
        ) -> Result<(), Self::Error> {
            self.entry(net_id.id())
                .and_modify(|addrs| new_addrs.iter().for_each(|addr| addrs.push(*addr)))
                .or_insert(new_addrs.to_vec());
            Ok(())
        }

        async fn release_addrs(
            &mut self,
            net_id: HeliumNetId,
            released_addrs: &[u32],
        ) -> Result<(), Self::Error> {
            self.entry(net_id.id())
                .and_modify(|addrs| addrs.retain(|addr| !released_addrs.contains(addr)));
            Ok(())
        }
    }

    #[tokio::test]
    async fn get_free_addrs_from_used_range() {
        let mut addr_store = HashMap::new();
        addr_store.insert(
            HeliumNetId::Type0_0x00003c.id(),
            vec![
                2013265920, 2013265921, 2013265922, 2013265923, 2013265928, 2013265929, 2013265930,
                2013265931, 2013265936, 2013265937,
            ],
        );
        let selected_constraints =
            checkout_devaddr_constraints(&mut addr_store, 10, HeliumNetId::Type0_0x00003c)
                .await
                .expect("constraints selected from available addrs");
        let expected_constraints = vec![
            DevAddrConstraint::new(2013265924.into(), 2013265927.into()).expect("new constraint 1"),
            DevAddrConstraint::new(2013265932.into(), 2013265935.into()).expect("new constraint 2"),
            DevAddrConstraint::new(2013265938.into(), 2013265939.into()).expect("new constraint 3"),
        ];
        assert_eq!(selected_constraints, expected_constraints);
        addr_store
            .entry(HeliumNetId::Type0_0x00003c.id())
            .and_modify(|addrs| addrs.sort());
        let used_addrs = addr_store
            .get(&HeliumNetId::Type0_0x00003c.id())
            .cloned()
            .unwrap();
        assert_eq!(used_addrs, (2013265920..=2013265939).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn get_free_addrs_from_new_range() {
        let mut addr_store = HashMap::new();
        let selected_constraints =
            checkout_devaddr_constraints(&mut addr_store, 10, HeliumNetId::Type0_0x00003c)
                .await
                .expect("constraints selected from available addrs");
        let expected_constraints =
            vec![DevAddrConstraint::new(2013265920.into(), 2013265929.into())
                .expect("new constraint")];
        assert_eq!(selected_constraints, expected_constraints);
        addr_store
            .entry(HeliumNetId::Type0_0x00003c.id())
            .and_modify(|addrs| addrs.sort());
        let used_addrs = addr_store
            .get(&HeliumNetId::Type0_0x00003c.id())
            .cloned()
            .unwrap();
        assert_eq!(used_addrs, (2013265920..=2013265929).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn error_when_no_devaddrs_available() {
        let mut addr_store = HashMap::new();
        addr_store.insert(
            HeliumNetId::Type6_0xc00053.id(),
            (4227943424..4227944443).collect::<Vec<_>>(),
        );
        assert!(
            checkout_devaddr_constraints(&mut addr_store, 6, HeliumNetId::Type6_0xc00053)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn error_when_odd_number_addrs_requested() {
        let mut addr_store = HashMap::new();
        assert!(
            checkout_devaddr_constraints(&mut addr_store, 5, HeliumNetId::Type0_0x00003c)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn error_when_addrs_uneven() {
        let mut addr_store = HashMap::new();
        addr_store.insert(
            HeliumNetId::Type3_0x60002d.id(),
            vec![
                3763994627, 3763994628, 3763994629, 3763994630, 3763994631, 3763994632,
            ],
        );
        assert!(
            checkout_devaddr_constraints(&mut addr_store, 8, HeliumNetId::Type3_0x60002d)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn allocate_fewer_than_existing_gap() {
        let mut addr_store = HashMap::new();
        checkout_devaddr_constraints(&mut addr_store, 8, HeliumNetId::Type0_0x00003c)
            .await
            .expect("allocate first round");
        checkout_devaddr_constraints(&mut addr_store, 32, HeliumNetId::Type0_0x00003c)
            .await
            .expect("allocate second round");
        checkout_devaddr_constraints(&mut addr_store, 8, HeliumNetId::Type0_0x00003c)
            .await
            .expect("allocate third round");
        // round 2 goes out of business, and their devaddrs are released back to the wild
        let remove: Vec<u32> = addr_store
            .get(&HeliumNetId::Type0_0x00003c.id())
            .cloned()
            .unwrap()
            .into_iter()
            .skip(8)
            .take(32)
            .collect();
        assert_eq!(
            Ok(()),
            addr_store
                .release_addrs(HeliumNetId::Type0_0x00003c, &remove)
                .await
        );
        assert_eq!(
            8 + 8,
            addr_store
                .get(&HeliumNetId::Type0_0x00003c.id())
                .unwrap()
                .len()
        );
        checkout_devaddr_constraints(&mut addr_store, 8, HeliumNetId::Type0_0x00003c)
            .await
            .expect("allocate fourth round");
        assert_eq!(
            8 + 8 + 8,
            addr_store
                .get(&HeliumNetId::Type0_0x00003c.id())
                .unwrap()
                .len()
        );
    }

    #[tokio::test]
    async fn allocate_across_net_id() {
        let mut addr_store = HashMap::new();
        checkout_devaddr_constraints(&mut addr_store, 8, HeliumNetId::Type6_0xc00053)
            .await
            .expect("testing allocation");
        checkout_devaddr_constraints(&mut addr_store, 8, HeliumNetId::Type3_0x60002d)
            .await
            .expect("special request allocation");
        checkout_devaddr_constraints(&mut addr_store, 8, HeliumNetId::Type0_0x00003c)
            .await
            .expect("average allocation");

        assert_eq!(
            8 + 8 + 8,
            addr_store.values().fold(0, |acc, elem| acc + elem.len())
        );
    }
}
