use crate::lora_field::{self, DevAddrConstraint, LoraField, NetIdField};
use helium_proto::services::iot_config::org_create_helium_req_v1::HeliumNetId as ProtoNetId;
use std::{collections::HashSet, ops::RangeInclusive};

const TYPE_0_ID: NetIdField = LoraField(0x00003c);
const TYPE_3_ID: NetIdField = LoraField(0x60002d);
const TYPE_6_ID: NetIdField = LoraField(0xc00053);
const TYPE_0_RANGE: RangeInclusive<u32> = 2_013_265_920..=2_046_820_351;
const TYPE_3_RANGE: RangeInclusive<u32> = 3_763_994_624..=3_764_125_695;
const TYPE_6_RANGE: RangeInclusive<u32> = 4_227_943_424..=4_227_944_447;

pub enum HeliumNetId {
    Type0,
    Type3,
    Type6,
}

impl HeliumNetId {
    pub fn id(&self) -> NetIdField {
        match *self {
            HeliumNetId::Type0 => TYPE_0_ID,
            HeliumNetId::Type3 => TYPE_3_ID,
            HeliumNetId::Type6 => TYPE_6_ID,
        }
    }

    pub fn addr_range(&self) -> RangeInclusive<u32> {
        match *self {
            HeliumNetId::Type0 => TYPE_0_RANGE,
            HeliumNetId::Type3 => TYPE_3_RANGE,
            HeliumNetId::Type6 => TYPE_6_RANGE,
        }
    }
}

impl TryFrom<NetIdField> for HeliumNetId {
    type Error = &'static str;

    fn try_from(field: NetIdField) -> Result<Self, Self::Error> {
        let id = match field {
            TYPE_0_ID => HeliumNetId::Type0,
            TYPE_3_ID => HeliumNetId::Type3,
            TYPE_6_ID => HeliumNetId::Type6,
            _ => return Err("not a helium id"),
        };
        Ok(id)
    }
}

#[async_trait::async_trait]
pub trait AddressStore {
    type Error;

    async fn get_used_addrs(&mut self) -> Result<Vec<u32>, Self::Error>;
    async fn claim_addrs(&mut self, new_addrs: &[u32]) -> Result<(), Self::Error>;
    async fn release_addrs(&mut self, released_addrs: &[u32]) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
impl AddressStore for sqlx::Pool<sqlx::Postgres> {
    type Error = sqlx::Error;

    async fn get_used_addrs(&mut self) -> Result<Vec<u32>, Self::Error> {
        Ok(
            sqlx::query_scalar::<_, i32>(" select devaddr from helium_used_devaddrs ")
                .fetch_all(&*self)
                .await?
                .into_iter()
                .map(|addr| addr as u32)
                .collect::<Vec<u32>>(),
        )
    }

    async fn claim_addrs(&mut self, new_addrs: &[u32]) -> Result<(), Self::Error> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(" insert into helium_used_devaddrs (devaddr) ");
        query_builder.push_values(new_addrs, |mut builder, addr| {
            builder.push_bind(*addr as i32);
        });
        Ok(query_builder.build().execute(&*self).await.map(|_| ())?)
    }

    async fn release_addrs(&mut self, released_addrs: &[u32]) -> Result<(), Self::Error> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(" delete from helium_used_devaddrs where devaddr in ");
        query_builder.push_values(released_addrs, |mut builder, addr| {
            builder.push_bind(*addr as i32);
        });
        Ok(query_builder.build().execute(&*self).await.map(|_| ())?)
    }
}

#[async_trait::async_trait]
impl AddressStore for sqlx::Transaction<'_, sqlx::Postgres> {
    type Error = sqlx::Error;

    async fn get_used_addrs(&mut self) -> Result<Vec<u32>, Self::Error> {
        Ok(
            sqlx::query_scalar::<_, i32>(" select devaddr from helium_used_devaddrs order by devaddr asc ")
                .fetch_all(self)
                .await?
                .into_iter()
                .map(|addr| addr as u32)
                .collect::<Vec<u32>>(),
        )
    }

    async fn claim_addrs(&mut self, new_addrs: &[u32]) -> Result<(), Self::Error> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(" insert into helium_used_devaddrs (devaddr) ");
        query_builder.push_values(new_addrs, |mut builder, addr| {
            builder.push_bind(*addr as i32);
        });
        Ok(query_builder.build().execute(self).await.map(|_| ())?)
    }

    async fn release_addrs(&mut self, released_addrs: &[u32]) -> Result<(), Self::Error> {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new(" delete from helium_used_devaddrs where devaddr in ");
        query_builder.push_values(released_addrs, |mut builder, addr| {
            builder.push_bind(*addr as i32);
        });
        Ok(query_builder.build().execute(self).await.map(|_| ())?)
    }
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
        .get_used_addrs()
        .await
        .map_err(DevAddrConstraintsError::AddressStore)?;
    let range_start = *addr_range.start();
    let range_end = *addr_range.end();
    let last_used = if let Some(last) = used_addrs.last() {
        *last
    } else {
        range_start
    };
    let used_range = (range_start..=last_used).collect::<HashSet<u32>>();
    let used_addrs = used_addrs.into_iter().collect::<HashSet<u32>>();
    let mut available_diff = used_range
        .difference(&used_addrs)
        .copied()
        .collect::<Vec<_>>();
    available_diff.sort();
    let mut claimed_addrs = available_diff.drain(0..(count as usize).min(available_diff.len())).collect::<Vec<_>>();
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
        .claim_addrs(&claimed_addrs)
        .await
        .map_err(DevAddrConstraintsError::AddressStore)?;
    let new_constraints = constraints_from_addrs(claimed_addrs)?;
    Ok(new_constraints)
}

pub async fn checkout_specified_devaddr_constraint<S>(
    addr_store: &mut S,
    requested_constraint: &DevAddrConstraint,
) -> Result<(), DevAddrConstraintsError<S::Error>>
where
    S: AddressStore,
{
    let used_addrs = addr_store
        .get_used_addrs()
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
        .claim_addrs(&request_addrs)
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
            ProtoNetId::Type00x00003c => Self::Type0,
            ProtoNetId::Type30x60002d => Self::Type3,
            ProtoNetId::Type60xc00053 => Self::Type6,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[async_trait::async_trait]
    impl AddressStore for Vec<u32> {
        type Error = std::convert::Infallible;

        async fn get_used_addrs(&mut self) -> Result<Vec<u32>, Self::Error> {
            let mut result = self.clone();
            result.sort();
            Ok(result)
        }

        async fn claim_addrs(&mut self, new_addrs: &[u32]) -> Result<(), Self::Error> {
            new_addrs.iter().for_each(|addr| self.push(*addr));
            self.sort();
            Ok(())
        }

        async fn release_addrs(&mut self, released_addrs: &[u32]) -> Result<(), Self::Error> {
            self.retain(|addr| !released_addrs.contains(addr));
            self.sort();
            Ok(())
        }
    }

    #[tokio::test]
    async fn get_free_addrs_from_used_range() {
        let mut used_addrs = vec![
            2013265920, 2013265921, 2013265922, 2013265923, 2013265928, 2013265929, 2013265930,
            2013265931, 2013265936, 2013265937,
        ];
        let selected_constraints =
            checkout_devaddr_constraints(&mut used_addrs, 10, HeliumNetId::Type0)
                .await
                .expect("constraints selected from available addrs");
        let expected_constraints = vec![
            DevAddrConstraint::new(2013265924.into(), 2013265927.into()).expect("new constraint 1"),
            DevAddrConstraint::new(2013265932.into(), 2013265935.into()).expect("new constraint 2"),
            DevAddrConstraint::new(2013265938.into(), 2013265939.into()).expect("new constraint 3"),
        ];
        assert_eq!(selected_constraints, expected_constraints);
        used_addrs.sort();
        assert_eq!(used_addrs, (2013265920..=2013265939).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn get_free_addrs_from_new_range() {
        let mut used_addrs = Vec::new();
        let selected_constraints =
            checkout_devaddr_constraints(&mut used_addrs, 10, HeliumNetId::Type0)
                .await
                .expect("constraints selected from available addrs");
        let expected_constraints =
            vec![DevAddrConstraint::new(2013265920.into(), 2013265929.into())
                .expect("new constraint")];
        assert_eq!(selected_constraints, expected_constraints);
        assert_eq!(used_addrs, (2013265920..=2013265929).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn error_when_no_devaddrs_available() {
        let mut used_addrs = (4227943424..4227944443).collect::<Vec<_>>();
        assert!(
            checkout_devaddr_constraints(&mut used_addrs, 6, HeliumNetId::Type6)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn error_when_odd_number_addrs_requested() {
        let mut used_addrs = Vec::new();
        assert!(
            checkout_devaddr_constraints(&mut used_addrs, 5, HeliumNetId::Type0)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn error_when_addrs_uneven() {
        let mut used_addrs = vec![
            3763994627, 3763994628, 3763994629, 3763994630, 3763994631, 3763994632,
        ];
        assert!(
            checkout_devaddr_constraints(&mut used_addrs, 8, HeliumNetId::Type3)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn allocate_fewer_than_existing_gap() {
        let mut used_addrs = vec![];
        checkout_devaddr_constraints(&mut used_addrs, 8, HeliumNetId::Type0)
            .await
            .expect("allocate first round");
        checkout_devaddr_constraints(&mut used_addrs, 32, HeliumNetId::Type0)
            .await
            .expect("allocate second round");
        checkout_devaddr_constraints(&mut used_addrs, 8, HeliumNetId::Type0)
            .await
            .expect("allocate third round");
        // round 2 goes out of business, and their devaddrs are released back to the wild
        let remove: Vec<u32> = used_addrs
            .clone()
            .into_iter()
            .skip(8)
            .take(32)
            .collect();
        assert_eq!(Ok(()), used_addrs.release_addrs(&remove).await);
        assert_eq!(8 + 8, used_addrs.len());
        checkout_devaddr_constraints(&mut used_addrs, 8, HeliumNetId::Type0)
            .await
            .expect("allocate fourth round");
        assert_eq!(8 + 8 + 8, used_addrs.len());
    }
}
