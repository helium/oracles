use crate::{
    helium_netids::{self, is_helium_netid, AddressStore, HeliumNetId},
    lora_field::{DevAddrConstraint, DevAddrField, NetIdField},
    org_service::UpdateAuthorizer,
};
use futures::stream::StreamExt;
use helium_crypto::PublicKeyBinary;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::Serialize;
use solana_sdk::pubkey::Pubkey;
use sqlx::{error::Error as SqlxError, postgres::PgRow, types::Uuid, FromRow, Row};
use std::collections::HashSet;
use std::str::FromStr;
use tokio::sync::watch;

pub mod proto {
    pub use helium_proto::services::iot_config::{
        org_update_req_v1::update_v1::Update, org_update_req_v1::UpdateV1, ActionV1, OrgResV1,
        OrgV1,
    };
}

#[derive(Clone, Debug, Serialize)]
pub struct Org {
    pub oui: u64,
    pub owner: Pubkey,
    pub escrow_key: String,
    pub locked: bool,
    pub delegate_keys: Option<Vec<Pubkey>>,
    pub constraints: Option<Vec<DevAddrConstraint>>,
}

impl FromRow<'_, PgRow> for Org {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let oui = row.try_get::<i64, &str>("oui")? as u64;
        let owner_str: String = row.get("authority");
        let owner = Pubkey::from_str(&owner_str).map_err(|e| SqlxError::Decode(Box::new(e)))?;
        let escrow_key = row.get::<String, _>("escrow_key");
        let locked = row.get::<bool, _>("locked");
        let raw_delegate_keys: Option<Vec<String>> = row.try_get("delegate_keys")?;
        let delegate_keys: Option<Vec<Pubkey>> = raw_delegate_keys.map(|keys| {
            keys.into_iter()
                .filter_map(|key| Pubkey::from_str(&key).ok())
                .collect()
        });

        let raw_constraints: Option<Vec<(Decimal, Decimal)>> = row.try_get("constraints")?;
        let constraints: Option<Vec<DevAddrConstraint>> = if let Some(constraints_data) =
            raw_constraints
        {
            let constraints_result: Result<Vec<DevAddrConstraint>, SqlxError> = constraints_data
                .into_iter()
                .map(|(start_decimal, end_decimal)| {
                    let start_u64 = start_decimal.to_u64().ok_or_else(|| {
                        SqlxError::Decode(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to convert NUMERIC 'start_addr' to u64",
                        )))
                    })?;

                    let end_u64 = end_decimal.to_u64().ok_or_else(|| {
                        SqlxError::Decode(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to convert NUMERIC 'end_addr' to u64",
                        )))
                    })?;

                    Ok(DevAddrConstraint {
                        start_addr: start_u64.into(),
                        end_addr: end_u64.into(),
                    })
                })
                .collect();

            Some(constraints_result?)
        } else {
            None
        };

        Ok(Self {
            oui,
            owner,
            escrow_key,
            locked,
            delegate_keys,
            constraints,
        })
    }
}

pub type DelegateCache = HashSet<Pubkey>;

pub async fn delegate_keys_cache(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(watch::Sender<DelegateCache>, watch::Receiver<DelegateCache>), sqlx::Error> {
    let key_set: HashSet<Pubkey> = sqlx::query_scalar(
        "SELECT delegate FROM solana_organization_delegate_keys WHERE delegate IS NOT NULL",
    )
    .fetch_all(db)
    .await?
    .into_iter()
    .filter_map(|delegate: String| Pubkey::from_str(&delegate).ok())
    .collect();

    Ok(watch::channel(key_set))
}

pub async fn get_org_netid(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<NetIdField, sqlx::Error> {
    let netid = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT sol_ni.id::bigint
        FROM solana_organization_devaddr_constraints sol_odc
        JOIN solana_organizations sol_org ON sol_org.address = sol_odc.organization
        JOIN solana_net_ids sol_ni ON sol_ni.address = sol_odc.net_id
        WHERE sol_org.oui = $1
        LIMIT 1
        "#,
    )
    .bind(Decimal::from(oui))
    .fetch_one(db)
    .await?;

    Ok(netid.into())
}

async fn add_constraint_update(
    oui: u64,
    net_id: NetIdField,
    added_constraint: DevAddrConstraint,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), OrgStoreError> {
    let helium_net_id: HeliumNetId = net_id
        .try_into()
        .map_err(|err: &'static str| OrgStoreError::InvalidUpdate(err.to_string()))?;
    helium_netids::checkout_specified_devaddr_constraint(db, helium_net_id, &added_constraint)
        .await
        .map_err(|err| OrgStoreError::InvalidUpdate(format!("{err:?}")))?;
    insert_helium_constraints(oui, net_id, &[added_constraint], db).await?;
    Ok(())
}

async fn remove_constraint_update(
    oui: u64,
    net_id: NetIdField,
    org_constraints: Option<&Vec<DevAddrConstraint>>,
    removed_constraint: DevAddrConstraint,
    db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), OrgStoreError> {
    let helium_net_id: HeliumNetId = net_id
        .try_into()
        .map_err(|err: &'static str| OrgStoreError::InvalidUpdate(err.to_string()))?;
    if let Some(org_constraints) = org_constraints {
        if org_constraints.contains(&removed_constraint) && org_constraints.len() > 1 {
            let remove_range = (u32::from(removed_constraint.start_addr)
                ..=u32::from(removed_constraint.end_addr))
                .collect::<Vec<u32>>();
            db.release_addrs(helium_net_id, &remove_range).await?;
            remove_helium_constraints(oui, &[removed_constraint], db).await?;
            Ok(())
        } else if org_constraints.len() == 1 {
            return Err(OrgStoreError::InvalidUpdate(
                "org must have at least one constraint range".to_string(),
            ));
        } else {
            return Err(OrgStoreError::InvalidUpdate(
                "cannot remove constraint leased by other org".to_string(),
            ));
        }
    } else {
        Err(OrgStoreError::InvalidUpdate(
            "no org constraints defined".to_string(),
        ))
    }
}

async fn add_devaddr_slab(
    oui: u64,
    net_id: NetIdField,
    addr_count: u64,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), OrgStoreError> {
    let helium_net_id: HeliumNetId = net_id
        .try_into()
        .map_err(|err: &'static str| OrgStoreError::InvalidUpdate(err.to_string()))?;
    let constraints = helium_netids::checkout_devaddr_constraints(txn, addr_count, helium_net_id)
        .await
        .map_err(|err| OrgStoreError::SaveConstraints(format!("{err:?}")))?;
    insert_helium_constraints(oui, net_id, &constraints, txn).await?;
    Ok(())
}

async fn insert_helium_constraints(
    oui: u64,
    net_id: NetIdField,
    devaddr_ranges: &[DevAddrConstraint],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        r#"
        insert into organization_devaddr_constraints (oui, net_id, start_addr, end_addr)
        "#,
    );
    query_builder.push_values(devaddr_ranges, |mut builder, range| {
        builder
            .push_bind(oui as i64)
            .push_bind(i32::from(net_id))
            .push_bind(i32::from(range.start_addr))
            .push_bind(i32::from(range.end_addr));
    });

    query_builder.build().execute(db).await.map(|_| ())
}

async fn remove_helium_constraints(
    oui: u64,
    devaddr_ranges: &[DevAddrConstraint],
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    let constraints = devaddr_ranges
        .iter()
        .map(|constraint| (oui, constraint.start_addr, constraint.end_addr))
        .collect::<Vec<(u64, DevAddrField, DevAddrField)>>();
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "delete from organization_devaddr_constraints where (oui, start_addr, end_addr) in ",
    );
    query_builder.push_tuples(constraints, |mut builder, (oui, start_addr, end_addr)| {
        builder
            .push_bind(oui as i64)
            .push_bind(i32::from(start_addr))
            .push_bind(i32::from(end_addr));
    });

    query_builder.build().execute(db).await.map(|_| ())
}

async fn check_roamer_constraint_count(
    net_id: NetIdField,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(sol_odc.net_id)
        FROM solana_organization_devaddr_constraints sol_odc
        JOIN solana_net_ids sol_ni ON sol_odc.net_id = sol_ni.address
        WHERE sol_ni.id = $1::numeric
        "#,
    )
    .bind(net_id.to_string())
    .fetch_one(db)
    .await
}

async fn insert_roamer_constraint(
    oui: u64,
    net_id: NetIdField,
    devaddr_range: &DevAddrConstraint,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into organization_devaddr_constraints (oui, net_id, start_addr, end_addr)
        values ($1, $2, $3, $4)
        "#,
    )
    .bind(oui as i64)
    .bind(i32::from(net_id))
    .bind(i32::from(devaddr_range.start_addr))
    .bind(i32::from(devaddr_range.end_addr))
    .execute(db)
    .await
    .map(|_| ())
}

const GET_ORG_SQL: &str = r#"
SELECT
    sol_org.oui::bigint,
    sol_org.authority,
    sol_org.escrow_key,
    COALESCE((SELECT locked
     FROM organization_locks
     WHERE organization = sol_org.address), true) AS locked,
    ARRAY(
        SELECT (start_addr, end_addr)
        FROM solana_organization_devaddr_constraints
        WHERE organization = sol_org.address
    ) AS constraints,
    ARRAY(
        SELECT delegate
        FROM solana_organization_delegate_keys
        WHERE organization = sol_org.address
    ) AS delegate_keys
FROM solana_organizations sol_org
"#;

pub async fn list(db: impl sqlx::PgExecutor<'_>) -> Result<Vec<Org>, sqlx::Error> {
    let orgs = sqlx::query_as::<_, Org>(GET_ORG_SQL)
        .fetch(db)
        .filter_map(|row| async move { row.ok() })
        .collect::<Vec<Org>>()
        .await;

    Ok(orgs)
}

pub async fn get(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<Option<Org>, sqlx::Error> {
    let mut query: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(GET_ORG_SQL);
    query.push(" where sol_org.oui = $1 ");
    query
        .build_query_as::<Org>()
        .bind(Decimal::from(oui))
        .fetch_optional(db)
        .await
}

pub async fn get_constraints_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<DevAddrConstraint>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;
    let constraints = sqlx::query(
        r#"
        SELECT sol_odc.start_addr, sol_odc.end_addr
        FROM solana_organization_devaddr_constraints sol_odc
        JOIN solana_organizations sol_orgs ON sol_odc.organization = sol_orgs.address
        JOIN routes ON routes.oui = sol_orgs.oui
        WHERE routes.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| -> Result<DevAddrConstraint, OrgStoreError> {
        let start_decimal: Decimal = row.get("start_addr");
        let start_u64 = start_decimal.to_u64().ok_or_else(|| {
            OrgStoreError::DecodeNumeric(
                "Failed to convert NUMERIC 'start_addr' to u64".to_string(),
            )
        })?;

        let end_decimal: Decimal = row.get("end_addr");
        let end_u64 = end_decimal.to_u64().ok_or_else(|| {
            OrgStoreError::DecodeNumeric("Failed to convert NUMERIC 'end_addr' to u64".to_string())
        })?;

        Ok(DevAddrConstraint {
            start_addr: start_u64.into(),
            end_addr: end_u64.into(),
        })
    })
    .collect::<Result<Vec<DevAddrConstraint>, OrgStoreError>>()?;

    Ok(constraints)
}

pub async fn get_route_ids_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<String>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;

    let route_ids = sqlx::query(
        r#"
        SELECT r2.id
        FROM routes r1
        JOIN routes r2 ON r1.oui = r2.oui
        WHERE r1.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| row.get::<Uuid, &str>("id").to_string())
    .collect();

    Ok(route_ids)
}

pub async fn is_locked(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT COALESCE(org_lock.locked, true)
        FROM solana_organizations sol_org
        LEFT JOIN organization_locks org_lock ON sol_org.address = org_lock.organization
        WHERE sol_org.oui = $1
        "#,
    )
    .bind(Decimal::from(oui))
    .fetch_one(db)
    .await
}

pub async fn toggle_locked(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO organization_locks (organization, locked)
        SELECT address, NOT COALESCE(org_lock.locked, true)
        FROM solana_organizations sol_org
        LEFT JOIN organization_locks org_lock ON sol_org.address = org_lock.organization
        WHERE sol_org.oui = $1
        ON CONFLICT (organization) DO UPDATE
        SET locked = NOT COALESCE(organization_locks.locked, true)
        "#,
    )
    .bind(Decimal::from(oui))
    .execute(db)
    .await?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum OrgStoreError {
    #[error("error retrieving saved org row: {0}")]
    FetchOrg(#[from] sqlx::Error),
    #[error("org not found: {0}")]
    NotFound(String),
    #[error("error saving org: {0}")]
    SaveOrg(String),
    #[error("error saving delegate keys: {0}")]
    SaveDelegates(String),
    #[error("error saving devaddr constraints: {0}")]
    SaveConstraints(String),
    #[error("unable to deserialize pubkey: {0}")]
    DecodeKey(#[from] helium_crypto::Error),
    #[error("Route Id parse error: {0}")]
    RouteIdParse(#[from] sqlx::types::uuid::Error),
    #[error("Invalid update: {0}")]
    InvalidUpdate(String),
    #[error("unable to decode numeric field: {0}")]
    DecodeNumeric(String),
}

pub async fn get_org_pubkeys(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<Pubkey>, OrgStoreError> {
    let org = get(oui, db)
        .await?
        .ok_or_else(|| OrgStoreError::NotFound(format!("{oui}")))?;

    let mut pubkeys: Vec<Pubkey> = vec![org.owner];

    if let Some(delegate_keys) = org.delegate_keys {
        pubkeys.extend(delegate_keys);
    }

    Ok(pubkeys)
}

pub async fn get_org_pubkeys_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<Pubkey>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;
    let org = sqlx::query_as::<_, Org>(
        r#"
        SELECT
            sol_org.authority AS owner,
            ARRAY(
                SELECT delegate
                FROM solana_organization_delegate_keys
                WHERE organization = sol_org.address
            ) AS delegate_keys
        FROM solana_organizations sol_org
        JOIN routes r ON sol_org.oui = r.oui
        WHERE r.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await?;

    let mut pubkeys: Vec<Pubkey> = vec![org.owner];
    if let Some(delegate_keys) = org.delegate_keys {
        pubkeys.extend(delegate_keys);
    }

    Ok(pubkeys)
}

impl From<Org> for proto::OrgV1 {
    fn from(org: Org) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.to_bytes().to_vec(),
            escrow_key: org.escrow_key,
            locked: org.locked,
            delegate_keys: org.delegate_keys.map_or_else(Vec::new, |keys| {
                keys.iter().map(|key| key.to_bytes().to_vec()).collect()
            }),
        }
    }
}
