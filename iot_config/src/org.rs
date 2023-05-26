use crate::{
    lora_field::{DevAddrConstraint, DevAddrField, NetIdField},
    HELIUM_NET_ID,
};
use futures::stream::StreamExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use serde::Serialize;
use sqlx::{postgres::PgRow, types::Uuid, FromRow, Row};
use std::collections::HashSet;
use tokio::sync::watch;

pub mod proto {
    pub use helium_proto::services::iot_config::{OrgResV1, OrgV1};
}

#[derive(Clone, Debug, Serialize)]
pub struct Org {
    pub oui: u64,
    pub owner: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub locked: bool,
    pub delegate_keys: Option<Vec<PublicKeyBinary>>,
    pub constraints: Option<Vec<DevAddrConstraint>>,
}

#[derive(Debug, Serialize)]
pub struct OrgList {
    orgs: Vec<Org>,
}

impl FromRow<'_, PgRow> for Org {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let delegate_keys = row
            .get::<Vec<PublicKeyBinary>, &str>("delegate_keys")
            .into_iter()
            .map(Some)
            .collect();
        let constraints = row
            .get::<Vec<(i32, i32)>, &str>("constraints")
            .into_iter()
            .map(|(start, end)| {
                Some(DevAddrConstraint {
                    start_addr: start.into(),
                    end_addr: end.into(),
                })
            })
            .collect();
        Ok(Self {
            oui: row.get::<i64, &str>("oui") as u64,
            owner: row.get("owner_pubkey"),
            payer: row.get("payer_pubkey"),
            locked: row.get("locked"),
            delegate_keys,
            constraints,
        })
    }
}

pub type DelegateCache = HashSet<PublicKeyBinary>;

pub async fn delegate_keys_cache(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(watch::Sender<DelegateCache>, watch::Receiver<DelegateCache>), sqlx::Error> {
    let key_set = sqlx::query(r#" select delegate_pubkey from organization_delegate_keys "#)
        .fetch(db)
        .filter_map(|row| async move { row.ok() })
        .map(|row| row.get("delegate_pubkey"))
        .collect::<HashSet<PublicKeyBinary>>()
        .await;

    Ok(watch::channel(key_set))
}

pub async fn create_org(
    owner: PublicKeyBinary,
    payer: PublicKeyBinary,
    delegate_keys: Vec<PublicKeyBinary>,
    net_id: NetIdField,
    devaddr_range: &DevAddrConstraint,
    db: impl sqlx::PgExecutor<'_> + sqlx::Acquire<'_, Database = sqlx::Postgres> + Copy,
) -> Result<Org, OrgStoreError> {
    let mut txn = db.begin().await?;

    let oui = sqlx::query(
        r#"
        insert into organizations (owner_pubkey, payer_pubkey)
        values ($1, $2)
        returning oui
        "#,
    )
    .bind(&owner)
    .bind(&payer)
    .fetch_one(&mut txn)
    .await
    .map_err(|_| {
        OrgStoreError::SaveOrg(format!("owner: {owner}, payer: {payer}, net_id: {net_id}"))
    })?
    .get::<i64, &str>("oui");

    if !delegate_keys.is_empty() {
        let delegate_keys = delegate_keys
            .into_iter()
            .map(|key| (key, oui))
            .collect::<Vec<(PublicKeyBinary, i64)>>();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            " insert into organization_delegate_keys (delegate_pubkey, oui) ",
        );
        query_builder
            .push_values(delegate_keys, |mut builder, (key, oui)| {
                builder.push_bind(key).push_bind(oui);
            })
            .push(" on conflict delegate_pubkey do nothing ");
        query_builder
            .build()
            .execute(&mut txn)
            .await
            .map_err(|_| {
                OrgStoreError::SaveDelegates(format!(
                    "owner: {owner}, payer: {payer}, net_id: {net_id}"
                ))
            })
            .map(|_| ())?
    };

    insert_constraints(oui as u64, net_id, devaddr_range, &mut txn)
        .await
        .map_err(|_| {
            OrgStoreError::SaveConstraints(format!(
                "{} - {}",
                devaddr_range.start_addr, devaddr_range.end_addr
            ))
        })?;

    let org = get(oui as u64, &mut txn).await?;

    txn.commit().await?;

    Ok(org)
}

async fn insert_constraints(
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
        select org.oui, org.owner_pubkey, org.payer_pubkey, org.locked,
            array(select (start_addr, end_addr) from organization_devaddr_constraints org_const where org_const.oui = org.oui) as constraints,
            array(select delegate_pubkey from organization_delegate_keys org_delegates where org_delegates.oui = org.oui) as delegate_keys
        from organizations org
        "#;

pub async fn list(db: impl sqlx::PgExecutor<'_>) -> Result<Vec<Org>, sqlx::Error> {
    Ok(sqlx::query_as::<_, Org>(GET_ORG_SQL)
        .fetch(db)
        .filter_map(|row| async move { row.ok() })
        .collect::<Vec<Org>>()
        .await)
}

pub async fn get(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<Org, sqlx::Error> {
    let mut query: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(GET_ORG_SQL);
    query.push(" where org.oui = $1 ");
    query
        .build_query_as::<Org>()
        .bind(oui as i64)
        .fetch_one(db)
        .await
}

pub async fn get_constraints_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<DevAddrConstraint>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;

    let constraints = sqlx::query(
        r#"
        select consts.start_addr, consts.end_addr from organization_devaddr_constraints consts
        join routes on routes.oui = consts.oui
        where routes.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| DevAddrConstraint {
        start_addr: row.get::<i32, &str>("start_addr").into(),
        end_addr: row.get::<i32, &str>("end_addr").into(),
    })
    .collect();

    Ok(constraints)
}

pub async fn get_route_ids_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<String>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;

    let route_ids = sqlx::query(
        r#"
        select routes.id from routes
        where oui = (
            select organizations.oui from organizations
            join routes on organizations.oui = routes.oui
            where routes.id = $1
        )
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
        select locked from organizations where oui = $1
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await
}

pub async fn toggle_locked(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        update organizations
        set locked = not locked
        where oui = $1
        "#,
    )
    .bind(oui as i64)
    .execute(db)
    .await?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum OrgStoreError {
    #[error("error retrieving saved org row: {0}")]
    FetchOrg(#[from] sqlx::Error),
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
}

pub async fn get_org_pubkeys(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, OrgStoreError> {
    let org = get(oui, db).await?;

    let mut pubkeys: Vec<PublicKey> = vec![
        PublicKey::try_from(org.owner)?,
        PublicKey::try_from(org.payer)?,
    ];

    if let Some(ref mut delegate_pubkeys) = org
        .delegate_keys
        .map(|keys| {
            keys.into_iter()
                .map(PublicKey::try_from)
                .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()
        })
        .transpose()?
    {
        pubkeys.append(delegate_pubkeys);
    }

    Ok(pubkeys)
}

pub async fn get_org_pubkeys_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, OrgStoreError> {
    let uuid = Uuid::try_parse(route_id)?;

    let org = sqlx::query_as::<_, Org>(
        r#"
        select org.oui, org.owner_pubkey, org.payer_pubkey, org.locked,
            array(select (start_addr, end_addr) from organization_devaddr_constraints org_const where org_const.oui = org.oui) as constraints,
            array(select delegate_pubkey from organization_delegate_keys org_delegates where org_delegates.oui = org.oui) as delegate_keys
        from organizations org
        join routes on org.oui = routes.oui
        where routes.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await?;

    let mut pubkeys: Vec<PublicKey> = vec![
        PublicKey::try_from(org.owner)?,
        PublicKey::try_from(org.payer)?,
    ];

    if let Some(ref mut delegate_keys) = org
        .delegate_keys
        .map(|keys| {
            keys.into_iter()
                .map(PublicKey::try_from)
                .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()
        })
        .transpose()?
    {
        pubkeys.append(delegate_keys);
    }

    Ok(pubkeys)
}

#[derive(thiserror::Error, Debug)]
pub enum NextHeliumDevAddrError {
    #[error("error retrieving next available addr: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("invalid devaddr from netid: {0}")]
    InvalidNetId(#[from] crate::lora_field::InvalidNetId),
}

#[derive(sqlx::FromRow)]
struct NextHeliumDevAddr {
    coalesce: i32,
}

pub async fn next_helium_devaddr(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<DevAddrField, NextHeliumDevAddrError> {
    let helium_default_start: i32 = HELIUM_NET_ID.range_start()?.into();

    let addr = sqlx::query_as::<_, NextHeliumDevAddr>(
            r#"
            select coalesce(max(end_addr), $1) from organization_devaddr_constraints where net_id = $2
            "#,
        )
        .bind(helium_default_start)
        .bind(i32::from(HELIUM_NET_ID))
        .fetch_one(db)
        .await?
        .coalesce;

    let next_addr = if addr == helium_default_start {
        addr
    } else {
        addr + 1
    };

    tracing::info!("next helium devaddr start {addr}");

    Ok(next_addr.into())
}

impl From<Org> for proto::OrgV1 {
    fn from(org: Org) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            locked: org.locked,
            delegate_keys: org.delegate_keys.map_or_else(Vec::new, |keys| {
                keys.iter().map(|key| key.as_ref().into()).collect()
            }),
        }
    }
}
