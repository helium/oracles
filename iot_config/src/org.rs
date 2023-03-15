use futures::stream::StreamExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use serde::Serialize;
use sqlx::{types::Uuid, Row};

use crate::{
    lora_field::{DevAddrConstraint, DevAddrField, NetIdField},
    HELIUM_NET_ID,
};

pub mod proto {
    pub use helium_proto::services::iot_config::{OrgResV1, OrgV1};
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct Org {
    #[sqlx(try_from = "i64")]
    pub oui: u64,
    #[sqlx(rename = "owner_pubkey")]
    pub owner: PublicKeyBinary,
    #[sqlx(rename = "payer_pubkey")]
    pub payer: PublicKeyBinary,
    pub delegate_keys: Vec<PublicKeyBinary>,
    pub locked: bool,
}

#[derive(Debug, Serialize)]
pub struct OrgList {
    orgs: Vec<Org>,
}

#[derive(Debug)]
pub struct OrgWithConstraints {
    pub org: Org,
    pub constraints: Vec<DevAddrConstraint>,
}

pub async fn create_org(
    owner: PublicKeyBinary,
    payer: PublicKeyBinary,
    delegate_keys: Vec<PublicKeyBinary>,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Org, sqlx::Error> {
    let org = sqlx::query_as::<_, Org>(
        r#"
        insert into organizations (owner_pubkey, payer_pubkey, delegate_keys)
        values ($1, $2, $3)
        returning *
        "#,
    )
    .bind(&owner)
    .bind(&payer)
    .bind(&delegate_keys)
    .fetch_one(db)
    .await?;

    Ok(org)
}

pub async fn insert_constraints(
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

pub async fn list(db: impl sqlx::PgExecutor<'_>) -> Result<Vec<Org>, sqlx::Error> {
    Ok(sqlx::query_as::<_, Org>(
        r#"
        select * from organizations
        "#,
    )
    .fetch(db)
    .filter_map(|row| async move { row.ok() })
    .collect::<Vec<Org>>()
    .await)
}

pub async fn get(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<Org, sqlx::Error> {
    sqlx::query_as::<_, Org>(
        r#"
        select * from organizations where oui = $1
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await
}

pub async fn get_with_constraints(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<OrgWithConstraints, sqlx::Error> {
    let row = sqlx::query(
        r#"
        select org.owner_pubkey, org.payer_pubkey, org.delegate_keys, org.locked,
            array(select (start_addr, end_addr) from organization_devaddr_constraints org_const where org_const.oui = org.oui) as constraints
        from organizations org
        where org.oui = $1
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await?;

    let constraints = row
        .get::<Vec<(i32, i32)>, &str>("constraints")
        .into_iter()
        .map(|(start, end)| DevAddrConstraint {
            start_addr: start.into(),
            end_addr: end.into(),
        })
        .collect();

    Ok(OrgWithConstraints {
        org: Org {
            oui,
            owner: row.get("owner_pubkey"),
            payer: row.get("payer_pubkey"),
            delegate_keys: row.get("delegate_keys"),
            locked: row.get("locked"),
        },
        constraints,
    })
}

pub async fn get_constraints_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<DevAddrConstraint>, DbOrgError> {
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
) -> Result<Vec<String>, DbOrgError> {
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
pub enum DbOrgError {
    #[error("error retrieving saved org row: {0}")]
    DbStore(#[from] sqlx::Error),
    #[error("unable to deserialize pubkey: {0}")]
    DecodeKey(#[from] helium_crypto::Error),
    #[error("Route Id parse error: {0}")]
    RouteIdParse(#[from] sqlx::types::uuid::Error),
}

pub async fn get_org_pubkeys(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, DbOrgError> {
    let org = get(oui, db).await?;

    let mut pubkeys: Vec<PublicKey> = vec![PublicKey::try_from(org.owner)?];

    let mut delegate_pubkeys: Vec<PublicKey> = org
        .delegate_keys
        .into_iter()
        .map(PublicKey::try_from)
        .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()?;

    pubkeys.append(&mut delegate_pubkeys);

    Ok(pubkeys)
}

pub async fn get_org_pubkeys_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, DbOrgError> {
    let uuid = Uuid::try_parse(route_id)?;

    let org = sqlx::query_as::<_, Org>(
        r#"
        select * from organizations
        join routes on organizations.oui = routes.oui
        where routes.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await?;

    let mut pubkeys: Vec<PublicKey> = vec![PublicKey::try_from(org.owner)?];

    let mut delegate_keys: Vec<PublicKey> = org
        .delegate_keys
        .into_iter()
        .map(PublicKey::try_from)
        .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()?;

    pubkeys.append(&mut delegate_keys);

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

impl From<proto::OrgV1> for Org {
    fn from(org: proto::OrgV1) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            delegate_keys: org
                .delegate_keys
                .into_iter()
                .map(|key| key.into())
                .collect(),
            locked: org.locked,
        }
    }
}

impl From<Org> for proto::OrgV1 {
    fn from(org: Org) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            delegate_keys: org
                .delegate_keys
                .iter()
                .map(|key| key.as_ref().into())
                .collect(),
            locked: org.locked,
        }
    }
}
