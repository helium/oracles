use futures::stream::{StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use serde::Serialize;
use sqlx::Row;

use crate::{
    lora_field::{net_id, DevAddrField, DevAddrRange},
    HELIUM_NET_ID, HELIUM_NWK_ID,
};

pub mod proto {
    pub use helium_proto::services::iot_config::{OrgResV1, OrgV1};
}

#[derive(Clone, Debug, Serialize, sqlx::Type)]
#[sqlx(type_name = "org_status", rename_all = "snake_case")]
pub enum OrgStatus {
    Enabled,
    Disabled,
}

#[derive(Clone, Debug, Serialize)]
pub struct Org {
    pub oui: u64,
    pub owner: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub delegate_keys: Vec<PublicKeyBinary>,
    pub status: OrgStatus,
}

#[derive(Debug, Serialize)]
pub struct OrgList {
    orgs: Vec<Org>,
}

#[derive(Debug)]
pub struct OrgWithConstraints {
    pub org: Org,
    pub constraints: DevAddrRange,
}

pub async fn create_org(
    owner: PublicKeyBinary,
    payer: PublicKeyBinary,
    delegate_keys: Vec<PublicKeyBinary>,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Org, sqlx::Error> {
    let row = sqlx::query(
        r#"
        insert into organizations (owner_pubkey, payer_pubkey, delegate_keys)
        values ($1, $2, $3)
        on conflict (owner_pubkey, payer_pubkey) do nothing
        returning oui
        "#,
    )
    .bind(&owner)
    .bind(&payer)
    .bind(&delegate_keys)
    .fetch_one(db)
    .await?;

    Ok(Org {
        oui: row.get::<i64, &str>("oui") as u64,
        owner,
        payer,
        delegate_keys,
        status: OrgStatus::Enabled,
    })
}

pub async fn insert_constraints(
    oui: u64,
    nwk_id: u32,
    devaddr_range: &DevAddrRange,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into organization_devaddr_constraints (oui, nwk_id, start_nwk_addr, end_nwk_addr)
        values ($1, $2, $3, $4)
        on conflict (oui) do update set
        nwk_id = EXCLUDED.nwk_id, start_nwk_addr = EXCLUDED.start_nwk_addr, end_nwk_addr = EXCLUDED.end_nwk_addr
        "#,
    )
    .bind(oui as i64)
    .bind(nwk_id as i32)
    .bind(i32::from(devaddr_range.start_addr))
    .bind(i32::from(devaddr_range.end_addr))
    .execute(db)
    .await
    .map(|_| ())
}

pub async fn list(db: impl sqlx::PgExecutor<'_>) -> Result<Vec<Org>, sqlx::Error> {
    Ok(sqlx::query(
        r#"
        select * from organizations
        "#,
    )
    .fetch(db)
    .map_ok(|row| Org {
        oui: row.get::<i64, &str>("oui") as u64,
        owner: row.get("owner_pubkey"),
        payer: row.get("payer_pubkey"),
        delegate_keys: row.get("delegate_keys"),
        status: row.get("status"),
    })
    .filter_map(|row| async move { row.ok() })
    .collect::<Vec<Org>>()
    .await)
}

pub async fn get(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<Org, sqlx::Error> {
    let row = sqlx::query(
        r#"
        select * from organizations where oui = $1
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await?;

    Ok(Org {
        oui,
        owner: row.get("owner_pubkey"),
        payer: row.get("payer_pubkey"),
        delegate_keys: row.get("delegate_keys"),
        status: row.get("status"),
    })
}

pub async fn get_with_constraints(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<OrgWithConstraints, sqlx::Error> {
    let row = sqlx::query(
        r#"
        select org.owner_pubkey, org.payer_pubkey, org.delegate_keys, org.enabled org_const.start_nwk_addr, org_const.end_nwk_addr
        from organizations org join organization_devaddr_constraints org_const
        on org.oui = org_const.oui
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await?;

    let start_addr = row.get::<i64, &str>("start_nwk_addr");
    let end_addr = row.get::<i64, &str>("end_nwk_addr");

    Ok(OrgWithConstraints {
        org: Org {
            oui,
            owner: row.get("owner_pubkey"),
            payer: row.get("payer_pubkey"),
            delegate_keys: row.get("delegate_keys"),
            status: row.get("status"),
        },
        constraints: DevAddrRange {
            start_addr: start_addr.into(),
            end_addr: end_addr.into(),
        },
    })
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
    addr: i64,
}

pub async fn next_helium_devaddr(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<DevAddrField, NextHeliumDevAddrError> {
    let helium_default_start: i64 = net_id(HELIUM_NET_ID).range_start()?.into();
    tracing::info!("helium devaddr default start {helium_default_start}");

    let addr = sqlx::query_as::<_, NextHeliumDevAddr>(
            r#"
            select coalesce(max(end_nwk_addr), $1) from organization_devaddr_constraints where nwk_id = $2
            "#,
        )
        .bind(helium_default_start)
        .bind(HELIUM_NWK_ID as i32)
        .fetch_one(db)
        .await?
        .addr;
    tracing::info!("next helium devaddr start {addr}");

    Ok(addr.into())
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
            status: OrgStatus::Enabled,
        }
    }
}

#[allow(deprecated)]
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
            // Deprecated proto field; flagged above to avoid compiler warning
            nonce: 0,
        }
    }
}
