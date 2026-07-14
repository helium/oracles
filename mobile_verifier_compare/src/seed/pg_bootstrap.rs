use super::Cmd;
use anyhow::{anyhow, Context, Result};
use sqlx::{migrate::Migrator, ConnectOptions, Executor, PgPool};
use std::path::Path;
use std::str::FromStr;

/// The two pools the seeders will write to.
pub struct Pools {
    pub mv: PgPool,
    pub mpv: PgPool,
}

/// Migration source dirs resolved at compile time from this crate's location
/// in the workspace, so the binary works regardless of the user's CWD.
/// `mobile_verifier_compare` sits next to `mobile_verifier` and `mobile_packet_verifier`
/// at the workspace root, so both paths walk one level up.
const MV_MIGRATIONS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../mobile_verifier/migrations");
const MPV_MIGRATIONS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../mobile_packet_verifier/migrations"
);

pub async fn run(args: &Cmd) -> Result<Pools> {
    let admin_url = &args.admin_database_url;
    let mv_url = &args.mv_database_url;
    let mpv_url = &args.mpv_database_url;

    let mv_db = db_name_from_url(mv_url).context("parsing mv database URL")?;
    let mpv_db = db_name_from_url(mpv_url).context("parsing mpv database URL")?;

    println!("[pg] using admin url {}", redact(admin_url));
    println!("[pg]   mobile_verifier db = {mv_db}");
    println!("[pg]   mobile_packet_verifier db = {mpv_db}");

    if args.reset {
        println!("[pg] --reset: dropping target databases");
        drop_database(admin_url, &mv_db).await?;
        drop_database(admin_url, &mpv_db).await?;
    }
    create_database_if_missing(admin_url, &mv_db).await?;
    create_database_if_missing(admin_url, &mpv_db).await?;

    println!("[pg] migrating {mv_db} from {MV_MIGRATIONS}");
    let mv_pool = PgPool::connect(mv_url)
        .await
        .with_context(|| format!("connecting to {mv_db}"))?;
    Migrator::new(Path::new(MV_MIGRATIONS))
        .await
        .context("loading mv migrations")?
        .run(&mv_pool)
        .await
        .context("running mv migrations")?;

    println!("[pg] migrating {mpv_db} from {MPV_MIGRATIONS}");
    let mpv_pool = PgPool::connect(mpv_url)
        .await
        .with_context(|| format!("connecting to {mpv_db}"))?;
    Migrator::new(Path::new(MPV_MIGRATIONS))
        .await
        .context("loading mpv migrations")?
        .run(&mpv_pool)
        .await
        .context("running mpv migrations")?;

    Ok(Pools {
        mv: mv_pool,
        mpv: mpv_pool,
    })
}

/// Pull the database name off a `postgres://user:pass@host:port/dbname` URL.
fn db_name_from_url(url: &str) -> Result<String> {
    let opts = sqlx::postgres::PgConnectOptions::from_str(url)
        .with_context(|| format!("not a valid postgres URL: {}", redact(url)))?;
    let db = opts
        .get_database()
        .ok_or_else(|| anyhow!("postgres URL is missing the /dbname path"))?
        .to_string();
    if db.is_empty() {
        anyhow::bail!("postgres URL has empty dbname");
    }
    Ok(db)
}

fn redact(url: &str) -> String {
    // Hide the password portion when echoing back to stdout.
    match url.find('@') {
        Some(at) => {
            let creds = &url[..at];
            if let Some(colon) = creds.rfind(':') {
                // Keep the scheme + user, replace password with ***
                let scheme_user = &creds[..colon];
                format!("{scheme_user}:***{}", &url[at..])
            } else {
                url.to_string()
            }
        }
        None => url.to_string(),
    }
}

async fn create_database_if_missing(admin_url: &str, db: &str) -> Result<()> {
    let mut conn = sqlx::postgres::PgConnectOptions::from_str(admin_url)?
        .connect()
        .await
        .context("connecting to admin DB")?;
    let exists: Option<i32> = sqlx::query_scalar("SELECT 1 FROM pg_database WHERE datname = $1")
        .bind(db)
        .fetch_optional(&mut conn)
        .await?;
    if exists.is_some() {
        println!("[pg]   {db} already exists");
        return Ok(());
    }
    // CREATE DATABASE does not accept bind parameters. The db name comes from
    // a URL we parsed, so we hand-quote it via the identifier-escape trick to
    // be defensive even though docker-compose names are tame.
    let quoted = quote_ident(db);
    conn.execute(&*format!("CREATE DATABASE {quoted}"))
        .await
        .with_context(|| format!("CREATE DATABASE {db}"))?;
    println!("[pg]   created database {db}");
    Ok(())
}

async fn drop_database(admin_url: &str, db: &str) -> Result<()> {
    let mut conn = sqlx::postgres::PgConnectOptions::from_str(admin_url)?
        .connect()
        .await
        .context("connecting to admin DB")?;
    // Kick existing sessions before dropping.
    sqlx::query(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(db)
    .execute(&mut conn)
    .await?;
    let quoted = quote_ident(db);
    conn.execute(&*format!("DROP DATABASE IF EXISTS {quoted}"))
        .await
        .with_context(|| format!("DROP DATABASE IF EXISTS {db}"))?;
    println!("[pg]   dropped database {db}");
    Ok(())
}

/// Postgres identifier-quoting: wrap in double quotes and double any embedded
/// double quotes.
fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_name_from_url_pulls_path() {
        let n = db_name_from_url("postgres://u:p@h:5432/mobile_verifier").unwrap();
        assert_eq!(n, "mobile_verifier");
    }

    #[test]
    fn redact_hides_password() {
        let r = redact("postgres://user:hunter2@host:5432/db");
        assert!(!r.contains("hunter2"));
        assert!(r.contains("user"));
    }

    #[test]
    fn quote_ident_escapes() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }
}
