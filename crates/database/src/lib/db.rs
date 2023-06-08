use crate::config::Config;
use diesel::{
    pg::PgConnection,
    r2d2::{ConnectionManager, Pool},
    Connection,
};
use r2d2::PooledConnection;
use std::time::Duration;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type PooledPgConnection = PooledConnection<ConnectionManager<PgConnection>>;

fn generate_postgres_url(
    user: &str,
    password: &str,
    host: &str,
    port: &u16,
    database: &str,
) -> String {
    format!(
        "postgres://{}:{}@{}:{}/{}",
        user, password, host, port, database
    )
}

pub fn pool(config: &Config) -> anyhow::Result<PgPool> {
    let db_url = generate_postgres_url(
        &config.user,
        &config.password,
        &config.host,
        &config.port,
        &config.database,
    );

    let manager = ConnectionManager::<PgConnection>::new(db_url);
    Ok(Pool::builder()
        .min_idle(Some(2))
        .max_size(config.pool_size as u32)
        .idle_timeout(Some(Duration::from_secs(5 * 60)))
        .build(manager)?)
}

pub fn unpooled(config: &Config) -> anyhow::Result<PgConnection> {
    let db_url = generate_postgres_url(
        &config.user,
        &config.password,
        &config.host,
        &config.port,
        &config.database,
    );

    PgConnection::establish(&db_url).map_err(|err| anyhow::anyhow!(err))
}
