#[macro_use]
extern crate diesel;

extern crate wavesexchange_log as log;

mod api;
mod config;
mod error;

use anyhow::Result;
use database::db;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_api_config().await?;

    let pg_pool = db::pool(&config.postgres)?;
    let repo = { api::repo::new(pg_pool.clone()) };

    api::server::start(
        config.api.port,
        config.api.metrics_port,
        config.api.rates_api_url,
        config.api.assets_api_url,
        repo,
    )
    .await?;

    Ok(())
}
