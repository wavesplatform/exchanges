#[macro_use]
extern crate diesel;

mod config;
mod consumer;
mod error;

use crate::consumer::storage::pg::PgConsumerRepo;
use anyhow::Result;
use database::db;
use tokio::select;

use wavesexchange_log::{error, info, warn};
use wavesexchange_warp::MetricsWarpBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_consumer_config().await?;

    info!(
        "Starting exchanges consumer with config: {:?}",
        config.consumer
    );

    let conn = db::pool(&config.postgres)?;

    let updates_src = consumer::updates::new(&config.consumer.blockchain_updates_url).await?;

    let storage = PgConsumerRepo::new(conn);

    let consumer_handle = consumer::start(
        config.consumer.starting_height,
        updates_src,
        storage,
        config.consumer.updates_per_request,
        config.consumer.max_wait_time_in_secs,
    );

    let metrics = MetricsWarpBuilder::new()
        .with_metrics_port(config.consumer.metrics_port)
        .run_async();

    select! {

        Err(err) = consumer_handle => {
            error!("{}", err);
            panic!("consumer panic: {}", err);
        },
        _ = metrics => error!("metrics stopped")

    };

    Ok(())
}
