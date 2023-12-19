#[macro_use]
extern crate diesel;

mod config;
mod consumer;
mod error;

use crate::consumer::storage::pg::PgConsumerRepo;
use anyhow::Result;
use database::db;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use wavesexchange_liveness::channel;
use wavesexchange_log::{error, info};
use wavesexchange_warp::MetricsWarpBuilder;

const POLL_INTERVAL_SECS: u64 = 60;
const MAX_BLOCK_AGE: Duration = Duration::from_secs(300);

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::load_consumer_config().await?;

    info!(
        "Starting exchanges consumer with config: {:?}",
        config.consumer
    );

    let conn = db::pool(&config.postgres)?;

    let updates_src = consumer::updates::new(&config.consumer.blockchain_updates_url).await?;

    let mut storage = PgConsumerRepo::new(conn);

    let consumer_handle = consumer::start(
        config.consumer.starting_height,
        updates_src,
        &mut storage,
        config.consumer.updates_per_request,
        config.consumer.max_wait_time_in_secs,
        Arc::new(config.consumer.matcher_address),
    );

    let db_url = config.postgres.database_url();
    let readiness_channel = channel(db_url, POLL_INTERVAL_SECS, MAX_BLOCK_AGE);

    let metrics = tokio::spawn(async move {
        MetricsWarpBuilder::new()
            .with_metrics_port(config.consumer.metrics_port)
            .with_readiness_channel(readiness_channel)
            .run_async()
            .await
    });

    select! {

        Err(err) = consumer_handle => {
            error!("{}", err);
            panic!("consumer panic: {}", err);
        },
        _ = metrics => error!("metrics stopped")

    };

    Ok(())
}
