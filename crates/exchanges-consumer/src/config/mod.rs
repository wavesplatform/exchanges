mod consumer;

#[derive(Clone)]
pub struct ConsumerConfig {
    pub consumer: consumer::Config,
    pub postgres: database::config::Config,
}

pub async fn load_consumer_config() -> anyhow::Result<ConsumerConfig> {
    let consumer_config = consumer::load()?;
    let postgres_config = database::config::load()?;

    Ok(ConsumerConfig {
        consumer: consumer_config,
        postgres: postgres_config,
    })
}
