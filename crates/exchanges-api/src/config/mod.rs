pub mod api;

#[derive(Clone)]
pub struct APIConfig {
    pub api: api::Config,
    pub postgres: database::config::Config,
}

pub async fn load_api_config() -> anyhow::Result<APIConfig> {
    let api_config = api::load()?;
    let postgres_config = database::config::load()?;

    Ok(APIConfig {
        api: api_config,
        postgres: postgres_config,
    })
}
