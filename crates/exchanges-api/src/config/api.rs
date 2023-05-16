use serde::Deserialize;

fn default_port() -> u16 {
    8080
}

fn default_metrics_port() -> u16 {
    9090
}

#[derive(Deserialize)]
struct ConfigFlat {
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_metrics_port")]
    metrics_port: u16,
    rates_api_url: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub metrics_port: u16,
    pub rates_api_url: String,
}

pub fn load() -> anyhow::Result<Config> {
    let api_config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        port: api_config_flat.port,
        metrics_port: api_config_flat.metrics_port,
        rates_api_url: api_config_flat.rates_api_url,
    })
}
