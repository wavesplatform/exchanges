use serde::Deserialize;

fn default_updates_per_request() -> usize {
    100
}

fn default_max_wait_time_in_secs() -> u64 {
    5
}

fn default_metrics_port() -> u16 {
    9090
}

#[derive(Deserialize)]
struct ConfigFlat {
    #[serde(default = "default_metrics_port")]
    metrics_port: u16,
    blockchain_updates_url: String,
    starting_height: u32,
    #[serde(default = "default_updates_per_request")]
    updates_per_request: usize,
    #[serde(default = "default_max_wait_time_in_secs")]
    max_wait_time_in_secs: u64,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub metrics_port: u16,
    pub blockchain_updates_url: String,
    pub starting_height: u32,
    pub updates_per_request: usize,
    pub max_wait_time_in_secs: u64,
}

pub fn load() -> anyhow::Result<Config> {
    let config_flat = envy::from_env::<ConfigFlat>()?;

    Ok(Config {
        metrics_port: config_flat.metrics_port,
        blockchain_updates_url: config_flat.blockchain_updates_url,
        starting_height: config_flat.starting_height,
        updates_per_request: config_flat.updates_per_request,
        max_wait_time_in_secs: config_flat.max_wait_time_in_secs,
    })
}
