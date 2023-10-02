//! Asset Service client to retrieve decimals, with cache

use async_trait::async_trait;
use itertools::Itertools;
use std::{collections::HashMap, hash::Hash};
use wavesexchange_apis::{assets::dto, AssetsService, HttpClient};
use wavesexchange_loaders::{CachedLoader, Loader as _, LoaderError, TimedCache};

pub type Asset = String;
pub type Decimals = i64;

/// Map of asset_id -> decimals.
pub struct AssetDecimalsMap {
    map: HashMap<Asset, Decimals>,
}

impl AssetDecimalsMap {
    pub fn get(&self, asset_id: &str) -> Result<Decimals, AssetError> {
        self.map
            .get(asset_id)
            .map(|&decimals| decimals)
            .ok_or_else(|| AssetError::DecimalsNotFound(asset_id.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AssetError {
    #[error("can't get decimals of '{0}' from asset service")]
    DecimalsNotFound(String),
}

#[derive(Clone)]
pub struct AssetServiceGateway {
    asset_service_client: HttpClient<AssetsService>,
}

pub type AssetServiceGatewayError = LoaderError<wavesexchange_apis::Error>;

pub type GWResult<T> = Result<T, AssetServiceGatewayError>;

// Special case for USD pseudo-asset: return hardcoded 2 decimals
const USD: &str = "USD";
const USD_DECIMALS: Decimals = 2;

impl AssetServiceGateway {
    pub fn new(asset_service_client: HttpClient<AssetsService>) -> Self {
        AssetServiceGateway {
            asset_service_client,
        }
    }

    /*pub fn from_url(asset_service_url: impl AsRef<str>) -> Self {
        let url = asset_service_url.as_ref();
        let asset_service_client = HttpClient::<AssetsService>::from_base_url(url);
        AssetServiceGateway {
            asset_service_client,
        }
    }*/

    /// Asynchronously request decimals for a list of assets from the asset service, with cache.
    pub async fn decimals_map(
        &self,
        asset_ids: impl IntoIterator<Item = impl Into<String> + Clone + Eq + Hash>,
    ) -> GWResult<AssetDecimalsMap> {
        let assets = asset_ids.into_iter().map(Into::into).collect_vec();
        let map = self.load_many(assets).await?;
        Ok(AssetDecimalsMap { map })
    }
}

#[async_trait]
impl CachedLoader<Asset, Decimals> for AssetServiceGateway {
    type Cache = TimedCache<Asset, Decimals>;

    type Error = wavesexchange_apis::Error;

    async fn load_fn(&mut self, keys: &[Asset]) -> Result<Vec<Decimals>, Self::Error> {
        log::trace!("load_fn(): {:?}", keys);

        let asset_ids = keys;
        let assets = self
            .asset_service_client
            .get(asset_ids, None, dto::OutputFormat::Full, false)
            .await?;
        assert_eq!(assets.data.len(), asset_ids.len(), "Broken API: length");

        Ok(assets
            .data
            .into_iter()
            .zip(keys)
            .map(|(asset, asset_id)| match asset.data {
                Some(dto::AssetInfo::Full(a)) => {
                    assert!(
                        a.precision >= 0 && a.precision < 30,
                        "Suspicious precision {} for asset {}",
                        a.precision,
                        a.id
                    );
                    assert_eq!(asset_id, &a.id, "Broken API: order of assets");
                    a.precision as Decimals
                }
                Some(dto::AssetInfo::Brief(_)) => {
                    unreachable!("Broken API: Full info expected for asset {}", asset_id);
                }
                None => {
                    if asset_id == USD {
                        USD_DECIMALS
                    } else {
                        panic!("Broken API: Missing asset {}", asset_id);
                    }
                }
            })
            .collect())
    }

    fn init_cache() -> Self::Cache {
        const ONE_DAY: u64 = 60 * 60 * 24;
        //TODO Ugly API requiring raw seconds instead of a Duration. Find ways to refactor.
        TimedCache::with_lifespan(ONE_DAY)
    }
}
