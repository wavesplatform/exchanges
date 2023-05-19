use super::{
    apply_decimals,
    repo::{self},
    ExchangeAggregatesRequest, ExchangesAggregate, Interval,
};
use crate::{
    api::repo::Repo,
    error::{self, Error},
};
use bigdecimal::{BigDecimal, Zero};
use chrono::{Days, NaiveDate};
use itertools::Itertools;
use shared::bigdecimal::round;
use std::{collections::HashMap, convert::Infallible, sync::Arc};
use wavesexchange_apis::{
    assets::dto::{AssetInfo, OutputFormat},
    rates::dto::{Rate, RateData},
    AssetsService, HttpClient as ApiHttpClient, RatesService,
};

use warp::{Filter, Rejection};
use wavesexchange_log::{debug, error, info, timer, warn};
use wavesexchange_warp::{
    error::{error_handler_with_serde_qs, handler, internal, not_found, timeout, validation},
    log::access,
    pagination::{List, PageInfo},
    MetricsWarpBuilder,
};

const ERROR_CODES_PREFIX: u16 = 95;

pub async fn start(
    port: u16,
    metrics_port: u16,
    rates_api_url: String,
    assets_api_url: String,
    repo: repo::PgRepo,
) -> Result<(), anyhow::Error> {
    fn with_warp<R: Clone + Send + Sync + 'static>(
        r: R,
    ) -> impl Filter<Extract = (R,), Error = Infallible> + Clone {
        warp::any().map(move || r.clone())
    }

    let f_trim = |c| c == '/' || c == ' ' || c == '?';

    let rates_api_client = ApiHttpClient::<RatesService>::builder()
        .with_base_url(rates_api_url.trim_end_matches(f_trim))
        .build();

    let assets_api_client = ApiHttpClient::<AssetsService>::builder()
        .with_base_url(assets_api_url.trim_end_matches(f_trim))
        .build();

    let arc_repo = Arc::new(repo);
    let with_repo = with_warp(arc_repo.clone());

    let arc_rates = Arc::new(rates_api_client);
    let with_rates = with_warp(arc_rates.clone());

    let arc_asssets = Arc::new(assets_api_client);
    let with_assets = with_warp(arc_asssets.clone());

    let error_handler = handler(ERROR_CODES_PREFIX, |err| match err {
        error::Error::ValidationError(_error_message, error_details) => {
            validation::invalid_parameter(
                ERROR_CODES_PREFIX,
                error_details.to_owned().map(|details| details.into()),
            )
        }
        error::Error::DbError(error_message)
            if error_message.to_string() == "canceling statement due to statement timeout" =>
        {
            error!("{:?}", err);
            timeout(ERROR_CODES_PREFIX)
        }
        error::Error::NotFound => not_found(ERROR_CODES_PREFIX),
        _ => {
            error!("{:?}", err);
            internal(ERROR_CODES_PREFIX)
        }
    });

    let create_serde_qs_config = || serde_qs::Config::new(5, false);

    let handler = warp::path!("interval_exchanges")
        .and(warp::get())
        .and(with_repo.clone())
        .and(with_rates.clone())
        .and(with_assets.clone())
        .and(serde_qs::warp::query::<ExchangeAggregatesRequest>(
            create_serde_qs_config(),
        ))
        .and_then(interval_exchanges)
        .map(|res| warp::reply::json(&res));

    let log = warp::log::custom(access);

    info!("Starting web server at 0.0.0.0:{}", port);

    let routes = handler
        .recover(move |rej| {
            error_handler_with_serde_qs(ERROR_CODES_PREFIX, error_handler.clone())(rej)
        })
        .with(log);

    MetricsWarpBuilder::new()
        .with_main_routes(routes)
        .with_main_routes_port(port)
        .with_metrics_port(metrics_port)
        .run_async()
        .await;

    Ok(())
}

async fn interval_exchanges(
    repo: Arc<repo::PgRepo>,
    rates_client: Arc<ApiHttpClient<RatesService>>,
    assets_client: Arc<ApiHttpClient<AssetsService>>,
    req: ExchangeAggregatesRequest,
) -> Result<List<ExchangesAggregate>, Rejection> {
    let req = ExchangeAggregatesRequest::default_merge(req)?;

    let db_items = repo.exchanges_aggregates(&req)?;

    let volume_base_asset = req.volume_base_asset.as_ref().unwrap();
    let fees_base_asset = req.fees_base_asset.as_ref().unwrap();

    let mut asset_pairs: Vec<(&str, &str)> = vec![];
    let mut assets: Vec<&str> = Vec::new();

    db_items.iter().for_each(|r| {
        asset_pairs.push((r.amount_asset_id.as_str(), volume_base_asset));

        asset_pairs.push((r.fee_asset_id.as_str(), fees_base_asset));

        assets.push(r.amount_asset_id.as_str());
        assets.push(r.fee_asset_id.as_str());
    });

    let assets_decimals = assets_client
        .get(assets.into_iter().unique(), None, OutputFormat::Full, false)
        .await
        .map_err(|e| Error::UpstreamAPIRequestError(e))?
        .data
        .into_iter()
        .filter_map(|info| match info.data {
            Some(AssetInfo::Full(a)) => Some((a.id, a.precision)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    let rates_map = rates_client
        .rates(asset_pairs.into_iter().unique())
        .await
        .map_err(|e| Error::UpstreamAPIRequestError(e))?
        .data
        .into_iter()
        .filter_map(|rate| Some((rate.pair.clone(), rate)))
        .collect::<HashMap<_, _>>();

    let mut histogram = HashMap::new();

    /*
        В rates получаются много пар которые не имеют курса.
        для таких пар в агрегации будут прибавляться 0
        не знаю на сколько этоправильно
    */
    let zero_rate = Rate {
        pair: "".to_owned(),
        heuristics: vec![],
        data: RateData {
            rate: BigDecimal::zero(),
            heuristic: None,
            exchange: None,
        },
    };

    let mut min_date = None::<NaiveDate>;
    let mut max_date = None::<NaiveDate>;

    for r in db_items {
        let e = histogram
            .entry(r.sum_date)
            .or_insert(ExchangesAggregate::empty(r.sum_date));

        match min_date {
            Some(min_d) => {
                if min_d > r.sum_date {
                    min_date = Some(r.sum_date)
                }
            }
            None => min_date = Some(r.sum_date),
        }

        match max_date {
            Some(max_d) => {
                if max_d < r.sum_date {
                    max_date = Some(r.sum_date)
                }
            }
            None => max_date = Some(r.sum_date),
        }

        let amount_dec = match assets_decimals.get(&r.amount_asset_id) {
            Some(d) => *d as i64,
            _ => {
                return Err(Error::UpstreamAPIBadResponse(format!(
                    "can't get decimals {} from asset service",
                    &r.amount_asset_id
                ))
                .into());
            }
        };

        let fee_dec = match assets_decimals.get(&r.fee_asset_id) {
            Some(d) => *d as i64,
            _ => {
                return Err(Error::UpstreamAPIBadResponse(format!(
                    "can't get decimals {} from asset service",
                    &r.fee_asset_id
                ))
                .into());
            }
        };

        let amount_rate_key = format!("{}/{}", r.amount_asset_id, volume_base_asset);
        let amount_rate = rates_map.get(&amount_rate_key).unwrap_or(&zero_rate);

        (*e).volume += round(
            apply_decimals(r.amount_sum, amount_dec) * amount_rate.data.rate.clone(),
            amount_dec,
        );
        (*e).count += r.count.clone();

        let fee_rate_key = format!("{}/{}", r.fee_asset_id, fees_base_asset);
        let fee_rate = rates_map.get(&fee_rate_key).unwrap_or(&zero_rate);

        (*e).fees += round(
            apply_decimals(r.fee_sum, fee_dec) * fee_rate.data.rate.clone(),
            fee_dec,
        );
    }

    let mut items = vec![];
    let mut last_cursor = 0;
    let mut has_next_page = false;

    if req.block_timestamp_gte.is_some() {
        min_date = Some(req.block_timestamp_gte.unwrap().date_naive());
    }

    if req.block_timestamp_lt.is_some() {
        max_date = Some(req.block_timestamp_lt.unwrap().date_naive());
    }

    if let (Some(min_date), Some(max_date)) = (min_date, max_date) {
        let mut cur_date = max_date;
        let mut n = 0;

        while cur_date >= min_date {
            if let Some(after) = req.after {
                if n < (after as usize) {
                    n += 1;

                    match max_date.checked_sub_days(Days::new(n as u64)) {
                        Some(d) => cur_date = d,
                        _ => break,
                    }

                    continue;
                }
            }

            let mut out_item = match histogram.get(&cur_date) {
                Some(h) => h.clone(),
                None => ExchangesAggregate::empty(cur_date),
            };

            out_item.uid = (n + 1) as i64;
            last_cursor = (n + 1) as i64;

            items.push(out_item);

            match max_date.checked_sub_days(Days::new(n as u64)) {
                Some(d) => cur_date = d,
                _ => break,
            }

            if let (Some(after), Some(limit)) = (req.after, req.limit) {
                if n + 1 >= (after + limit) as usize {
                    has_next_page = histogram.get(&cur_date).is_some();
                    break;
                }
            }

            n += 1;
        }
    }

    /*
    let mut h_keys = histogram.keys().sorted().rev().enumerate();

        while let Some((n, h)) = h_keys.next() {
            if let Some(after) = req.after {
                if n < (after as usize) {
                    continue;
                }
            }

            let mut out_item = histogram.get(h).unwrap().clone();
            out_item.uid = (n + 1) as i64;
            last_cursor = (n + 1) as i64;

            items.push(out_item);

            if let (Some(after), Some(limit)) = (req.after, req.limit) {
                if n + 1 >= (after + limit) as usize {
                    has_next_page = h_keys.next().is_some();
                    break;
                }
            }
        }
    */

    let res = List {
        items,
        page_info: PageInfo {
            has_next_page,
            last_cursor: Some(format!("{}", last_cursor)),
        },
    };

    Ok(res)
}
