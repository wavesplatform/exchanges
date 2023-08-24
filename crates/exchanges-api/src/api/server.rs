use super::{
    apply_decimals,
    repo::{self},
    ExchangeAggregatesItem, ExchangeAggregatesRequest, IntervalExchangeItem,
    IntervalExchangesRequest, MatcherExchangeAggregatesItem, MatcherExchangeAggregatesRequest,
    NaiveDateInterval, PnlAggregatesItem, PnlAggregatesRequest,
};
use crate::{
    api::repo::Repo,
    error::{self, Error},
};
use bigdecimal::{BigDecimal, Zero};
use chrono::{Days, NaiveDate};
use itertools::Itertools;
use shared::bigdecimal::round;
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    iter::once,
    sync::Arc,
};
use warp::{Filter, Rejection};
use wavesexchange_apis::{
    assets::dto::{AssetInfo, OutputFormat},
    rates::dto::{Rate, RateData},
    AssetsService, HttpClient as ApiHttpClient, RatesService,
};
use wavesexchange_log::{error, info};
use wavesexchange_warp::{
    error::{error_handler_with_serde_qs, handler, internal, timeout, validation},
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

    let arc_assets = Arc::new(assets_api_client);
    let with_assets = with_warp(arc_assets.clone());

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
        _ => {
            error!("{:?}", err);
            internal(ERROR_CODES_PREFIX)
        }
    });

    let create_serde_qs_config = || serde_qs::Config::new(5, false);

    let interval_exchanges_handler = warp::path!("interval_exchanges")
        .and(warp::get())
        .and(with_repo.clone())
        .and(with_rates.clone())
        .and(with_assets.clone())
        .and(serde_qs::warp::query::<IntervalExchangesRequest>(
            create_serde_qs_config(),
        ))
        .and_then(interval_exchanges)
        .map(|res| warp::reply::json(&res));

    let exchange_aggregates_handler = warp::path!("exchange_aggregates")
        .and(warp::get())
        .and(with_repo.clone())
        .and(with_rates.clone())
        .and(with_assets.clone())
        .and(serde_qs::warp::query::<ExchangeAggregatesRequest>(
            create_serde_qs_config(),
        ))
        .and_then(exchange_aggregates)
        .map(|res| warp::reply::json(&res));

    let matcher_exchange_aggregates_handler = warp::path!("matcher_exchange_aggregates")
        .and(warp::get())
        .and(with_repo.clone())
        .and(with_assets.clone())
        .and(serde_qs::warp::query::<MatcherExchangeAggregatesRequest>(
            create_serde_qs_config(),
        ))
        .and_then(matcher_exchange_aggregates)
        .map(|res| warp::reply::json(&res));

    let pnl_aggregates_handler = warp::path!("pnl_aggregates")
        .and(warp::get())
        .and(with_repo.clone())
        .and(with_rates.clone())
        .and(with_assets.clone())
        .and(serde_qs::warp::query::<PnlAggregatesRequest>(
            create_serde_qs_config(),
        ))
        .and_then(pnl_aggregates)
        .map(|res| warp::reply::json(&res));

    let log = warp::log::custom(access);

    info!("Starting web server at 0.0.0.0:{}", port);

    let routes = interval_exchanges_handler
        .or(exchange_aggregates_handler)
        .or(matcher_exchange_aggregates_handler)
        .or(pnl_aggregates_handler)
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
    req: IntervalExchangesRequest,
) -> Result<List<IntervalExchangeItem>, Rejection> {
    let req = IntervalExchangesRequest::default_merge(req)?;

    let db_items = repo.interval_exchanges(&req)?;

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
        .rates(asset_pairs.into_iter().unique(), req.block_timestamp_lt)
        .await
        .map_err(|e| Error::UpstreamAPIRequestError(e))?
        .data
        .into_iter()
        .filter_map(|rate| Some((rate.pair.clone(), rate)))
        .collect::<HashMap<_, _>>();

    let mut histogram = HashMap::new();

    /*
        В rates получаются много пар, которые не имеют курса.
        Для таких пар курс будет считаться 0.
        На результат агрегации они влиять не будут.
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
            .or_insert(IntervalExchangeItem::empty(r.sum_date));

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
        let dates = date_interval(min_date, max_date);
        let mut h_keys = dates.iter().enumerate();

        while let Some((n, h)) = h_keys.next() {
            if let Some(after) = req.after {
                if n < (after as usize) {
                    continue;
                }
            }

            let out_item = histogram
                .get(h)
                .cloned()
                .unwrap_or(IntervalExchangeItem::empty(*h));

            last_cursor = (n + 1) as i64;

            items.push(out_item);

            if let (Some(after), Some(limit)) = (req.after, req.limit) {
                if n + 1 >= (after + limit) as usize {
                    has_next_page = h_keys.next().is_some();
                    break;
                }
            }
        }
    }

    let res = List {
        items,
        page_info: PageInfo {
            has_next_page,
            last_cursor: Some(format!("{}", last_cursor)),
        },
    };

    Ok(res)
}

async fn exchange_aggregates(
    repo: Arc<repo::PgRepo>,
    rates_client: Arc<ApiHttpClient<RatesService>>,
    assets_client: Arc<ApiHttpClient<AssetsService>>,
    req: ExchangeAggregatesRequest,
) -> Result<List<ExchangeAggregatesItem>, Rejection> {
    let req = ExchangeAggregatesRequest::default_merge(req)?;

    let db_items = repo.exchange_aggregates(&req)?;

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
        .rates(asset_pairs.into_iter().unique(), req.block_timestamp_lt)
        .await
        .map_err(|e| Error::UpstreamAPIRequestError(e))?
        .data
        .into_iter()
        .filter_map(|rate| Some((rate.pair.clone(), rate)))
        .collect::<HashMap<_, _>>();

    let mut histogram: HashMap<String, ExchangeAggregatesItem> = HashMap::new();

    let zero_rate = Rate {
        pair: "".to_owned(),
        heuristics: vec![],
        data: RateData {
            rate: BigDecimal::zero(),
            heuristic: None,
            exchange: None,
        },
    };

    for r in db_items {
        let e = histogram
            .entry(r.sender.clone())
            .or_insert(ExchangeAggregatesItem::empty(r.sender.clone()));

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

    let items = histogram
        .keys()
        .sorted()
        .map(|i| histogram.get(i).unwrap().clone())
        .collect();

    let res = List {
        items,
        page_info: PageInfo {
            has_next_page: false,
            last_cursor: None,
        },
    };

    Ok(res)
}

async fn matcher_exchange_aggregates(
    repo: Arc<repo::PgRepo>,
    assets_client: Arc<ApiHttpClient<AssetsService>>,
    req: MatcherExchangeAggregatesRequest,
) -> Result<List<MatcherExchangeAggregatesItem>, Rejection> {
    let req = MatcherExchangeAggregatesRequest::default_merge(req)?;

    let db_items = repo.matcher_exchange_aggregates(&req)?;

    let mut assets = HashSet::new();

    db_items.iter().for_each(|r| {
        assets.insert(r.amount_asset_id.as_str());
        assets.insert(r.price_asset_id.as_str());
    });

    let assets_decimals = assets_client
        .get(assets.into_iter(), None, OutputFormat::Full, false)
        .await
        .map_err(|e| Error::UpstreamAPIRequestError(e))?
        .data
        .into_iter()
        .filter_map(|info| match info.data {
            Some(AssetInfo::Full(a)) => Some((a.id, a.precision)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    let get_decimals = |asset_id: &str| -> Result<i64, Error> {
        match assets_decimals.get(asset_id) {
            Some(d) => Ok(*d as i64),
            None => Err(Error::UpstreamAPIBadResponse(format!(
                "can't get decimals {} from asset service",
                asset_id
            ))
            .into()),
        }
    };

    let mut items = vec![];

    for r in db_items {
        //let price_dec = get_decimals(&r.price_asset_id)?;
        let amount_dec = get_decimals(&r.amount_asset_id)?;
        let price_dec = 8; // Order v3 - fixed 8 decimals for price

        let mut item =
            MatcherExchangeAggregatesItem::empty(r.amount_asset_id, r.price_asset_id, r.agg_date);

        item.total_amount = apply_decimals(r.total_amount, amount_dec);
        item.price_open = apply_decimals(r.price_open, price_dec);
        item.price_close = apply_decimals(r.price_close, price_dec);
        item.price_high = apply_decimals(r.price_high, price_dec);
        item.price_low = apply_decimals(r.price_low, price_dec);

        items.push(item);
    }

    let res = List {
        items,
        page_info: PageInfo {
            has_next_page: false,
            last_cursor: None,
        },
    };

    Ok(res)
}

async fn pnl_aggregates(
    repo: Arc<repo::PgRepo>,
    rates_client: Arc<ApiHttpClient<RatesService>>,
    assets_client: Arc<ApiHttpClient<AssetsService>>,
    req: PnlAggregatesRequest,
) -> Result<List<PnlAggregatesItem>, Rejection> {
    let req = PnlAggregatesRequest::default_merge(req)?;

    // Output currency, default USD
    let out_asset = req.pnl_asset.as_ref().expect("output asset").as_str();

    // Aggregates with fixed interval 1day
    let db_items = repo.pnl_aggregates(&req)?;

    let assets = db_items
        .iter()
        .map(|it| [it.amount_asset_id.as_str(), it.price_asset_id.as_str()])
        .flatten()
        .collect::<HashSet<_>>();

    let assets_decimals = assets_client
        .get(
            assets.iter().map(|&a| a).chain(once(out_asset)),
            None,
            OutputFormat::Full,
            false,
        )
        .await
        .map_err(|e| Error::UpstreamAPIRequestError(e))?
        .data
        .into_iter()
        .filter_map(|asset| match asset.data {
            Some(AssetInfo::Full(a)) => Some((a.id, a.precision)),
            _ => None,
        })
        .chain(once(("USD".to_string(), 2)))
        .collect::<HashMap<_, _>>();

    let get_decimals = |asset_id: &str| -> Result<i64, Error> {
        match assets_decimals.get(asset_id) {
            Some(d) => Ok(*d as i64),
            None => Err(Error::UpstreamAPIBadResponse(format!(
                "can't get decimals {} from asset service",
                asset_id
            ))
            .into()),
        }
    };

    struct Item<'a> {
        interval: NaiveDateInterval,
        /// Asset ID -> Volume (with applied decimals)
        volumes: Vec<(&'a str, BigDecimal)>,
    }

    let mut daily = HashMap::new();
    for row in &db_items {
        let amount_dec = get_decimals(&row.amount_asset_id)?;
        let price_dec = get_decimals(&row.price_asset_id)?;

        let delta_base_vol = apply_decimals(&row.delta_base_vol, amount_dec);
        let delta_quote_vol = apply_decimals(&row.delta_quote_vol, price_dec);

        let entry = daily.entry(row.agg_date).or_insert_with(|| Item {
            interval: NaiveDateInterval::new(row.agg_date),
            volumes: vec![],
        });
        entry.volumes.push((&row.amount_asset_id, delta_base_vol));
        entry.volumes.push((&row.price_asset_id, delta_quote_vol));
    }

    let mut items = vec![];

    for date in daily.keys().sorted() {
        let entry = &daily[date];

        let rate_assets = entry.volumes.iter().map(|&(asset, _)| (asset, out_asset));

        let assets_rates = rates_client
            .rates(rate_assets, Some(entry.interval.interval_end.and_utc()))
            .await
            .map_err(Error::UpstreamAPIRequestError)?
            .data
            .into_iter()
            .map(|rate| rate.data.rate);

        let volumes_with_rates = entry
            .volumes
            .iter()
            .zip(assets_rates)
            .map(|(&(_, ref volume), rate)| (volume.clone(), rate));

        let sum_pnl = volumes_with_rates.fold(BigDecimal::zero(), |acc, (volume, rate)| {
            if rate > BigDecimal::zero() {
                acc + volume / rate
            } else {
                acc
            }
        });

        //TODO support intervals other than 1d - pass r.interval as parameter and use properly
        let mut item = PnlAggregatesItem::empty(*date);

        item.pnl = sum_pnl.with_scale(get_decimals(out_asset)?);

        items.push(item);
    }

    let res = List {
        items,
        page_info: PageInfo {
            has_next_page: false,
            last_cursor: None,
        },
    };

    Ok(res)
}

fn date_interval(min_date: NaiveDate, max_date: NaiveDate) -> Vec<NaiveDate> {
    let mut out = vec![];
    let mut cur_date = max_date.clone();
    out.push(max_date);

    while cur_date > min_date {
        cur_date = match cur_date.clone().checked_sub_days(Days::new(1)) {
            Some(d) => {
                out.push(d.clone());
                d
            }
            _ => return out,
        };
    }
    out
}
