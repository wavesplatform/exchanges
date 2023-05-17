use super::{
    repo::{self},
    ExchangeAggregatesRequest, ExchangesAggregate, Interval,
};
use crate::{
    api::repo::Repo,
    error::{self, Error},
};
use bigdecimal::{BigDecimal, Zero};
use chrono::NaiveDate;
use itertools::Itertools;
use std::{collections::HashMap, convert::Infallible, sync::Arc};
use wavesexchange_apis::{
    rates::dto::{Rate, RateData},
    HttpClient as ApiHttpClient, RatesService,
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
    repo: repo::PgRepo,
) -> Result<(), anyhow::Error> {
    fn with_warp<R: Clone + Send + Sync + 'static>(
        r: R,
    ) -> impl Filter<Extract = (R,), Error = Infallible> + Clone {
        warp::any().map(move || r.clone())
    }

    let arc_repo = Arc::new(repo);

    let rates_api_client = ApiHttpClient::<RatesService>::builder()
        .with_base_url(rates_api_url.trim_end_matches(|c| c == '/' || c == ' ' || c == '?'))
        .build();

    let arc_rates = Arc::new(rates_api_client);

    let with_repo = with_warp(arc_repo.clone());
    let with_rates = with_warp(arc_rates.clone());

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
    rates: Arc<ApiHttpClient<RatesService>>,
    req: ExchangeAggregatesRequest,
) -> Result<List<ExchangesAggregate>, Rejection> {
    let req = ExchangeAggregatesRequest::default_merge(req)?;

    let db_items = repo.exchanges_aggregates(&req)?;
    let volume_base_asset = req.volume_base_asset.as_ref().unwrap();
    let fee_base_asset = req.fees_base_asset.as_ref().unwrap();

    let mut asset_pairs: Vec<(&str, &str)> = vec![];

    db_items.iter().for_each(|r| {
        asset_pairs.push((r.amount_asset_id.as_str(), volume_base_asset));

        asset_pairs.push((r.fee_asset_id.as_str(), fee_base_asset));
    });

    let rates_map = rates
        .rates(asset_pairs.into_iter().unique())
        .await
        .map_err(|e| Error::UpstreamAPIRequestError(e))?
        .data
        .into_iter()
        .filter_map(|rate| Some((rate.pair.clone(), rate)))
        .collect::<HashMap<_, _>>();

    // let rates_map: HashMap<String, Rate> = HashMap::new();
    let mut histogram: HashMap<NaiveDate, ExchangesAggregate> = HashMap::new();

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

    db_items.iter().for_each(|r| {
        let e = histogram.entry(r.sum_date).or_insert(ExchangesAggregate {
            uid: 0,
            interval: Interval::Day1,
            interval_start: r.sum_date.and_hms_opt(0, 0, 0).unwrap(),
            interval_end: r.sum_date.and_hms_opt(23, 59, 59).unwrap(),
            volume: BigDecimal::zero(),
            fees: BigDecimal::zero(),
            count: 0,
        });

        let amount_rate_key = format!("{}/{}", r.amount_asset_id, volume_base_asset);
        let amount_rate = rates_map.get(&amount_rate_key).unwrap_or(&zero_rate);

        (*e).volume += r.amount_sum.clone() * amount_rate.data.rate.clone();
        (*e).count += r.count.clone();

        let fee_rate_key = format!("{}/{}", r.fee_asset_id, fee_base_asset);
        let fee_rate = rates_map.get(&fee_rate_key).unwrap_or(&zero_rate);

        (*e).fees += r.fee_sum.clone() * fee_rate.data.rate.clone();
    });

    let mut items = vec![];
    let mut last_cursor = 0;
    let mut has_next_page = false;

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

    let res = List {
        items,
        page_info: PageInfo {
            has_next_page,
            last_cursor: Some(format!("{}", last_cursor)),
        },
    };

    Ok(res)
}
