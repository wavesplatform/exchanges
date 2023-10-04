pub mod repo;
pub mod server;

use crate::error::{self, Error};
use bigdecimal::{BigDecimal, Zero};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use diesel::sql_types::{Date, Int8, Numeric, Text};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::{convert::TryFrom, fmt::Display};

pub(crate) fn apply_decimals(num: impl Borrow<BigDecimal>, dec: impl Into<i64>) -> BigDecimal {
    let dec = dec.into();
    (num.borrow() / (10i64.pow(dec as u32))).with_scale(dec)
}

#[derive(Debug)]
struct QsError(serde_qs::Error);

#[derive(Debug, Deserialize)]
pub struct CursorQuery {
    pub after: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum Interval {
    #[serde(rename = "1d")]
    Day1,
}

impl Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Interval::Day1 => write!(f, "Day1"),
        }
    }
}

impl TryFrom<&str> for Interval {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "1d" => Ok(Self::Day1),
            _ => Err(error::Error::ValidationError(
                format!("Interval {} is invalid. Should be one of: 1d", value),
                None,
            )),
        }
    }
}

#[derive(Clone, Debug, Queryable, QueryableByName)]
pub(crate) struct ExchangeAggregateDbRow {
    #[sql_type = "Text"]
    pub sender: String,
    #[sql_type = "Text"]
    pub amount_asset_id: String,
    #[sql_type = "Text"]
    pub fee_asset_id: String,
    #[sql_type = "Numeric"]
    pub amount_sum: BigDecimal,
    #[sql_type = "Numeric"]
    pub fee_sum: BigDecimal,
    #[sql_type = "Int8"]
    pub count: i64,
}

#[derive(Clone, Debug, Queryable, QueryableByName)]
pub(crate) struct IntervalExchangeDbRow {
    #[sql_type = "Date"]
    pub sum_date: NaiveDate,
    // #[sql_type = "Text"]
    // sender: String,
    #[sql_type = "Text"]
    pub amount_asset_id: String,
    #[sql_type = "Text"]
    pub fee_asset_id: String,
    #[sql_type = "Numeric"]
    pub amount_sum: BigDecimal,
    #[sql_type = "Numeric"]
    pub fee_sum: BigDecimal,
    #[sql_type = "Int8"]
    pub count: i64,
}

#[derive(Clone, Debug, Queryable, QueryableByName)]
pub(crate) struct MatcherExchangeDbRow {
    #[sql_type = "Date"]
    pub agg_date: NaiveDate,
    #[sql_type = "Text"]
    pub amount_asset_id: String,
    #[sql_type = "Text"]
    pub price_asset_id: String,
    #[sql_type = "Numeric"]
    pub total_amount: BigDecimal,
    #[sql_type = "Numeric"]
    pub price_open: BigDecimal,
    #[sql_type = "Numeric"]
    pub price_close: BigDecimal,
    #[sql_type = "Numeric"]
    pub price_high: BigDecimal,
    #[sql_type = "Numeric"]
    pub price_low: BigDecimal,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IntervalExchangesRequest {
    pub interval: Option<Interval>,
    #[serde(rename = "block_timestamp__gte")]
    pub block_timestamp_gte: Option<DateTime<Utc>>,
    #[serde(rename = "block_timestamp__lt")]
    pub block_timestamp_lt: Option<DateTime<Utc>>,
    pub order_sender: Option<String>,
    #[serde(rename = "order_sender__in")]
    pub order_sender_in: Option<Vec<String>>,
    pub volume_base_asset: Option<String>,
    pub fees_base_asset: Option<String>,
    pub limit: Option<u32>,
    pub after: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename = "interval_exchange")]
pub(crate) struct IntervalExchangeItem {
    interval: Interval,
    interval_start: NaiveDateTime,
    interval_end: NaiveDateTime,
    volume: BigDecimal,
    fees: BigDecimal,
    count: i64,
}

impl IntervalExchangeItem {
    pub fn empty(d: NaiveDate) -> Self {
        Self {
            interval: Interval::Day1,
            interval_start: d.and_hms_opt(0, 0, 0).unwrap(),
            interval_end: d.and_hms_opt(23, 59, 59).unwrap(),
            volume: BigDecimal::zero(),
            fees: BigDecimal::zero(),
            count: 0,
        }
    }
}

impl IntervalExchangesRequest {
    fn default_merge(req: Self) -> Result<Self, Error> {
        let mut def = Self::default();
        if req.interval.is_some() {
            def.interval = req.interval;
        }

        if req.block_timestamp_gte.is_some() {
            def.block_timestamp_gte = req.block_timestamp_gte;
        }

        def.block_timestamp_lt = match req.block_timestamp_lt {
            Some(d) => Some(d),
            None => Some(
                Utc::now()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap(),
            ),
        };

        let mut senders = vec![];

        if req.order_sender_in.is_some() {
            senders = req.order_sender_in.unwrap();
        }

        if req.order_sender.is_some() {
            senders.push(req.order_sender.unwrap());
            senders = senders.into_iter().unique().collect_vec();
        }

        def.order_sender_in = match senders.len() {
            0 => {
                return Err(
                    Error::ValidationError("invalid sender query param".into(), None).into(),
                )
            }
            _ => Some(senders),
        };

        if req.volume_base_asset.is_some() {
            def.volume_base_asset = req.volume_base_asset;
        }

        if req.fees_base_asset.is_some() {
            def.fees_base_asset = req.fees_base_asset;
        }

        let lim = 100 as u32;
        if req.limit.is_some() {
            def.limit = Some(lim.min(req.limit.unwrap()));
        } else {
            def.limit = Some(lim)
        }

        if req.after.is_some() {
            def.after = req.after;
        }

        Ok(def)
    }
}

impl Default for IntervalExchangesRequest {
    fn default() -> Self {
        Self {
            interval: Some("1d".try_into().unwrap()),
            block_timestamp_gte: None,
            block_timestamp_lt: None,
            order_sender: None,
            order_sender_in: None,
            volume_base_asset: Some("USD".to_string()),
            fees_base_asset: Some("USD".to_string()),
            limit: Some(100),
            after: Some(0),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum ExchangeAggregatesGroupBy {
    #[serde(rename = "order_sender")]
    OrderSender,
    #[serde(rename = "amount_asset")]
    AmountAsset,
    #[serde(rename = "price_asset")]
    PriceAsset,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ExchangeAggregatesRequest {
    pub order_sender: Option<String>,
    #[serde(rename = "order_sender__in")]
    pub order_sender_in: Option<Vec<String>>,
    #[serde(rename = "block_timestamp__gte")]
    pub block_timestamp_gte: Option<DateTime<Utc>>,
    #[serde(rename = "block_timestamp__lt")]
    pub block_timestamp_lt: Option<DateTime<Utc>>,
    pub volume_base_asset: Option<String>,
    pub fees_base_asset: Option<String>,
    pub group_by: Option<Vec<ExchangeAggregatesGroupBy>>,
    pub limit: Option<u32>,
    pub after: Option<u32>,
}

impl Default for ExchangeAggregatesRequest {
    fn default() -> Self {
        Self {
            block_timestamp_gte: None,
            block_timestamp_lt: None,
            order_sender: None,
            order_sender_in: None,
            volume_base_asset: Some("USD".to_string()),
            fees_base_asset: Some("USD".to_string()),
            limit: Some(100),
            after: Some(0),
            group_by: Some(vec![ExchangeAggregatesGroupBy::OrderSender]),
        }
    }
}

impl ExchangeAggregatesRequest {
    fn default_merge(req: Self) -> Result<Self, Error> {
        let mut def = Self::default();

        match req.block_timestamp_gte {
            Some(_) => def.block_timestamp_gte = req.block_timestamp_gte,
            None => return validate_error("missing required param block_timestamp__gte"),
        }

        def.block_timestamp_lt = match req.block_timestamp_lt {
            Some(d) => Some(d),
            None => Some(
                Utc::now()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap(),
            ),
        };

        match (
            def.block_timestamp_lt.as_ref(),
            def.block_timestamp_gte.as_ref(),
        ) {
            (Some(lt), Some(gte)) => {
                let diff = lt.signed_duration_since(gte.clone()).num_days();
                if diff > 33 || diff < 0 {
                    return validate_error("invalid interval in params (block_timestamp__lt - block_timestamp__gte) must be in interval beetwen 1 and 32 days");
                }
            }
            _ => unreachable!(),
        }

        let mut senders = vec![];

        if req.order_sender_in.is_some() {
            senders = req.order_sender_in.unwrap();
        }

        if req.order_sender.is_some() {
            senders.push(req.order_sender.unwrap());
            senders = senders.into_iter().unique().collect_vec();
        }

        def.order_sender_in = match senders.len() {
            0 => None,
            _ => Some(senders),
        };

        if req.volume_base_asset.is_some() {
            def.volume_base_asset = req.volume_base_asset;
        }

        if req.fees_base_asset.is_some() {
            def.fees_base_asset = req.fees_base_asset;
        }

        let lim = 100 as u32;
        if req.limit.is_some() {
            def.limit = Some(lim.min(req.limit.unwrap()));
        } else {
            def.limit = Some(lim)
        }

        if req.group_by.is_some() {
            def.group_by = req.group_by;
        }

        match def.group_by.as_ref() {
            Some(g) => {
                if g[0] != ExchangeAggregatesGroupBy::OrderSender {
                    return validate_error("unimplemented");
                }
            }
            _ => return validate_error("unimplemented"),
        }

        if req.after.is_some() {
            def.after = req.after;
        }

        Ok(def)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ExchangeAggregatesAggFields {
    order_sender: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    amount_asset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    price_asset: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExchangeAggregatesItem {
    aggregation_fields: ExchangeAggregatesAggFields,
    count: i64,
    volume: BigDecimal,
    fees: BigDecimal,
}

impl ExchangeAggregatesItem {
    pub fn empty(sender: String) -> Self {
        Self {
            aggregation_fields: ExchangeAggregatesAggFields {
                order_sender: sender,
                amount_asset: None,
                price_asset: None,
            },
            volume: BigDecimal::zero(),
            fees: BigDecimal::zero(),
            count: 0,
        }
    }
}

fn validate_error(err: &str) -> Result<ExchangeAggregatesRequest, Error> {
    Err(Error::ValidationError(
        err.into(),
        Some(HashMap::from_iter(
            [("reason".to_owned(), err.to_owned())].into_iter(),
        )),
    )
    .into())
}

#[derive(Clone, Debug, Deserialize)]
pub struct MatcherExchangeAggregatesRequest {
    pub interval: Option<Interval>,
    #[serde(rename = "block_timestamp__gte")]
    pub block_timestamp_gte: Option<DateTime<Utc>>,
    #[serde(rename = "block_timestamp__lt")]
    pub block_timestamp_lt: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
    pub after: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename = "matcher_exchange_aggregates")]
pub(crate) struct MatcherExchangeAggregatesItem {
    amount_asset: String,
    price_asset: String,
    interval: Interval,
    interval_start: NaiveDateTime,
    interval_end: NaiveDateTime,
    total_amount: BigDecimal,
    price_open: BigDecimal,
    price_close: BigDecimal,
    price_high: BigDecimal,
    price_low: BigDecimal,
}

impl MatcherExchangeAggregatesItem {
    pub fn empty(amount_asset: String, price_asset: String, d: NaiveDate) -> Self {
        Self {
            amount_asset,
            price_asset,
            interval: Interval::Day1,
            interval_start: d.and_hms_opt(0, 0, 0).unwrap(),
            interval_end: d.and_hms_opt(23, 59, 59).unwrap(),
            total_amount: BigDecimal::zero(),
            price_open: BigDecimal::zero(),
            price_close: BigDecimal::zero(),
            price_high: BigDecimal::zero(),
            price_low: BigDecimal::zero(),
        }
    }
}

impl MatcherExchangeAggregatesRequest {
    fn default_merge(req: Self) -> Result<Self, Error> {
        let mut def = Self::default();

        if req.interval.is_some() {
            def.interval = req.interval;
        }

        if req.block_timestamp_gte.is_some() {
            def.block_timestamp_gte = req.block_timestamp_gte;
        }

        def.block_timestamp_lt = match req.block_timestamp_lt {
            Some(d) => Some(d),
            None => Some(
                Utc::now()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap(),
            ),
        };

        let lim = 100 as u32;
        if req.limit.is_some() {
            def.limit = Some(lim.min(req.limit.unwrap()));
        } else {
            def.limit = Some(lim)
        }

        if req.after.is_some() {
            def.after = req.after;
        }

        Ok(def)
    }
}

impl Default for MatcherExchangeAggregatesRequest {
    fn default() -> Self {
        Self {
            interval: Some("1d".try_into().unwrap()),
            block_timestamp_gte: None,
            block_timestamp_lt: None,
            limit: Some(100),
            after: Some(0),
        }
    }
}
