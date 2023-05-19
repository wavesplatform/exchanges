pub mod repo;
pub mod server;

use crate::error::{self, Error};
use bigdecimal::{BigDecimal, Zero};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use diesel::sql_types::{Date, Int8, Numeric, Text};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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
pub(crate) struct ExchangesAggregateDbRow {
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

#[derive(Clone, Debug, Deserialize)]
pub struct ExchangeAggregatesRequest {
    pub interval: Option<Interval>,
    #[serde(rename = "block_timestamp__gte")]
    pub block_timestamp_gte: Option<DateTime<Utc>>,
    #[serde(rename = "block_timestamp__lt")]
    pub block_timestamp_lt: Option<DateTime<Utc>>,
    pub order_sender: Option<String>,
    pub order_sender_in: Option<Vec<String>>,
    pub volume_base_asset: Option<String>,
    pub fees_base_asset: Option<String>,
    pub limit: Option<u32>,
    pub after: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename = "interval_exchange")]
pub(crate) struct ExchangesAggregate {
    uid: i64,
    interval: Interval,
    interval_start: NaiveDateTime,
    interval_end: NaiveDateTime,
    volume: BigDecimal,
    fees: BigDecimal,
    count: i64,
}

impl ExchangesAggregate {
    pub fn empty(d: NaiveDate) -> Self {
        Self {
            uid: 0,
            interval: Interval::Day1,
            interval_start: d.and_hms_opt(0, 0, 0).unwrap(),
            interval_end: d.and_hms_opt(23, 59, 59).unwrap(),
            volume: BigDecimal::zero(),
            fees: BigDecimal::zero(),
            count: 0,
        }
    }
}

impl ExchangeAggregatesRequest {
    fn default_merge(req: Self) -> Result<Self, Error> {
        let mut def = Self::default();
        if req.interval.is_some() {
            def.interval = req.interval;
        }

        if req.block_timestamp_gte.is_some() {
            def.block_timestamp_gte = req.block_timestamp_gte;
        }

        if req.block_timestamp_lt.is_some() {
            def.block_timestamp_lt = req.block_timestamp_lt;
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

impl Default for ExchangeAggregatesRequest {
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
