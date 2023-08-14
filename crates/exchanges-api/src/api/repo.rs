use super::{
    ExchangeAggregateDbRow, ExchangeAggregatesRequest, IntervalExchangeDbRow,
    IntervalExchangesRequest, MatcherExchangeAggregatesRequest, MatcherExchangeDbRow,
};
use crate::api::Interval;
use crate::error::Error;
use database::db::PgPool;
use database::schema::{
    exchange_transactions_daily_price_aggregates, exchange_transactions_grouped,
};
use diesel::{
    dsl::*,
    prelude::*,
    sql_types::{BigInt, Numeric},
};

pub(crate) trait Repo {
    fn interval_exchanges(
        &self,
        req: &IntervalExchangesRequest,
    ) -> Result<Vec<IntervalExchangeDbRow>, Error>;

    fn exchange_aggregates(
        &self,
        req: &ExchangeAggregatesRequest,
    ) -> Result<Vec<ExchangeAggregateDbRow>, Error>;

    fn matcher_exchange_aggregates(
        &self,
        req: &MatcherExchangeAggregatesRequest,
    ) -> Result<Vec<MatcherExchangeDbRow>, Error>;
}

pub struct PgRepo {
    pg_pool: PgPool,
}

pub fn new(pg_pool: PgPool) -> PgRepo {
    PgRepo { pg_pool }
}

impl Repo for PgRepo {
    fn interval_exchanges(
        &self,
        req: &IntervalExchangesRequest,
    ) -> Result<Vec<IntervalExchangeDbRow>, Error> {
        let mut query = exchange_transactions_grouped::table
            .select((
                exchange_transactions_grouped::sum_date,
                // exchange_transactions_grouped::sender,
                exchange_transactions_grouped::amount_asset_id,
                exchange_transactions_grouped::fee_asset_id,
                sql::<Numeric>("sum(amount_sum) as amount_sum"),
                sql::<Numeric>("sum(fee_sum) as fee_sum"),
                sql::<BigInt>("sum(tx_count)::BIGINT as count"),
            ))
            .group_by((
                exchange_transactions_grouped::sum_date,
                // exchange_transactions_grouped::sender,
                exchange_transactions_grouped::amount_asset_id,
                exchange_transactions_grouped::fee_asset_id,
            ))
            .into_boxed();

        if req.block_timestamp_gte.is_some() {
            query = query.filter(
                exchange_transactions_grouped::sum_date.ge(req
                    .block_timestamp_gte
                    .unwrap()
                    .naive_utc()
                    .date()),
            );
        }

        if req.block_timestamp_lt.is_some() {
            query = query.filter(
                exchange_transactions_grouped::sum_date.le(req
                    .block_timestamp_lt
                    .unwrap()
                    .naive_utc()
                    .date()),
            );
        }

        if req.order_sender_in.is_some() {
            query = query.filter(
                exchange_transactions_grouped::sender.eq_any(req.order_sender_in.as_ref().unwrap()),
            );
        }

        let rows = query
            .get_results::<IntervalExchangeDbRow>(&mut self.pg_pool.get()?)
            .map_err(|err| Error::DbError(err))?;

        Ok(rows)
    }

    fn exchange_aggregates(
        &self,
        req: &ExchangeAggregatesRequest,
    ) -> Result<Vec<ExchangeAggregateDbRow>, Error> {
        let mut query = exchange_transactions_grouped::table
            .select((
                exchange_transactions_grouped::sender,
                exchange_transactions_grouped::amount_asset_id,
                exchange_transactions_grouped::fee_asset_id,
                sql::<Numeric>("sum(amount_sum) as amount_sum"),
                sql::<Numeric>("sum(fee_sum) as fee_sum"),
                sql::<BigInt>("sum(tx_count)::BIGINT as count"),
            ))
            .group_by((
                exchange_transactions_grouped::sender,
                exchange_transactions_grouped::amount_asset_id,
                exchange_transactions_grouped::fee_asset_id,
            ))
            .order(exchange_transactions_grouped::sender)
            .into_boxed();

        if req.block_timestamp_gte.is_some() {
            query = query.filter(
                exchange_transactions_grouped::sum_date.ge(req
                    .block_timestamp_gte
                    .unwrap()
                    .naive_utc()
                    .date()),
            );
        }

        if req.block_timestamp_lt.is_some() {
            query = query.filter(
                exchange_transactions_grouped::sum_date.le(req
                    .block_timestamp_lt
                    .unwrap()
                    .naive_utc()
                    .date()),
            );
        }

        if req.order_sender_in.is_some() {
            query = query.filter(
                exchange_transactions_grouped::sender.eq_any(req.order_sender_in.as_ref().unwrap()),
            );
        }

        let rows = query
            .get_results::<ExchangeAggregateDbRow>(&mut self.pg_pool.get()?)
            .map_err(|err| Error::DbError(err))?;

        Ok(rows)
    }

    fn matcher_exchange_aggregates(
        &self,
        req: &MatcherExchangeAggregatesRequest,
    ) -> Result<Vec<MatcherExchangeDbRow>, Error> {
        let interval = req.interval.unwrap_or(Interval::Day1);
        assert_eq!(interval, Interval::Day1, "Unsupported interval");

        let mut query = exchange_transactions_daily_price_aggregates::table
            .select((
                exchange_transactions_daily_price_aggregates::agg_date,
                exchange_transactions_daily_price_aggregates::amount_asset_id,
                exchange_transactions_daily_price_aggregates::price_asset_id,
                exchange_transactions_daily_price_aggregates::price_open,
                exchange_transactions_daily_price_aggregates::price_close,
                exchange_transactions_daily_price_aggregates::price_high,
                exchange_transactions_daily_price_aggregates::price_low,
            ))
            .into_boxed();

        if req.block_timestamp_gte.is_some() {
            query = query.filter(
                exchange_transactions_daily_price_aggregates::agg_date.ge(req
                    .block_timestamp_gte
                    .unwrap()
                    .naive_utc()
                    .date()),
            );
        }

        if req.block_timestamp_lt.is_some() {
            query = query.filter(
                exchange_transactions_daily_price_aggregates::agg_date.le(req
                    .block_timestamp_lt
                    .unwrap()
                    .naive_utc()
                    .date()),
            );
        }

        let rows = query
            .get_results::<MatcherExchangeDbRow>(&mut self.pg_pool.get()?)
            .map_err(|err| Error::DbError(err))?;

        Ok(rows)
    }
}
