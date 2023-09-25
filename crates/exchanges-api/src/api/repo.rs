use super::{
    error::Error, ExchangeAggregateDbRow, ExchangeAggregatesRequest, IntervalExchangeDbRow,
    IntervalExchangesRequest, MatcherExchangeAggregatesRequest, MatcherExchangeDbRow,
    PnlAggregatesRequest, PnlDbRow,
};
use database::{
    db::PgPool,
    schema::{
        exchange_transactions_daily_by_sender_and_pair,
        exchange_transactions_daily_price_aggregates, exchange_transactions_grouped,
    },
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

    fn pnl_aggregates(&self, req: &PnlAggregatesRequest) -> Result<Vec<PnlDbRow>, Error>;
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

        let date_range = req.time_range().to_date_range();

        if let Some(date) = date_range.date_from {
            query = query.filter(exchange_transactions_grouped::sum_date.ge(date));
        }

        if let Some(date) = date_range.date_to {
            query = query.filter(exchange_transactions_grouped::sum_date.le(date));
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

        let date_range = req.time_range().to_date_range();

        if let Some(date) = date_range.date_from {
            query = query.filter(exchange_transactions_grouped::sum_date.ge(date));
        }

        if let Some(date) = date_range.date_to {
            query = query.filter(exchange_transactions_grouped::sum_date.le(date));
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
        let mut query = exchange_transactions_daily_price_aggregates::table
            .select((
                exchange_transactions_daily_price_aggregates::agg_date,
                exchange_transactions_daily_price_aggregates::amount_asset_id,
                exchange_transactions_daily_price_aggregates::price_asset_id,
                exchange_transactions_daily_price_aggregates::total_amount,
                exchange_transactions_daily_price_aggregates::price_open,
                exchange_transactions_daily_price_aggregates::price_close,
                exchange_transactions_daily_price_aggregates::price_high,
                exchange_transactions_daily_price_aggregates::price_low,
                exchange_transactions_daily_price_aggregates::price_avg,
            ))
            .into_boxed();

        let date_range = req.time_range().to_date_range();

        if let Some(date) = date_range.date_from {
            query = query.filter(exchange_transactions_daily_price_aggregates::agg_date.ge(date));
        }

        if let Some(date) = date_range.date_to {
            query = query.filter(exchange_transactions_daily_price_aggregates::agg_date.le(date));
        }

        let rows = query
            .order(exchange_transactions_daily_price_aggregates::agg_date.asc())
            .get_results::<MatcherExchangeDbRow>(&mut self.pg_pool.get()?)
            .map_err(|err| Error::DbError(err))?;

        Ok(rows)
    }

    fn pnl_aggregates(&self, req: &PnlAggregatesRequest) -> Result<Vec<PnlDbRow>, Error> {
        let mut q = exchange_transactions_daily_by_sender_and_pair::table
            .select((
                exchange_transactions_daily_by_sender_and_pair::agg_date,
                exchange_transactions_daily_by_sender_and_pair::amount_asset_id,
                exchange_transactions_daily_by_sender_and_pair::price_asset_id,
                exchange_transactions_daily_by_sender_and_pair::delta_base_vol,
                exchange_transactions_daily_by_sender_and_pair::delta_quote_vol,
            ))
            .into_boxed();

        // Filter by sender
        q = q.filter(exchange_transactions_daily_by_sender_and_pair::sender.eq(&req.sender));

        let date_range = req.time_range().to_date_range();

        if let Some(date) = date_range.date_from {
            q = q.filter(exchange_transactions_daily_by_sender_and_pair::agg_date.ge(date));
        }

        if let Some(date) = date_range.date_to {
            q = q.filter(exchange_transactions_daily_by_sender_and_pair::agg_date.le(date));
        }

        let rows = q
            .get_results::<PnlDbRow>(&mut self.pg_pool.get()?)
            .map_err(|err| Error::DbError(err))?;

        Ok(rows)
    }
}
