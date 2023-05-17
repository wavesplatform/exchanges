use crate::error::Error;
use database::db::PgPool;
use database::schema::exchange_transactions_grouped;

use diesel::{
    dsl::*,
    prelude::*,
    sql_types::{BigInt, Numeric},
};

use super::{ExchangeAggregatesRequest, ExchangesAggregateDbRow};

pub(crate) trait Repo {
    fn exchanges_aggregates(
        &self,
        req: &ExchangeAggregatesRequest,
    ) -> Result<Vec<ExchangesAggregateDbRow>, Error>;
}

pub struct PgRepo {
    pg_pool: PgPool,
}

pub fn new(pg_pool: PgPool) -> PgRepo {
    PgRepo { pg_pool }
}

impl Repo for PgRepo {
    fn exchanges_aggregates(
        &self,
        req: &ExchangeAggregatesRequest,
    ) -> Result<Vec<ExchangesAggregateDbRow>, Error> {
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

        /*
                if req.volume_base_asset.is_some() {
                    query = query.filter(
                        exchange_transactions_grouped::amount_asset_id
                            .eq(req.volume_base_asset.as_ref().unwrap()),
                    );
                }

                if req.fees_base_asset.is_some() {
                    query = query.filter(
                        exchange_transactions_grouped::amount_asset_id
                            .eq(req.fees_base_asset.as_ref().unwrap()),
                    );
                }
        */

        // let query = query.order_by(exchange_transactions_grouped::sum_date.desc());

        let rows = query
            .get_results::<ExchangesAggregateDbRow>(&mut self.pg_pool.get()?)
            .map_err(|err| Error::DbError(err))?;

        Ok(rows)
    }
}
