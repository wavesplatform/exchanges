use crate::error::Error;
use bigdecimal::BigDecimal;
use chrono::NaiveDate;
use database::db::PgPool;
use database::schema::exchange_transactions_grouped;

use diesel::{
    dsl::*,
    prelude::*,
    sql_types::{BigInt, Date, Int8, Numeric, Text},
};

use super::ExchangeAggregatesRequest;

pub(crate) trait Repo {
    fn exchanges_aggregates(
        &self,
        req: &ExchangeAggregatesRequest,
    ) -> Result<Vec<ExchangesAggregateDbRow>, Error>;
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
    pub amount_volume_sum: BigDecimal,
    #[sql_type = "Numeric"]
    pub fee_volume_sum: BigDecimal,
    #[sql_type = "Int8"]
    pub count: i64,
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
                sql::<Numeric>("sum(amount_volume_sum) as amount_volume_sum"),
                sql::<Numeric>("sum(fee_volume_sum) as fee_volume_sum"),
                sql::<BigInt>("count(*) as count"),
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
