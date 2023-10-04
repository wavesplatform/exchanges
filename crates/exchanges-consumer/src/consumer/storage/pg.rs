use super::{BlockHeightDate, BlockMicroblock};
pub use super::{ConsumerRepo, ConsumerRepoOperations};
use crate::consumer::{InsertableExchangeTx, PrevHandledHeight};
use crate::error::Error as AppError;
use anyhow::{Error, Result};
use chrono::{NaiveDate, NaiveDateTime};
use database::db::{PgPool, PooledPgConnection};
use database::schema::{blocks_microblocks, exchange_transactions};
use diesel::dsl::sql;
use diesel::sql_types::{BigInt, Date, Nullable, Timestamp};
use diesel::{prelude::*, sql_query};
use std::collections::HashMap;

/// Consumer's repo implementation that uses Postgres database as the storage.
///
/// Can be cloned freely, no need to wrap in `Arc`.
#[derive(Clone)]
pub struct PgConsumerRepo {
    pool: PgPool,
}

impl PgConsumerRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn get_conn(&self) -> Result<PooledPgConnection> {
        Ok(self.pool.get()?)
    }
}

impl ConsumerRepo for PgConsumerRepo {
    type Operations = PooledPgConnection;

    fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(PooledPgConnection) -> Result<R>,
    {
        tokio::task::block_in_place(move || {
            let conn = self.get_conn()?;
            f(conn)
        })
    }

    fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&PooledPgConnection) -> Result<R>,
    {
        tokio::task::block_in_place(move || {
            let conn = self.get_conn()?;
            conn.transaction(|| f(&conn))
        })
    }
}

impl ConsumerRepoOperations for PooledPgConnection {
    //
    // COMMON
    //

    fn get_first_height_in_last_day(&self) -> Result<Option<PrevHandledHeight>> {
        // get first blocks_microblocks.uid in last date
        let filter_sql = format!("(select min(height) from blocks_microblocks where time_stamp > (select extract(EPOCH from date_trunc('DAY', to_timestamp(time_stamp/1000)))*1000  from blocks_microblocks where time_stamp is not null order by uid desc limit 1))");

        blocks_microblocks::table
            .select((blocks_microblocks::uid, blocks_microblocks::height))
            .filter(blocks_microblocks::height.eq(sql(&filter_sql)))
            .order(blocks_microblocks::uid.asc())
            .first(self)
            .optional()
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn get_handled_height(&self, depth: u32) -> Result<Option<PrevHandledHeight>> {
        let filter_sql = format!("(select max(height) - {} from blocks_microblocks)", depth);

        blocks_microblocks::table
            .select((blocks_microblocks::uid, blocks_microblocks::height))
            .filter(blocks_microblocks::height.eq(sql(&filter_sql)))
            .order(blocks_microblocks::uid.asc())
            .first(self)
            .optional()
            .map_err(|err| Error::new(AppError::DbError(err)))
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64> {
        blocks_microblocks::table
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(self)
            .map_err(|err| {
                let context = format!("Cannot get block_uid by block id {}: {}", block_id, err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn get_key_block_uid(&self) -> Result<i64> {
        blocks_microblocks::table
            .select(sql("max(uid)"))
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .get_result(self)
            .map_err(|err| {
                let context = format!("Cannot get key block uid: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn get_total_block_id(&self) -> Result<Option<String>> {
        blocks_microblocks::table
            .select(blocks_microblocks::id)
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(self)
            .optional()
            .map_err(|err| {
                let context = format!("Cannot get total block id: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn insert_blocks_or_microblocks(&self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>> {
        diesel::insert_into(blocks_microblocks::table)
            .values(blocks)
            .returning(blocks_microblocks::uid)
            .get_results(self)
            .map_err(|err| {
                let context = format!("Cannot insert blocks/microblocks: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()> {
        diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.eq(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot change block id: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn delete_microblocks(&self) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot delete microblocks: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()> {
        diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot rollback blocks/microblocks: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn insert_exchange_transactions(&self, transactions: &Vec<InsertableExchangeTx>) -> Result<()> {
        transactions
            .to_owned()
            .chunks(4000)
            .into_iter()
            .try_fold((), |_, chunk| {
                diesel::insert_into(exchange_transactions::table)
                    .values(chunk)
                    .execute(self)
                    .map(|_| ())
            })
            .map_err(|err| {
                let context = format!("Cannot insert exchange_transactions: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })?;

        Ok(())
    }

    fn update_exchange_transactions_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(exchange_transactions::table)
            .set((exchange_transactions::block_uid.eq(block_uid),))
            .filter(exchange_transactions::block_uid.gt(block_uid))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!(
                    "Cannot update exchange_transactions block references: {}",
                    err
                );
                Error::new(AppError::DbError(err)).context(context)
            })
    }

    fn block_timestamps_by_heights(
        &self,
        from_height: i32,
        to_height: i32,
    ) -> Result<HashMap<i32, NaiveDateTime>, Error> {
        let q = blocks_microblocks::table
            .select((
                blocks_microblocks::height,
                sql::<Timestamp>("date_trunc('DAY', to_timestamp(time_stamp /1000))"),
            ))
            .filter(blocks_microblocks::height.ge(from_height))
            .filter(blocks_microblocks::height.le(to_height))
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .order_by(blocks_microblocks::uid);

        let res: Vec<BlockHeightDate> = q
            .load::<BlockHeightDate>(self)
            .map_err(|err| Error::new(AppError::DbError(err)))?;

        let map = res
            .iter()
            .fold(HashMap::with_capacity(res.len()), |mut hm, cur| {
                hm.insert(cur.height as i32, cur.time_stamp);
                hm
            });

        Ok(map)
    }

    fn block_uids_by_timestamps(
        &self,
        from_timestamp: NaiveDateTime,
        to_timestamp: NaiveDateTime,
    ) -> Result<(Option<i64>, Option<i64>), Error> {
        let q = blocks_microblocks::table
            .select((
                sql::<Nullable<BigInt>>("min(uid) as min_uid"),
                sql::<Nullable<BigInt>>("max(uid) as max_uid"),
            ))
            .filter(
                sql::<Timestamp>("to_timestamp(blocks_microblocks.time_stamp / 1000.0)::Timestamp")
                    .le(to_timestamp),
            )
            .filter(
                sql::<Timestamp>("to_timestamp(blocks_microblocks.time_stamp / 1000.0)::Timestamp")
                    .ge(from_timestamp),
            )
            .filter(blocks_microblocks::time_stamp.is_not_null());

        let res = q
            .load::<(Option<i64>, Option<i64>)>(self)
            .map_err(|err| Error::new(AppError::DbError(err)))?;

        if res.len() == 0 {
            return Ok((None, None));
        }

        Ok(res[0])
    }

    fn update_aggregates(&self) -> Result<()> {
        let last_dates = exchange_transactions::table
            .select(exchange_transactions::tx_date)
            .order(exchange_transactions::tx_date.desc())
            .limit(1)
            .load::<NaiveDate>(self)
            .map_err(|err| Error::new(AppError::DbError(err)))?;

        if last_dates.is_empty() {
            return Ok(());
        }

        let last_date = last_dates.first().expect("empty date");

        // Update `exchange_transactions_grouped`

        let sql = r#"
            insert into exchange_transactions_grouped (sum_date, sender, amount_asset_id, fee_asset_id, amount_sum, fee_sum, tx_count)
                select tx.tx_date, tx.sender, tx.amount_asset_id, tx.fee_asset_id, sum(tx.amount) amount_sum, sum(tx.fee) fee_sum, count(*) tx_count
                    from exchange_transactions tx
                        inner join blocks_microblocks b on tx.block_uid = b.uid
                    where
                    tx.tx_date >= ($1::Date - '1 DAY'::Interval)
                    and b.time_stamp is not null
                group by 1,2,3,4

                on conflict on constraint exchange_transactions_grouped_pkey
                do update set
                    amount_sum = excluded.amount_sum,
                    fee_sum = excluded.fee_sum,
                    tx_count = excluded.tx_count
        "#;
        let q = sql_query(sql).bind::<Date, _>(&last_date);

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot save exchange_transactions_grouped: {}", err);
            Error::new(AppError::DbError(err)).context(context)
        })?;

        // Update `exchange_transactions_daily_price_aggregates`

        let sql = r#"
            insert into exchange_transactions_daily_price_aggregates (agg_date, amount_asset_id, price_asset_id, price_open, price_close, total_amount, price_high, price_low, price_avg)
            SELECT
                tx_date,
                amount_asset_id,
                price_asset_id,
                price_open,
                price_close,
                sum(amount) / 2.0 total_amount, -- Divided by 2 because each TX has 2 rows: byu side and sell side
                max(price) price_high,
                min(price) price_low,
                sum(amount::numeric * price::numeric) / sum(amount::numeric) price_avg
            FROM (
                    select
                        tx.tx_date, tx.amount_asset_id, tx.price_asset_id, tx.amount, tx.price,
                        first_value(tx.price) OVER (PARTITION BY tx.tx_date, tx.amount_asset_id, tx.price_asset_id ORDER BY tx.uid ASC) price_open,
                        first_value(tx.price) OVER (PARTITION BY tx.tx_date, tx.amount_asset_id, tx.price_asset_id ORDER BY tx.uid DESC) price_close
                    from exchange_transactions tx
                             inner join blocks_microblocks b on tx.block_uid = b.uid
                    where
                        tx.tx_date >= ($1::Date - '1 DAY'::Interval)
                        and b.time_stamp is not null
                 ) x
            group by 1,2,3,4,5

            on conflict on constraint exchange_transactions_daily_price_aggregates_pkey
                do update set
                              total_amount = excluded.total_amount,
                              price_open = excluded.price_open,
                              price_close = excluded.price_close,
                              price_high = excluded.price_high,
                              price_low = excluded.price_low,
                              price_avg = excluded.price_avg
        "#;
        let q = sql_query(sql).bind::<Date, _>(&last_date);

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot save exchange_transactions_daily_price_aggregates: {}", err);
            Error::new(AppError::DbError(err)).context(context)
        })?;

        // Update `exchange_transactions_daily_by_sender_and_pair`

        let sql = r#"
            insert into exchange_transactions_daily_by_sender_and_pair (agg_date, sender, amount_asset_id, price_asset_id, delta_base_vol, delta_quote_vol)
            select tx.tx_date, tx.sender, tx.amount_asset_id, tx.price_asset_id,
                   sum(tx.amount::NUMERIC * tx.buy_sell) delta_base_vol,
                   sum(tx.price::NUMERIC * tx.amount::NUMERIC * (-tx.buy_sell)) delta_quote_vol
            from exchange_transactions tx
                     inner join blocks_microblocks b on tx.block_uid = b.uid
            where
                  tx.tx_date >= ($1::Date - '1 DAY'::Interval)
              and b.time_stamp is not null
            group by 1,2,3,4

            on conflict on constraint exchange_transactions_daily_by_sender_and_pair_pkey
                do update set
                              delta_base_vol = excluded.delta_base_vol,
                              delta_quote_vol = excluded.delta_quote_vol
        "#;
        let q = sql_query(sql).bind::<Date, _>(&last_date);

        q.execute(self).map(|_| ()).map_err(|err| {
            let context = format!("Cannot save exchange_transactions_daily_by_sender_and_pair: {}", err);
            Error::new(AppError::DbError(err)).context(context)
        })?;

        Ok(())
    }

    fn delete_old_exchange_transactions(&self) -> Result<()> {
        let old_dates = exchange_transactions::table
            .select(sql::<Date>("(tx_date - '2 DAY'::interval)::Date"))
            .order(exchange_transactions::tx_date.desc())
            .limit(1)
            .load::<NaiveDate>(self)
            .map_err(|err| Error::new(AppError::DbError(err)))?;

        if old_dates.is_empty() {
            return Ok(());
        }

        let old_date = old_dates.first().expect("invalid old date");

        diesel::delete(exchange_transactions::table)
            .filter(exchange_transactions::tx_date.lt(old_date))
            .execute(self)
            .map(|_| ())
            .map_err(|err| {
                let context = format!("Cannot delete old exchange_transactions: {}", err);
                Error::new(AppError::DbError(err)).context(context)
            })
    }
}
