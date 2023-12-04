pub mod pg;

use super::InsertableExchangeTx;
use crate::consumer::PrevHandledHeight;
use anyhow::{Error, Result};
use chrono::NaiveDateTime;
use database::schema::blocks_microblocks;
use diesel::sql_types::{Int4, Timestamp};
use std::collections::HashMap;

#[derive(Clone, Debug, Queryable, QueryableByName)]
pub struct BlockHeightDate {
    #[diesel(sql_type = Int4)]
    pub height: i32,
    #[diesel(sql_type = Timestamp)]
    pub time_stamp: NaiveDateTime,
}

#[derive(Clone, Debug, Insertable, QueryableByName)]
#[diesel(table_name = blocks_microblocks)]
pub struct BlockMicroblock {
    pub id: String,
    pub time_stamp: Option<i64>,
    pub height: i32,
}

pub trait ConsumerRepo {
    type Operations: ConsumerRepoOperations;

    /// Execute some operations on a pooled connection without creating a database transaction.
    fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Operations) -> Result<R>;

    /// Execute some operations within a database transaction.
    fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Operations) -> Result<R>;
}

pub trait ConsumerRepoOperations {
    //
    // COMMON
    //

    fn get_handled_height(&mut self, depth: u32) -> Result<Option<PrevHandledHeight>>;

    fn get_first_height_in_last_day(&mut self) -> Result<Option<PrevHandledHeight>>;

    fn get_block_uid(&mut self, block_id: &str) -> Result<i64>;

    fn get_key_block_uid(&mut self) -> Result<i64>;

    fn get_total_block_id(&mut self) -> Result<Option<String>>;

    fn insert_blocks_or_microblocks(&mut self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>>;

    fn change_block_id(&mut self, block_uid: &i64, new_block_id: &str) -> Result<()>;

    fn delete_microblocks(&mut self) -> Result<()>;

    fn rollback_blocks_microblocks(&mut self, block_uid: &i64) -> Result<()>;

    fn insert_exchange_transactions(&mut self, transactions: &Vec<InsertableExchangeTx>) -> Result<()>;

    fn update_exchange_transactions_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn update_aggregates(&mut self) -> Result<()>;

    fn delete_old_exchange_transactions(&mut self) -> Result<()>;

    fn block_timestamps_by_heights(
        &mut self,
        from_height: i32,
        to_height: i32,
    ) -> Result<HashMap<i32, NaiveDateTime>, Error>;

    fn block_uids_by_timestamps(
        &mut self,
        from_timestamp: NaiveDateTime,
        to_timestamp: NaiveDateTime,
    ) -> Result<(Option<i64>, Option<i64>), Error>;
}
