table! {
    blocks_microblocks (id) {
        uid -> Int8,
        id -> Varchar,
        height -> Int4,
        time_stamp -> Nullable<Int8>,
    }
}

table! {
     exchange_transactions(uid) {
        uid -> Int8,
        block_uid -> Int8,
        tx_date -> Date,
        tx_id -> Varchar,
        sender -> Varchar,
        price_asset_id -> Varchar,
        price -> Int8,
        amount_asset_id -> Varchar,
        amount -> Int8,
        order_amount -> Int8,
        fee_asset_id  -> Varchar,
        fee -> Int8,
    }
}

table! {
     exchange_transactions_grouped(sum_date) {
        sum_date -> Date,
        tx_count -> Int8,
        sender -> Varchar,
        amount_asset_id -> Varchar,
        amount_sum -> Numeric,
        fee_asset_id -> Varchar,
        fee_sum -> Numeric,
    }
}

table! {
     exchange_transactions_daily_price_aggregates(agg_date) {
        agg_date -> Date,
        amount_asset_id -> Varchar,
        price_asset_id -> Varchar,
        price_open -> Numeric,
        price_close -> Numeric,
        price_high -> Numeric,
        price_low -> Numeric,
    }
}

allow_tables_to_appear_in_same_query!(
    blocks_microblocks,
    exchange_transactions,
    exchange_transactions_grouped,
    exchange_transactions_daily_price_aggregates
);
