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
        amount_sum -> Int8,
        fee_asset_id -> Varchar,
        fee_sum -> Numeric,
    }
}

allow_tables_to_appear_in_same_query!(
    blocks_microblocks,
    exchange_transactions,
    exchange_transactions_grouped
);
