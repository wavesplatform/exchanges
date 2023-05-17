CREATE TABLE IF NOT EXISTS blocks_microblocks (
    uid BIGINT GENERATED BY DEFAULT AS IDENTITY
        CONSTRAINT blocks_microblocks_uid_key
            UNIQUE,
    id VARCHAR NOT NULL
        CONSTRAINT blocks_microblocks_pkey
            PRIMARY KEY,
    height INTEGER NOT NULL,
    time_stamp BIGINT
);

CREATE INDEX IF NOT EXISTS blocks_microblocks_id_idx
    ON blocks_microblocks (id);

CREATE INDEX IF NOT EXISTS blocks_microblocks_time_stamp_uid_idx
    ON blocks_microblocks (time_stamp DESC, uid DESC);


CREATE TABLE exchange_transactions (
    uid BIGINT GENERATED BY DEFAULT AS IDENTITY,
    block_uid BIGINT NOT NULL
        CONSTRAINT pool_users_balances_block_uid_fkey
            REFERENCES blocks_microblocks (uid)
                ON DELETE CASCADE,
    tx_date DATE,
    tx_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    amount_asset_id TEXT NOT NULL,
    amount BIGINT NOT NULL,
    fee_asset_id TEXT NOT NULL,
    fee BIGINT
);

CREATE INDEX IF NOT EXISTS exchange_transactions_block_uid_idx
    ON exchange_transactions (block_uid);

CREATE INDEX IF NOT EXISTS exchange_transactions_tx_date_idx ON exchange_transactions (tx_date);

CREATE TABLE exchange_transactions_grouped(
    sum_date DATE,
    tx_count BIGINT,
    sender TEXT NOT NULL,
    amount_asset_id TEXT NOT NULL,
    fee_asset_id TEXT NOT NULL,
    amount_sum BIGINT NOT NULL,
    fee_sum BIGINT,
    PRIMARY KEY (sum_date, amount_asset_id, fee_asset_id, sender)
);
