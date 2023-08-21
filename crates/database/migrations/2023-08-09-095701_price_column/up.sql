-- Need to clear the database before adding Price Asset ID and Price columns
TRUNCATE TABLE blocks_microblocks CASCADE;
TRUNCATE TABLE exchange_transactions CASCADE;

ALTER TABLE exchange_transactions
ADD COLUMN price_asset_id TEXT NOT NULL;

ALTER TABLE exchange_transactions
ADD COLUMN price BIGINT NOT NULL;
