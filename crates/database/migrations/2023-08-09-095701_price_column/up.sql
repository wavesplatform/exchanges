ALTER TABLE exchange_transactions
ADD COLUMN price_asset_id TEXT NOT NULL;

ALTER TABLE exchange_transactions
ADD COLUMN price BIGINT NOT NULL;
