CREATE TABLE exchange_transactions_daily_by_sender_and_pair(
    agg_date DATE,
    sender TEXT NOT NULL,
    amount_asset_id TEXT NOT NULL,
    price_asset_id TEXT NOT NULL,
    delta_base_vol Numeric(100, 10),
    delta_quote_vol Numeric(100, 10),
    PRIMARY KEY (agg_date, sender, amount_asset_id, price_asset_id)
);
