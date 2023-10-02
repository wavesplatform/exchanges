CREATE TABLE exchange_transactions_daily_price_aggregates(
    agg_date DATE,
    amount_asset_id TEXT NOT NULL,
    price_asset_id TEXT NOT NULL,
    total_amount Numeric(100, 10),
    price_open Numeric(100, 10),
    price_close Numeric(100, 10),
    price_high Numeric(100, 10),
    price_low Numeric(100, 10),
    PRIMARY KEY (agg_date, amount_asset_id, price_asset_id)
);
