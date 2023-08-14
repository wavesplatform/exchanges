CREATE TABLE exchange_transactions_daily_price_aggregates(
    agg_date DATE,
    amount_asset_id TEXT NOT NULL,
    price_asset_id TEXT NOT NULL,
    price_open Numeric(100, 10),
    price_close Numeric(100, 10),
    price_high Numeric(100, 10),
    price_low Numeric(100, 10),
    PRIMARY KEY (agg_date, amount_asset_id, price_asset_id)
);


-- Aggregate function FIRST() and LAST()
-- Source: https://wiki.postgresql.org/wiki/First/last_%28aggregate%29

-- Create a function that always returns the first non-NULL value:
CREATE OR REPLACE FUNCTION public.first_agg (anyelement, anyelement)
    RETURNS anyelement
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
'SELECT $1';

-- Then wrap an aggregate around it:
CREATE OR REPLACE AGGREGATE public.first (anyelement) (
    SFUNC    = public.first_agg
    , STYPE    = anyelement
    , PARALLEL = safe
);

-- Create a function that always returns the last non-NULL value:
CREATE OR REPLACE FUNCTION public.last_agg (anyelement, anyelement)
    RETURNS anyelement
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
'SELECT $2';

-- Then wrap an aggregate around it:
CREATE OR REPLACE AGGREGATE public.last (anyelement) (
    SFUNC    = public.last_agg
    , STYPE    = anyelement
    , PARALLEL = safe
);
