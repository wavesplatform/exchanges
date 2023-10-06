ALTER TABLE exchange_transactions_daily_price_aggregates
ADD COLUMN IF NOT EXISTS price_avg Numeric(100, 10);

-- Cleanup previous attempt on first/last functions
DROP AGGREGATE IF EXISTS public.first (anyelement);
DROP FUNCTION IF EXISTS public.first_agg;
DROP AGGREGATE IF EXISTS public.last (anyelement);
DROP FUNCTION IF EXISTS public.last_agg;
