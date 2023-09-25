DROP TABLE exchange_transactions_daily_price_aggregates;

DROP AGGREGATE public.first (anyelement);
DROP FUNCTION public.first_agg;
DROP AGGREGATE public.last (anyelement);
DROP FUNCTION public.last_agg;
