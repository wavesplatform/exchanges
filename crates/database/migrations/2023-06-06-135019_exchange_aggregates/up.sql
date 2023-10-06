CREATE INDEX IF NOT EXISTS "ex_tx_gr_sum_date_idx" ON exchange_transactions_grouped(sum_date) INCLUDE(sender, amount_asset_id, fee_asset_id);