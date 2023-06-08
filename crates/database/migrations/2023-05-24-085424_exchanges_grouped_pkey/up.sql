ALTER TABLE exchange_transactions_grouped DROP CONSTRAINT exchange_transactions_grouped_pkey;
ALTER TABLE exchange_transactions_grouped ADD PRIMARY KEY (sender, sum_date, amount_asset_id, fee_asset_id);
