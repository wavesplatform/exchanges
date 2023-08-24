-- Need to clear the database before adding Buy/Sell column
TRUNCATE TABLE blocks_microblocks CASCADE;
TRUNCATE TABLE exchange_transactions CASCADE;

-- Buy/Sell column: +1 = Buy, -1 = Sell
ALTER TABLE exchange_transactions
ADD COLUMN buy_sell INTEGER NOT NULL;
