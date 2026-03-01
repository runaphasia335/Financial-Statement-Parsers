drop table if exists bank.transactions;
CREATE TABLE IF NOT EXISTS bank.transactions (
statement_date DATE NOT NULL,
transact_date DATE NOT NULL,
amount NUMERIC(10,2) NOT NULL,
description VARCHAR(100) NULL,
type VARCHAR(6) NULL,
insert_timestamp TIMESTAMP DEFAULT NOW()
);