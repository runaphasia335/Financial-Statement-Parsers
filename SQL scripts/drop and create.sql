-- -- SELECT rolname, rolconfig 
-- -- FROM pg_roles 
-- -- WHERE rolname = 'postgres';

-- CREATE SCHEMA IF NOT EXISTS credit_card

-- CREATE TABLE IF NOT EXISTS credit_card (
-- cc_last_four VARCHAR(4) NOT NULL, 
-- statement_date,
-- transact_date,
-- post_date,
-- description,
-- amount,
-- type
-- )

-- CREATE TABLE IF NOT EXISTS bank.transactions (
-- statement_date DATE NOT NULL,
-- transact_date DATE NOT NULL,
-- amount NUMERIC(10,2) NOT NULL,
-- description VARCHAR(100) NULL,
-- type VARCHAR(6) NULL,
-- insert_timestamp TIMESTAMP DEFAULT NOW()
-- );

DROP DATABASE finance;