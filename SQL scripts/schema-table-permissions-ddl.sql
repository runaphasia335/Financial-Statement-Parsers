-- -- SELECT rolname, rolconfig 
-- -- FROM pg_roles 
-- -- WHERE rolname = 'postgres';

-- CREATE SCHEMA IF NOT EXISTS Statements; 

-- CREATE TABLE IF NOT EXISTS statements.capital_one (
-- cc_last_four VARCHAR(4) NOT NULL, 
-- statement_date DATE NOT NULL,
-- transact_date DATE NOT NULL,
-- post_date DATE NOT NULL,
-- amount NUMERIC(10,2) NOT NULL,
-- description VARCHAR(100) NULL,
-- type VARCHAR(6) NULL,
-- insert_timestamp TIMESTAMP DEFAULT NOW()
-- );

-- CREATE TABLE IF NOT EXISTS statements.synovus (
-- statement_date DATE NOT NULL,
-- transact_date DATE NOT NULL,
-- amount NUMERIC(10,2) NOT NULL,
-- description VARCHAR(100) NULL,
-- type VARCHAR(6) NULL,
-- insert_timestamp TIMESTAMP DEFAULT NOW()
-- );

-- Grant SELECT on all existing tables
-- GRANT SELECT ON ALL TABLES IN SCHEMA statements TO powerbi_user;
-- GRANT USAGE ON SCHEMA statements TO powerbi_user;
-- SELECT table_schema, table_name, privilege_type
-- FROM information_schema.role_table_grants
-- WHERE grantee = 'powerbi_user'
-- ORDER BY table_schema, table_name;

