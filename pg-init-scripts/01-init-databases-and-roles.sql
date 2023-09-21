-- Database for operational application data
CREATE DATABASE application;
CREATE USER app_user WITH PASSWORD 'application';
GRANT ALL PRIVILEGES ON DATABASE application TO app_user;
\c application postgres;
GRANT ALL ON SCHEMA public TO app_user;

-- Postgres data warehouse
CREATE DATABASE warehouse;
CREATE USER warehouse_user WITH PASSWORD 'warehouse';
GRANT ALL PRIVILEGES ON DATABASE warehouse TO warehouse_user;
\c warehouse postgres;
GRANT ALL ON SCHEMA public TO warehouse_user;

-- Used for the metabase service
CREATE DATABASE metabase;
CREATE USER metabase_user WITH PASSWORD 'metabase';
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase_user;
\c metabase postgres;
GRANT ALL ON SCHEMA public TO metabase_user;

-- Used for the iceberg catalog
CREATE DATABASE iceberg_catalog;
CREATE USER iceberg_user WITH PASSWORD 'iceberg';
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO iceberg_user;
\c iceberg_catalog postgres;
GRANT ALL ON SCHEMA public TO iceberg_user;

-- User for debezium connect to fetch CDC events
\c postgres postgres;
CREATE USER debezium SUPERUSER LOGIN PASSWORD 'debezium';
