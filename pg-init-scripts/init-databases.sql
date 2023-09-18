CREATE DATABASE my_application;
CREATE USER app_user WITH PASSWORD 'application';
GRANT ALL PRIVILEGES ON DATABASE my_application TO app_user;
\c my_application postgres;
GRANT ALL ON SCHEMA public TO app_user;

CREATE DATABASE warehouse;
CREATE USER warehouse_user WITH PASSWORD 'warehouse';
GRANT ALL PRIVILEGES ON DATABASE warehouse TO warehouse_user;
\c warehouse postgres;
GRANT ALL ON SCHEMA public TO warehouse_user;


CREATE DATABASE metabase;
CREATE USER metabase_user WITH PASSWORD 'metabase';
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase_user;
\c metabase postgres;
GRANT ALL ON SCHEMA public TO metabase_user;

-- User for debezium connect to fetch CDC events
\c postgres postgres;
CREATE USER debezium SUPERUSER LOGIN PASSWORD 'debezium';
