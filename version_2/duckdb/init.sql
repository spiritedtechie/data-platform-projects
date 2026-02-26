INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

SET GLOBAL memory_limit='512MB';

SET GLOBAL s3_region='us-east-1';
SET GLOBAL s3_endpoint=getenv('S3_ENDPOINT');
SET GLOBAL s3_use_ssl=false;
SET GLOBAL s3_url_style='path';

SET GLOBAL s3_access_key_id='minioadmin';
SET GLOBAL s3_secret_access_key='minioadmin123';

SET GLOBAL unsafe_enable_version_guessing=true;

-- Bronze / Silver
CREATE OR REPLACE VIEW bronze_tfl_line_status AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/bronze/tfl_line_status');

CREATE OR REPLACE VIEW silver_tfl_line_status_events AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/silver/tfl_line_status_events');

CREATE OR REPLACE VIEW tfl_quarantine AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/quarantine/tfl_bad_records');

CREATE OR REPLACE VIEW dq_metrics AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/ops/dq_metrics');

-- Gold dimensions
CREATE OR REPLACE VIEW gold_dim_line AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/dim_line');

CREATE OR REPLACE VIEW gold_dim_status AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/dim_status');

CREATE OR REPLACE VIEW gold_dim_datetime AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/dim_datetime');

-- Gold facts
CREATE OR REPLACE VIEW gold_fact_line_status_interval AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/fact_line_status_interval');

CREATE OR REPLACE VIEW gold_fact_line_status_change AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/fact_line_status_change');

-- Gold marts
CREATE OR REPLACE VIEW gold_mart_line_hour AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/mart_line_hour');

CREATE OR REPLACE VIEW gold_mart_line_day AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/mart_line_day');

CREATE OR REPLACE VIEW gold_mart_network_hour AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/mart_network_hour');

CREATE OR REPLACE VIEW gold_mart_network_day AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/mart_network_day');

CREATE OR REPLACE VIEW gold_mart_line_incident AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/mart_line_incident');

CREATE OR REPLACE VIEW gold_mart_disruption_category_day AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/mart_disruption_category_day');

CREATE OR REPLACE VIEW gold_current_line_status AS
SELECT *
FROM iceberg_scan('s3://lake/warehouse/gold/current_line_status');
