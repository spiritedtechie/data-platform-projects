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