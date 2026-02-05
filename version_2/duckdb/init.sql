INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

SET GLOBAL memory_limit='256MB';

SET GLOBAL s3_region='us-east-1';
SET GLOBAL s3_endpoint=getenv('S3_ENDPOINT');;
SET GLOBAL s3_use_ssl=false;
SET GLOBAL s3_url_style='path';

SET GLOBAL s3_access_key_id='minioadmin';
SET GLOBAL s3_secret_access_key='minioadmin123';


-- Convenience views
CREATE OR REPLACE VIEW raw_tfl_line_status AS
SELECT *
FROM iceberg_scan(
  's3://lake/warehouse/bronze/tfl_raw_line_status/metadata/v11.metadata.json'
);

CREATE OR REPLACE VIEW tfl_line_status_events AS
SELECT *
FROM iceberg_scan(
  's3://lake/warehouse/silver/tfl_line_status_events/metadata/v6.metadata.json'
);

CREATE OR REPLACE VIEW tfl_disruption_events AS
SELECT *
FROM iceberg_scan(
  's3://lake/warehouse/silver/tfl_disruption_events/metadata/v6.metadata.json'
);