Start the local development platform stack:

```bash
docker-compose up -d
```

Check interfaces:

- http://localhost:8080 - Kafka UI
- http://localhost:9001 - MinIO console

Build and run scheduled ingest job:

```bash
docker build -f ingest/Dockerfile -t tfl-to-kafka-job ingest

docker run --network version_2_network --rm --env-file ingest/.env tfl-to-kafka-job
```

## Spark pipeline execution order

```bash
sh submit_spark_job.sh spark-app/tfl_line_status_raw_s3.py
sh submit_spark_job.sh spark-app/tfl_line_status_bronze.py
sh submit_spark_job.sh spark-app/tfl_line_status_silver.py
sh submit_spark_job.sh spark-app/tfl_line_status_gold_batch.py
```

## dbt Gold project

```bash
cd version_2/dbt_gold
python -m pip install -r requirements.txt
export DBT_TARGET_SCHEMA='gold'
export DBT_TFL_SILVER_SCHEMA='silver'
export DBT_TFL_SILVER_IDENTIFIER='tfl_line_status_events'

dbt run
dbt test
```

## DuckDB inspection

```bash
docker exec -it duckdb duckdb -init /init.sql
```

Then query views like:

```sql
select * from gold_mart_line_day limit 20;
select * from gold_current_line_status;
```
