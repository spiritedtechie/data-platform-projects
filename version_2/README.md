Start the local development platform stack:

```
docker-compose up -d
```

Check the following UI interfaces are accessible:

- http://localhost:8080 - kafka ui
- http://localhost:9001 - minio/s3




Build and run the container for the scheduled ingest job:

```
docker build -f ingest/Dockerfile -t tfl-to-kafka-job ingest

docker run --network version_2_network --rm --env-file ingest/.env tfl-to-kafka-job

# The above command can be added to crontab for scheduling
crontab -e
```

To run a spark job:

```bash
sh submit_spark_job.sh spark-app/tfl_line_status_raw_s3.py
sh submit_spark_job.sh spark-app/tfl_line_status_bronze.py
sh submit_spark_job.sh spark-app/tfl_line_status_silver.py
sh submit_spark_job.sh spark-app/tfl_line_status_gold_batch.py
```


View a Parquet file:

```
duckdb
SELECT * FROM '<file_path>'
```

View an Avro file: 

```
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
echo $JAVA_HOME  

avro-tools getschema file.avro
avro-tools tojson <file path> | head
```


Use DuckDB CLI to inspect Apache Iceberg tables:

```
docker exec -it duckdb duckdb -init /init.sql

select * from tfl_line_status_events;
```

Or use DuckDB UI:

```
S3_ENDPOINT="localhost:9000" duckdb --ui --init ./duckdb/init.sql 

# In a UI notebook 
select * from tfl_line_status_events;

```




