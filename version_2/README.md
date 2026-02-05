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