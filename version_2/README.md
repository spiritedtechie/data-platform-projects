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