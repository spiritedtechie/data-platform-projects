# data-platform-projects

Exploring data platform technologies:

- Prefect (Batch Orchestration)
- DBT (Batch Transform)
- Soda.io (Data quality checks)
- Metabase (BI)
- Debezium (Log-based CDC)
- Kafka (Event stream storage)
- Apache Iceberg (Open table format over file formats e.g. Parquet, Avro)
- Minio (S3-compatible blob storage service)


## Running things locally
```
pip install -r requirements.txt
```

### Prefect
Start Prefect local server
```
prefect server start
```
Prefect server: http://localhost:4200

Create Prefect configuration blocks:
```
PYTHONPATH="." python flows/create_blocks.py
```

Deploy and run flow
```
PYTHONPATH="." python flows/flow.py
prefect deployment run 'Retail data/retail-data-deployment'
```

### Soda
Source environment and run Soda tests
```
set -a; source .env; set +a
soda test-connection -d raw -c soda/configuration.yml
soda scan -d raw -c soda/configuration.yml soda/checks/sources/raw_invoices.yml
```

### DBT
Source environment and run DBT
```
set -a; source .env; set +a
cd dbt/online_retail
dbt deps
dbt debug --profiles-dir ..
dbt run --profiles-dir ..
```

### Start Docker services

```
docker compose build
docker compose up
```

Note: Metabase takes a few minutes to start.

### New Debezium connector

Define the config in the `debezium` subdirectory. Submits the config to debezium connect,
to start the CDC sourcing and sinking.

```
./debezium/submit.sh <path to config>
```

# Service URLs
- Prefect - http://localhost:4200/dashboard
- Metabase - http://localhost:3000
- Kafka UI - http://localhost:8080
- Kafka Connect REST - http://localhost:8083/connectors/
    - Lots of APIs to inspect connectors, status, tasks: https://docs.confluent.io/platform/current/connect/references/restapi.html
- Debezium Connector UI - http://localhost:8081
- Iceberg REST catalog - http://localhost:8181
    - Table def: http://localhost:8181/v1/namespaces/iceberg/tables/application_db_public_products
    - REST spec: https://github.com/apache/iceberg/blob/master/open-api/rest-catalog-open-api.yaml
- Minio - http://localhost:9001/login