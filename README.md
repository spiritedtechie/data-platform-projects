# data-platform-projects

Exploring data platform technologies:

- Prefect (Batch Orchestration)
- DBT (Batch Transform)
- Soda.io (Data quality checks)
- Metabase (BI)
- Debezium (Log-based CDC)
- Kafka (Event stream storage)


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

Define a new config in the `debezium` subdirectory,

```
./debezium/submit.sh <path to config>
```

# Service URLs
- Prefect - http://localhost:4200/dashboard
- Metabase - http://localhost:3000
- Kafka UI - http://localhost:8080
- Debezium Connector UI - http://localhost:8081
