# data-platform-projects

Exploring data platform technologies:

- Prefect
- DBT
- Soda.io
- Metabase


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


### Dockerised services

```
docker compose build
docker compose up
```

Note: Metabase takes a few minutes to start first time.

Metabase server: http://localhost:3000