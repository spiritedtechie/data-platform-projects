# data-platform-projects

Exploring data platform technologies.

## Commands
```
pip install -r requirements.txt
```

Start Prefect local server
```
prefect server start
```

Start a local Postgres
```
docker build . -t data-platform-postgres
docker run -d -p 5432:5432 data-platform-postgres
```

Create Prefect configuration blocks:
```
PYTHONPATH="." python flows/create_blocks.py
```

Deploy and run flow
```
PYTHONPATH="." python flows/flow.py
prefect deployment run 'Retail data/retail-data-deployment'
```

Source environment and run Soda tests
```
set -a; source .env; set +a
soda test-connection -d raw -c soda/configuration.yml
soda scan -d raw -c soda/configuration.yml soda/checks/sources/raw_invoices.yml
```

Source environment and run DBT
```
set -a; source .env; set +a
cd dbt/online_retail
dbt debug --profiles-dir ..
dbt run --profiles-dir ..
```

