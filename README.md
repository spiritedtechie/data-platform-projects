# data-platform-projects

Exploring data platform technologies.

## Commands
Source environment and run DBT
```
set -a; source .env; set +a
cd dbt/online_retail
dbt debug --profiles-dir ..
dbt run --profiles-dir ..
```
