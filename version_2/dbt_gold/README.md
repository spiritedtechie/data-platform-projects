# dbt Gold Layer

This directory contains the dbt project for building (via batch processing) Gold-layer analytics tables from curated upstream data.

## 1) dbt CLI usage

### Prerequisites

- Python `3.10`-`3.13`
- Java runtime (required by `dbt-spark` session mode)
- Dependencies from `requirements.txt`
- A dbt profile file (`profiles.yml`)

### Setup

```bash
cd version_2/dbt_gold
python -m pip install -r requirements.txt
cp profiles.yml.example profiles.yml
```

### Environment variables

Set project-specific environment variables before running dbt. Common ones are:

```bash
export DBT_TARGET_SCHEMA='gold'
export DBT_TFL_SILVER_SCHEMA='silver'
export DBT_TFL_SILVER_IDENTIFIER='tfl_line_status_events'
```

You can also set storage and Spark connection settings used by your `profiles.yml` (for example `DBT_ICEBERG_WAREHOUSE`, `DBT_S3_ENDPOINT`, and credentials).

### Common dbt commands

Run from `version_2/dbt_gold`:

```bash
dbt debug --profiles-dir .
dbt parse --profiles-dir .

dbt source freshness --profiles-dir .

dbt run --profiles-dir .
dbt test --profiles-dir .

dbt build --profiles-dir .
dbt build --full-refresh --profiles-dir .
```

### Linting

```bash
python -m sqlfluff lint models --config .sqlfluff
```

### Scoped runs (optional)

Use dbt selectors to run only what you need:

```bash
dbt build --profiles-dir . --select <selector>
dbt test --profiles-dir . --select <selector>
```

Build only `fact_line_status_change`:

```bash
dbt build --profiles-dir . --select path:models/facts/fact_line_status_change.sql
```

Equivalent model-name selector:

```bash
dbt build --profiles-dir . --select fact_line_status_change
```

### See failing rows for a test

After a failing `dbt test`, inspect the actual failing records with:

```bash
dbt show --profiles-dir . --select <test_name> --limit 50
```

Example:

```bash
dbt show --profiles-dir . --select test_fact_line_status_interval_bounds --limit 50
```

Tip: use the test name shown in the failing test output.

### Inspect model output

Preview rows from a model:

```bash
dbt show --profiles-dir . --select <model_name> --limit 50
```

Example:

```bash
dbt show --profiles-dir . --select fact_line_status_interval --limit 50
```

You can also run ad-hoc SQL against refs:

```bash
dbt show --profiles-dir . --inline "select * from {{ ref('<model_name>') }} limit 50"
```

### Visual lineage graph (DAG)

Generate and open dbt docs to see the model lineage graph:

```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8080
```

Then open `http://localhost:8080`, go to **Lineage Graph**, and search for a model (for example `fact_line_status_change`) to view upstream and downstream dependencies.

Optional: generate docs for a subset to focus the graph:

```bash
dbt docs generate --profiles-dir . --select +fact_line_status_change+
```

## 2) Makefile usage (optional wrappers)

The Makefile provides shortcuts for the dbt commands above.

```bash
cd version_2/dbt_gold
make parse
make freshness
make build
make full-build
make test
make lint
```

Optional variables:

- `DBT_PROFILES_DIR` (default: `.`)
- `PYTHON` (default: `python3`)

The Make targets also pass defaults for source env vars if not already set.

## 3) CI helper

`scripts/ci.sh` runs parse/freshness/build/lint and can send a webhook alert on failure when `ALERT_WEBHOOK_URL` is set.

```bash
cd version_2/dbt_gold
ALERT_WEBHOOK_URL='https://hooks.example' ALERT_CONTEXT='prod-nightly' ./scripts/ci.sh
```
