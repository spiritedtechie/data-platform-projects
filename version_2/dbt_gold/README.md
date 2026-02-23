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
