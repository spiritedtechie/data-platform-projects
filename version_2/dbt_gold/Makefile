DBT_PROFILES_DIR ?= .
DBT_ENV = DBT_TFL_SILVER_SCHEMA=$${DBT_TFL_SILVER_SCHEMA:-silver} DBT_TFL_SILVER_IDENTIFIER=$${DBT_TFL_SILVER_IDENTIFIER:-tfl_line_status_events}
PYTHON ?= python3

.PHONY: parse freshness build full-build test lint ci

parse:
	@$(DBT_ENV) dbt parse --profiles-dir $(DBT_PROFILES_DIR)

freshness:
	@$(DBT_ENV) dbt source freshness --profiles-dir $(DBT_PROFILES_DIR)

build:
	@$(DBT_ENV) dbt build --profiles-dir $(DBT_PROFILES_DIR)

full-build:
	@$(DBT_ENV) dbt build --full-refresh --profiles-dir $(DBT_PROFILES_DIR)

test:
	@$(DBT_ENV) dbt test --profiles-dir $(DBT_PROFILES_DIR)

lint:
	@$(PYTHON) -m sqlfluff lint models --config .sqlfluff