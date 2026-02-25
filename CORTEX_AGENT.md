# Cortex Agent Guide

Reusable context for future coding threads in this repo.

## Scope

- Repository: `data-platform-projects`
- Main analytics area: `version_2/`
- Transformation stack: `dbt` on Spark/Iceberg

## Environment

- Default working directory: `version_2/dbt_gold`
- Python env: `PYENV_VERSION=data-platform-projects`
- Command pattern: `PYENV_VERSION=data-platform-projects pyenv exec dbt <command>`

## Standard Workflow

Use this order unless the user requests otherwise:

1. `dbt parse --profiles-dir .`
2. `dbt build --profiles-dir .`
3. `dbt build --full-refresh --profiles-dir .` (production-readiness check)

## Production Gate

- Call a change "production ready" only when a clean full-refresh build passes end-to-end.
- Required outcome: `ERROR=0` (and ideally `SKIP=0`) on `dbt build --full-refresh`.

## Common Commands

- Compile one model:
  - `PYENV_VERSION=data-platform-projects pyenv exec dbt compile --profiles-dir . --select <model_name>`
- Build selected graph:
  - `PYENV_VERSION=data-platform-projects pyenv exec dbt build --profiles-dir . --select <node>+`
- Full refresh build:
  - `PYENV_VERSION=data-platform-projects pyenv exec dbt build --full-refresh --profiles-dir .`
- Show failing rows for a test:
  - `PYENV_VERSION=data-platform-projects pyenv exec dbt show --profiles-dir . --select <test_name> --limit 50`
- Show model output:
  - `PYENV_VERSION=data-platform-projects pyenv exec dbt show --profiles-dir . --select <model_name> --limit 50`

## Spark/Iceberg Runbook

- If Spark fails with `JAVA_GATEWAY_EXITED` or local bind errors, rerun outside sandbox / with elevated permissions.
- Iceberg adapter caveat: avoid `on_schema_change='sync_all_columns'` in places where drop-column sync may be triggered.
- For incremental marts with declared `unique_key`, add explicit singular tests for unique grain.

## Failure Triage

When build fails, report:

1. Failing node path and node name.
2. Exact error text.
3. Which downstream nodes were skipped.
4. Smallest safe fix to unblock.

## Warning Interpretation

- `version-hint.text` warnings in `s3a://lake/.../metadata/` are common during first table creation and usually non-blocking.
- Spark `WindowExec` warnings about no partition are performance warnings unless tests fail or runtime is unacceptable.

## Design Principles

- Keep shared guidance general, not model-specific.
- Keep transformations/tests explicit and deterministic.
- Prefer backward-compatible changes unless user asks for schema-breaking behavior.
- If output schema changes are intentional, update downstream refs and tests in the same change set.

## Change Checklist

Before finishing:

1. Update downstream refs if output columns changed.
2. Run targeted validation: `dbt build --profiles-dir . --select <changed_node>+`.
3. Run full-refresh when claiming readiness.
4. If tests fail, use `dbt show` to inspect failing rows and include root-cause summary.
