# meridian-pipelines

ETL transforms and the **data quality framework** for the Meridian platform. This is the repo that makes the platform *trustworthy* rather than merely functional: configurable checks with severity levels, quarantine for rejected rows, source-to-warehouse reconciliation, and late-arriving dimension handling.

Sits between `meridian-ingestion` (which lands data in the lake) and `meridian-warehouse` (which models it).

## The data quality gate

```
   source data
        │
        ▼
   ┌─────────────────┐
   │  run checks     │  six check types, ERROR or WARN severity
   └────────┬────────┘
            │
     ┌──────┴──────┐
     │             │
  all pass     any ERROR
     │             │
     ▼             ▼
  proceed    quarantine bad rows  →  exit code 1
             clean rows still flow    (Airflow stops downstream tasks)
```

**Exit code is the contract.** `python -m meridian_pipelines check` returns 0 to proceed and 1 to block. That's what turns a quality report into a real gate in Sprint 6 — the orchestrator refuses to run downstream tasks on a non-zero exit.

## The six check types

| Type | Catches |
|---|---|
| **completeness** | Required fields that are null |
| **uniqueness** | Duplicate keys (which silently multiply rows on every join) |
| **validity** | Values out of range, outside an allowed set, or failing a pattern |
| **referential** | Foreign keys pointing at rows that don't exist |
| **freshness** | Data that's structurally perfect but stale |
| **volume** | A dataset where every row is fine but 90% are missing |

That last one is the subtle one: no row-level check can detect a silently truncated export, because every surviving row is immaculate. Only comparing the count against a trailing baseline notices.

## Severity: ERROR vs WARN

- **ERROR** — a downstream *number* would be wrong. Block the load, quarantine the rows.
- **WARN** — a human should look, but the arithmetic still holds. Log and proceed.

The temptation is to mark everything ERROR "to be safe." Resist it: a pipeline that halts nightly on trivia gets bypassed, and a control everyone bypasses is worse than no control because it manufactures false confidence.

## Quarantine, not rejection

A naive pipeline has two bad options when data is dirty: reject the whole batch (losing thousands of good rows over a handful of bad ones) or load everything (corrupting the warehouse). Quarantine is the third option — good rows flow, bad rows are isolated in `dq.quarantine` as JSONB with reason codes, and someone can fix and replay them.

Rows are stored as JSONB deliberately: quarantine must accept malformed data, which is precisely what a typed table would reject.

## Reconciliation

The control that makes a bank trust a warehouse. Compare row counts and amount totals at each hop and require every difference to be *explained*:

```
  source      300,000
- quarantined      12
= expected    299,988
  warehouse   299,988   →  balanced
```

A difference isn't automatically a bug — quarantined rows legitimately reduce the count. But an *unexplained* break of even one row means you don't know what your pipeline did, and "we lost a transaction somewhere" is not an acceptable sentence in banking.

## Late-arriving dimensions

When a fact references a dimension row that doesn't exist yet, this repo creates an **inferred placeholder** with a real surrogate key, flagged `is_inferred`. When the true record arrives, the placeholder is updated *in place* — the surrogate key never changes, so every fact already pointing at it becomes correct without touching a single fact row.

See [`docs/adr/0006-inferred-dimension-members.md`](docs/adr/0006-inferred-dimension-members.md) for why this beats the unknown-member fallback here.

## Quick start

```bash
pip install -e ".[dev]"

export WAREHOUSE_HOST=localhost
export WAREHOUSE_PORT=5432
export WAREHOUSE_DB=meridian
export WAREHOUSE_USER=meridian
export WAREHOUSE_PASSWORD=<your warehouse password>

python -m meridian_pipelines migrate
python -m meridian_pipelines check --source-dir ../meridian-data-generator/output/parquet
python -m meridian_pipelines reconcile --source-dir ../meridian-data-generator/output/parquet
python -m meridian_pipelines quarantine-report
```

## Rules are data, not code

Checks live in [`config/dq_rules.yaml`](config/dq_rules.yaml), not Python. The person who knows that `credit_score` must be 300–850, or that a missing `account_id` is a crisis while a missing middle name isn't, is often an analyst rather than a developer. Expressing rules as data means they're reviewable in a PR by someone who doesn't read Python, and adding one doesn't require a deploy.

CI validates the rule file structure, so a malformed rule fails in a pull request rather than at 3am.

## Development

```bash
make test    # 28 tests, no database required
make lint
make check   # run the gate against generator output
```

Checks are pure functions over DataFrames — that separation between "produce a result" and "write it down" is why the test suite needs no infrastructure at all.

Part of the 8-repository Meridian platform.


_Verified locally: the quality gate blocked corrupted data (exit code 1), quarantined 4 defective rows across customers and transactions with full diagnostic payloads, while 3.15M+ clean transactions still proceeded._
