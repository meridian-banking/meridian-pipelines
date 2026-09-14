# ADR 0007: Two severity levels, and the rule for choosing between them

## Status
Accepted — 2026-07

## Context
Every data-quality check needs an answer to "what happens when this fails?".
Options range from a single level (everything blocks) through elaborate
multi-tier schemes (INFO/LOW/MEDIUM/HIGH/CRITICAL).

## Decision
Exactly two levels:

- **ERROR** — block the load, quarantine the offending rows
- **WARN** — log and alert, let the pipeline proceed

The rule for assigning them: **ERROR when a downstream NUMBER would be wrong.
WARN when a human should look but the arithmetic still holds.**

## Rationale
Two levels map directly onto the only two behaviours that actually exist:
the pipeline either stops or it does not. Additional tiers would all collapse
into one of those two at execution time while adding classification debates.

The failure mode we are most guarding against is **alert fatigue**. Marking
everything ERROR feels safe and is not: a pipeline that halts nightly on trivia
gets bypassed, muted, or run with `--force`. A control everyone bypasses is
worse than no control, because it manufactures false confidence.

Worked examples of the rule:
- Duplicate customer_id → ERROR (multiplies rows on every join, inflating totals)
- Orphaned account_id on a transaction → ERROR (vanishes on inner join, silent loss)
- Null annual_income → WARN (reports can exclude nulls explicitly; no total breaks)
- Customer age of 200 → WARN (implausible, harms no arithmetic)
- Stale transaction feed → WARN (the rows are fine, they are just the wrong day's;
  a human must investigate but yesterday's data is still loadable)
- Transaction volume 90% below baseline → ERROR (every row is perfect and the
  dataset is still wrong; loading it would silently understate the day)

## Consequences
+ The blocking rule lives in one property (`CheckSuiteResult.should_block`), so
  it cannot drift between pipelines.
+ Severity is configured per-check in YAML, so it can be tuned without code.
- Two levels cannot express "block in production, warn in backfill". If that is
  needed, the answer is environment-specific rule files, not more severity tiers.
