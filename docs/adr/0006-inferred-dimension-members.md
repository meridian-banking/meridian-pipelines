# ADR 0006: Inferred dimension members for late-arriving facts

## Status
Accepted — 2026-07

## Context
Facts sometimes reference a dimension record that has not loaded yet. A
transaction arrives at 09:00 for an account whose feed runs at 10:00; for one
hour that transaction references an account the warehouse has never seen.

ADR 0005 (Sprint 3) established the **unknown member** as the fallback: point
the fact at a designated key -1 row so it is counted rather than dropped. That
remains correct as a last resort. This ADR adds a better option for the specific
case where the fact tells us the business key.

## Decision
When a fact references a missing dimension row and carries the business key,
create an **inferred placeholder** dimension row: real business key, real
surrogate key, hollow attributes, flagged `is_inferred = TRUE`. When the true
dimension record arrives, UPDATE the placeholder in place.

## Rationale
The unknown member collapses every late fact into a single bucket, so 500 late
transactions for one account are indistinguishable from 500 for five hundred
accounts. An inferred member preserves identity.

Critically, because the surrogate key is stable, back-filling real attributes
later is a metadata update — **no fact rows are touched**. Verified in testing:
surrogate key 5 before resolution, surrogate key 5 after.

Resolution is a Type 1 style overwrite even though `dim_customer` is Type 2.
Filling in a placeholder is not a real-world change to the customer; it is us
finally learning what was always true. Recording it as a Type 2 version would
fabricate a segment change that never happened.

## Consequences
+ Late facts join to a real, specific dimension row rather than a shared bucket.
+ Resolution touches only the dimension, never the facts.
+ `count_inferred()` is a monitoring signal: a handful is normal operational
  noise, a number growing day over day means a dimension feed is broken.
- The dimension briefly contains rows with real keys but hollow attributes.
  Reports must handle a customer whose segment is 'UNKNOWN' — hence the
  `is_inferred` flag, so a dashboard can exclude or highlight them deliberately
  rather than silently mixing them into real segments.
- Requires the fact to carry the business key. Where it does not, the unknown
  member from ADR 0005 remains the fallback.
