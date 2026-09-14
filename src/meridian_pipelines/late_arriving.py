"""Handle late-arriving dimensions: facts that reference a dimension row that
does not exist yet.

THE PROBLEM, concretely:
A transaction arrives at 09:00 for account ACCT_00099999. The account feed runs
at 10:00. For one hour, that transaction references an account the warehouse has
never heard of. What do you do with it?

FOUR STRATEGIES, and this is a real interview question:

  1. REJECT THE FACT
     Lose real money movement. Never acceptable for financial data — the
     transaction genuinely happened.

  2. NULL FOREIGN KEY
     The worst option, and the one people pick by accident. An INNER JOIN
     silently drops the row, so totals are quietly short with no error anywhere.
     Silent loss beats loud failure only in the sense that nobody notices until
     a regulator does.

  3. UNKNOWN MEMBER (key -1)
     The fact is counted, and the problem shows up as an "Unknown" bucket on
     reports. Wrong-but-visible. This is Sprint 3's ADR 0005, and it is a solid
     default.

  4. INFERRED MEMBER (what this module adds)
     Create a PLACEHOLDER dimension row from what the fact tells us, flagged as
     inferred. The fact joins to a real surrogate key. When the true dimension
     record arrives, we UPDATE the placeholder in place — so history, surrogate
     keys, and every fact already pointing at it all stay correct.

WHY INFERRED BEATS UNKNOWN HERE:
With the unknown member, every late fact collapses into one bucket, so you
cannot tell 500 late transactions for one account apart from 500 for five
hundred accounts. An inferred member preserves the identity. Crucially, because
the surrogate key is stable, back-filling the real attributes later does not
require touching a single fact row.

THE TRADE-OFF to state out loud: inferred members mean the dimension briefly
contains rows that are real keys but hollow attributes. Reports must be able to
handle a customer whose segment is 'UNKNOWN' — hence the is_inferred flag, which
lets a dashboard exclude or highlight them deliberately rather than silently
mixing them in.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger("meridian_pipelines.late_arriving")


def find_orphans(facts: pd.DataFrame, fk_column: str, known_keys: set[str]) -> pd.Series:
    """Business keys referenced by facts that have no dimension row yet."""
    if fk_column not in facts.columns:
        return pd.Series(dtype=str)
    referenced = facts[fk_column].dropna().astype(str)
    orphans = referenced[~referenced.isin(known_keys)].unique()
    return pd.Series(sorted(orphans), dtype=str)


def create_inferred_customers(conn, orphan_ids: pd.Series, effective_date) -> int:
    """Insert placeholder dim_customer rows for unknown customer ids.

    The placeholder gets:
      - the real business key (so identity is preserved)
      - a surrogate key (so facts can join properly)
      - hollow attributes marked UNKNOWN
      - is_inferred = TRUE so downstream can tell placeholders apart

    valid_from is set to the beginning of history, matching the initial-load
    reasoning from Sprint 3: a fact dated in the past must be able to find this
    row, and a placeholder created today with today's valid_from would fail the
    point-in-time join for exactly the fact that caused it to exist.
    """
    if orphan_ids.empty:
        return 0

    with conn.cursor() as cur:
        # Add the flag column if this is the first time we have needed it.
        cur.execute("""
            ALTER TABLE curated.dim_customer
            ADD COLUMN IF NOT EXISTS is_inferred BOOLEAN NOT NULL DEFAULT FALSE
        """)
        inserted = 0
        for cid in orphan_ids:
            cur.execute(
                """
                INSERT INTO curated.dim_customer (
                    customer_id, first_name, last_name, full_name,
                    segment, valid_from, valid_to, is_current, row_hash, is_inferred
                )
                SELECT %s, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN (inferred)',
                       'UNKNOWN', DATE '1900-01-01', DATE '9999-12-31', TRUE,
                       'inferred', TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM curated.dim_customer
                    WHERE customer_id = %s AND is_current
                )
                """,
                (cid, cid),
            )
            inserted += cur.rowcount
    conn.commit()

    if inserted:
        logger.warning(
            "created %d inferred customer dimension row(s) for late-arriving facts",
            inserted,
        )
    return inserted


def resolve_inferred_customers(conn, effective_date) -> int:
    """Back-fill inferred rows once the real dimension data arrives.

    THE KEY PROPERTY: we UPDATE in place rather than inserting a new version.
    The surrogate key does not change, so every fact already pointing at the
    placeholder is instantly correct — no fact rows are touched.

    Note this is deliberately an SCD TYPE 1 style overwrite even though
    dim_customer is Type 2. Filling in a placeholder is not a real-world change
    to the customer; it is us finally learning what was always true. Recording
    it as a Type 2 version would fabricate a segment change that never happened.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE curated.dim_customer d
               SET first_name   = s.first_name,
                   last_name    = s.last_name,
                   full_name    = s.first_name || ' ' || s.last_name,
                   age          = s.age::smallint,
                   annual_income= s.annual_income,
                   credit_score = s.credit_score::smallint,
                   credit_band  = CASE
                                    WHEN s.credit_score >= 740 THEN 'excellent'
                                    WHEN s.credit_score >= 670 THEN 'good'
                                    WHEN s.credit_score >= 580 THEN 'fair'
                                    ELSE 'poor' END,
                   dti          = s.dti,
                   segment      = s.segment,
                   join_date    = s.join_date,
                   home_branch_id = s.home_branch_id,
                   is_inferred  = FALSE,
                   updated_at   = now()
              FROM staging.stg_customers s
             WHERE d.customer_id = s.customer_id
               AND d.is_current
               AND d.is_inferred
        """)
        resolved = cur.rowcount
    conn.commit()

    if resolved:
        logger.info("resolved %d previously-inferred customer row(s)", resolved)
    return resolved


def count_inferred(conn) -> int:
    """How many placeholders are still unresolved?

    Worth alerting on: a handful is normal operational noise, but a number that
    grows day over day means a dimension feed is broken and nobody noticed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM curated.dim_customer
            WHERE is_current AND COALESCE(is_inferred, FALSE)
        """)
        return int(cur.fetchone()[0])
