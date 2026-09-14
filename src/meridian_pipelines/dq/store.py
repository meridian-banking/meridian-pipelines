"""Persist data-quality results, quarantined rows, and reconciliation records.

SEPARATION OF CONCERNS: checks produce results (pure, testable, no I/O); this
module writes them down. That split is why the check functions can be tested
with no database at all, and why the same results could be sent somewhere else
entirely — a metrics system, a Slack alert — without touching check logic.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_batch

from .types import CheckSuiteResult

logger = logging.getLogger("meridian_pipelines.dq.store")


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


def connect(cfg: DbConfig):
    return psycopg2.connect(cfg.dsn())


def save_results(conn, suite: CheckSuiteResult, run_id: str, load_id: int | None = None) -> int:
    """Write every check result from a suite."""
    rows = [
        (
            run_id,
            load_id,
            r.dataset,
            r.check_name,
            r.check_type.value,
            r.severity.value,
            r.column,
            r.passed,
            r.rows_checked,
            r.rows_failed,
            round(r.failure_rate, 6),
            r.message,
            json.dumps(r.sample_failures),
        )
        for r in suite.results
    ]
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO dq.check_results (
                run_id, load_id, dataset, check_name, check_type, severity,
                column_name, passed, rows_checked, rows_failed, failure_rate,
                message, sample_failures
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    conn.commit()
    logger.info("saved %d check results for %s", len(rows), suite.dataset)
    return len(rows)


def quarantine_rows(
    conn,
    df: pd.DataFrame,
    dataset: str,
    run_id: str,
    reason_codes: dict[int, list[str]] | None = None,
    business_key_column: str | None = None,
) -> int:
    """Store rejected rows as JSON with their reason codes.

    Rows go in as JSONB so that malformed data — the very thing quarantine
    exists to capture — can still be stored. A typed table would reject it.
    """
    if df.empty:
        return 0

    reason_codes = reason_codes or {}
    records = []
    for idx, row in df.iterrows():
        payload = {
            k: (None if pd.isna(v) else (v.isoformat() if hasattr(v, "isoformat") else v))
            for k, v in row.items()
        }
        # numpy scalars are not JSON-serialisable; coerce to plain Python.
        payload = {k: (v.item() if hasattr(v, "item") else v) for k, v in payload.items()}
        has_key = business_key_column and business_key_column in df.columns
        key = str(row[business_key_column]) if has_key else None
        reasons = ",".join(reason_codes.get(idx, ["unspecified"]))
        records.append((run_id, dataset, key, reasons, Json(payload)))

    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO dq.quarantine (run_id, dataset, business_key, reason_codes, row_data)
            VALUES (%s,%s,%s,%s,%s)
            """,
            records,
        )
    conn.commit()
    logger.warning("quarantined %d rows from %s", len(records), dataset)
    return len(records)


def save_reconciliation(
    conn,
    run_id: str,
    dataset: str,
    recon_date: date,
    stage_from: str,
    stage_to: str,
    rows_from: int,
    rows_to: int,
    amount_from: float | None = None,
    amount_to: float | None = None,
    explanation: str | None = None,
    quarantined: int = 0,
) -> bool:
    """Record a reconciliation between two pipeline stages.

    RETURNS whether it balanced, so the caller can act on a break.

    `quarantined` matters: rows removed by data-quality rules are a LEGITIMATE,
    explained reduction, so the expected target count is (source - quarantined),
    not source. Omitting this was a real bug during development — the report and
    the stored record disagreed about whether the same run balanced, which is
    exactly how a control loses credibility. One definition of "balanced", used
    everywhere.

    A note on comparing money: we compare rounded decimals, not floats. Floating
    point cannot represent 0.1 exactly, so summing millions of float amounts
    accumulates error and two mathematically identical totals can differ by
    cents. In finance that is unacceptable — hence NUMERIC in the schema and
    explicit rounding here.
    """
    expected_rows = rows_from - quarantined
    rows_balanced = rows_to == expected_rows
    # Amounts are only expected to tie when nothing was removed: a quarantined
    # row legitimately takes its amount with it.
    if quarantined == 0 and amount_from is not None and amount_to is not None:
        amounts_balanced = round(float(amount_from), 2) == round(float(amount_to), 2)
    else:
        amounts_balanced = True
    balanced = rows_balanced and amounts_balanced

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dq.reconciliation (
                run_id, dataset, recon_date, stage_from, stage_to,
                rows_from, rows_to, amount_from, amount_to, balanced, explanation
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                run_id,
                dataset,
                recon_date,
                stage_from,
                stage_to,
                rows_from,
                rows_to,
                amount_from,
                amount_to,
                balanced,
                explanation,
            ),
        )
    conn.commit()

    if balanced:
        logger.info(
            "reconciliation OK  %s %s->%s: %s rows", dataset, stage_from, stage_to, f"{rows_to:,}"
        )
    else:
        logger.error(
            "RECONCILIATION BREAK  %s %s->%s: expected %s rows "
            "(%s source - %s quarantined) but found %s (unexplained %s)",
            dataset,
            stage_from,
            stage_to,
            f"{expected_rows:,}",
            f"{rows_from:,}",
            f"{quarantined:,}",
            f"{rows_to:,}",
            f"{rows_to - expected_rows:+,}",
        )
    return balanced


def get_baseline_rows(conn, dataset: str, lookback: int = 7) -> float | None:
    """Trailing average row count for volume-anomaly detection.

    We use a trailing AVERAGE rather than a fixed threshold because real volumes
    drift — business grows, seasonality bites, a marketing campaign lands. A
    hard-coded number is either too loose to catch anything or too tight to
    survive a busy Monday.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT avg(rows_checked)::float
            FROM (
                SELECT rows_checked
                FROM dq.check_results
                WHERE dataset = %s AND check_type = 'volume' AND passed
                ORDER BY executed_at DESC
                LIMIT %s
            ) recent
            """,
            (dataset, lookback),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] is not None else None
