"""Reconciliation: prove the warehouse agrees with the source.

THE CONTROL THAT MAKES A BANK TRUST A WAREHOUSE.

Every hop in a pipeline is an opportunity to lose rows (a failed join, a bad
filter, a quarantine) or duplicate them (a re-run without idempotency, a fan-out
join). Reconciliation asks the blunt question at each hop:

    Same number of rows?  Same total amount?

If not, the difference must be EXPLAINED before anyone trusts the numbers. In a
real bank this runs daily, is formally signed off, and an unexplained break is
an incident with a named owner — not a backlog ticket.

WHAT COUNTS AS AN EXPLAINED BREAK:
A break is not automatically a bug. Quarantined rows legitimately reduce the
warehouse count. The discipline is that every difference is ACCOUNTED FOR:

    source 300,000
  - quarantined     12
  = expected   299,988
    warehouse   299,988   -> balanced, explained

An unexplained break of even one row means you do not know what your pipeline
did, and in banking "we lost one transaction somewhere" is not an acceptable
sentence.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd

logger = logging.getLogger("meridian_pipelines.recon")


@dataclass
class StageTotals:
    """Row count and optional amount total at one pipeline stage."""

    stage: str
    rows: int
    amount: float | None = None


@dataclass
class ReconResult:
    dataset: str
    recon_date: date
    stages: list[StageTotals]
    quarantined: int = 0

    @property
    def source(self) -> StageTotals:
        return self.stages[0]

    @property
    def final(self) -> StageTotals:
        return self.stages[-1]

    @property
    def expected_final_rows(self) -> int:
        """Source rows minus anything legitimately removed."""
        return self.source.rows - self.quarantined

    @property
    def row_break(self) -> int:
        """Unexplained row difference. Zero means fully accounted for."""
        return self.final.rows - self.expected_final_rows

    @property
    def amount_break(self) -> float | None:
        if self.source.amount is None or self.final.amount is None:
            return None
        return round(self.final.amount - self.source.amount, 2)

    @property
    def balanced(self) -> bool:
        if self.row_break != 0:
            return False
        # Amount is only expected to tie when nothing was quarantined; a
        # quarantined row removes its amount too, which is legitimate.
        if self.quarantined == 0 and self.amount_break not in (None, 0.0):
            return False
        return True

    def report(self) -> str:
        lines = [
            f"Reconciliation — {self.dataset} @ {self.recon_date}",
            f"  {'stage':<14}{'rows':>12}{'amount':>18}",
        ]
        for s in self.stages:
            amt = f"{s.amount:,.2f}" if s.amount is not None else "-"
            lines.append(f"  {s.stage:<14}{s.rows:>12,}{amt:>18}")
        if self.quarantined:
            lines.append(f"  {'quarantined':<14}{-self.quarantined:>12,}")
            lines.append(f"  {'expected':<14}{self.expected_final_rows:>12,}")
        status = "BALANCED" if self.balanced else "*** BREAK ***"
        lines.append(f"  {status}  row break: {self.row_break:+,}")
        if self.amount_break is not None:
            lines.append(f"            amount break: {self.amount_break:+,.2f}")
        return "\n".join(lines)


def totals_from_dataframe(
    df: pd.DataFrame, stage: str, amount_column: str | None = None
) -> StageTotals:
    """Compute stage totals from an in-memory batch."""
    amount = None
    if amount_column and amount_column in df.columns:
        amount = round(float(pd.to_numeric(df[amount_column], errors="coerce").sum()), 2)
    return StageTotals(stage=stage, rows=len(df), amount=amount)


def totals_from_warehouse(
    conn, table: str, amount_column: str | None = None, where: str | None = None
) -> StageTotals:
    """Compute stage totals from a warehouse table.

    NOTE the explicit ::numeric cast on the sum. Postgres would happily return a
    float for a float column, reintroducing exactly the precision problem we are
    trying to detect. Money is compared as decimal, always.
    """
    amount_expr = f", round(sum({amount_column})::numeric, 2)" if amount_column else ", NULL"
    clause = f" WHERE {where}" if where else ""
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*){amount_expr} FROM {table}{clause}")
        rows, amount = cur.fetchone()
    return StageTotals(
        stage=table, rows=int(rows), amount=float(amount) if amount is not None else None
    )


def reconcile(
    dataset: str,
    recon_date: date,
    stages: list[StageTotals],
    quarantined: int = 0,
) -> ReconResult:
    """Compare totals across stages and report any unexplained difference."""
    result = ReconResult(
        dataset=dataset, recon_date=recon_date, stages=stages, quarantined=quarantined
    )
    if result.balanced:
        logger.info("reconciliation balanced for %s", dataset)
    else:
        logger.error("RECONCILIATION BREAK for %s: %+d rows unexplained", dataset, result.row_break)
    return result
