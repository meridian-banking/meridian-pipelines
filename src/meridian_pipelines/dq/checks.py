"""The six check types, implemented against pandas DataFrames.

Each check is a small function with the same shape:
    (df, config) -> CheckResult

That uniform signature is what lets the runner execute them generically from a
YAML config without knowing anything about individual checks.

WHY PANDAS AND NOT SQL HERE?
These checks run on data in flight, BEFORE it lands in the warehouse — you
cannot SQL-query a table that has not been written yet. Catching a problem here
means the bad data never enters the warehouse at all, rather than entering and
needing to be surgically removed. (Warehouse-side checks also exist, in sql/ —
the two layers catch different things.)
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from .types import CheckResult, CheckType, Severity

_SAMPLE_SIZE = 5


def _sample(series: pd.Series, mask: pd.Series) -> list[Any]:
    """A few offending values, for diagnosis. Never the whole failing set."""
    failing = series[mask]
    return [str(v) for v in failing.head(_SAMPLE_SIZE).tolist()]


# --- 1. COMPLETENESS -------------------------------------------------------


def check_completeness(
    df: pd.DataFrame, column: str, severity: Severity, max_null_pct: float = 0.0
) -> CheckResult:
    """Are required fields populated?

    max_null_pct allows a tolerance. Some columns are genuinely optional-ish:
    a middle name may be 40% null and that is fine. An account_id at 0.1% null
    is a crisis. The tolerance makes that distinction explicit rather than
    hiding it in code.
    """
    if column not in df.columns:
        return CheckResult(
            f"completeness_{column}",
            CheckType.COMPLETENESS,
            severity,
            "",
            column,
            False,
            len(df),
            len(df),
            f"column {column!r} is missing",
        )

    null_mask = df[column].isna()
    failed = int(null_mask.sum())
    pct = failed / len(df) if len(df) else 0.0
    passed = pct <= max_null_pct

    return CheckResult(
        check_name=f"completeness_{column}",
        check_type=CheckType.COMPLETENESS,
        severity=severity,
        dataset="",
        column=column,
        passed=passed,
        rows_checked=len(df),
        rows_failed=failed,
        message=(
            f"{pct:.2%} null (tolerance {max_null_pct:.2%})" if not passed else f"{pct:.2%} null"
        ),
        sample_failures=_sample(df.index.to_series(), null_mask),
    )


# --- 2. UNIQUENESS ---------------------------------------------------------


def check_uniqueness(df: pd.DataFrame, columns: list[str], severity: Severity) -> CheckResult:
    """Are the declared key columns actually unique?

    Duplicate keys are among the most destructive defects in analytics: they
    silently multiply rows on every downstream join, so totals inflate without
    anything erroring. A 2x duplicate in a dimension can double a revenue
    figure. This is why key uniqueness is almost always ERROR severity.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        return CheckResult(
            f"uniqueness_{'_'.join(columns)}",
            CheckType.UNIQUENESS,
            severity,
            "",
            ",".join(columns),
            False,
            len(df),
            len(df),
            f"missing column(s): {missing}",
        )

    dupe_mask = df.duplicated(subset=columns, keep=False)
    failed = int(dupe_mask.sum())

    return CheckResult(
        check_name=f"uniqueness_{'_'.join(columns)}",
        check_type=CheckType.UNIQUENESS,
        severity=severity,
        dataset="",
        column=",".join(columns),
        passed=failed == 0,
        rows_checked=len(df),
        rows_failed=failed,
        message=f"{failed:,} rows involved in duplicate keys",
        sample_failures=_sample(df[columns[0]], dupe_mask),
    )


# --- 3. VALIDITY -----------------------------------------------------------


def check_validity(
    df: pd.DataFrame,
    column: str,
    severity: Severity,
    min_value: float | None = None,
    max_value: float | None = None,
    allowed: list[str] | None = None,
    pattern: str | None = None,
) -> CheckResult:
    """Are values within range, in the allowed set, or matching a pattern?

    Nulls are EXCLUDED from validity checks on purpose: "is this null?" is the
    completeness check's job. Conflating the two produces confusing results
    where a completeness failure also trips three validity checks, burying the
    actual root cause under noise.
    """
    if column not in df.columns:
        return CheckResult(
            f"validity_{column}",
            CheckType.VALIDITY,
            severity,
            "",
            column,
            False,
            len(df),
            len(df),
            f"column {column!r} is missing",
        )

    series = df[column]
    non_null = series.notna()
    fail_mask = pd.Series(False, index=df.index)
    reasons: list[str] = []

    if min_value is not None or max_value is not None:
        numeric = pd.to_numeric(series, errors="coerce")
        if min_value is not None:
            below = non_null & (numeric < min_value)
            fail_mask |= below.fillna(False)
            if below.sum():
                reasons.append(f"{int(below.sum())} below min {min_value}")
        if max_value is not None:
            above = non_null & (numeric > max_value)
            fail_mask |= above.fillna(False)
            if above.sum():
                reasons.append(f"{int(above.sum())} above max {max_value}")

    if allowed is not None:
        outside = non_null & ~series.astype(str).isin(set(allowed))
        fail_mask |= outside
        if outside.sum():
            reasons.append(f"{int(outside.sum())} outside allowed set")

    if pattern is not None:
        rx = re.compile(pattern)
        unmatched = non_null & ~series.astype(str).str.match(rx)
        fail_mask |= unmatched
        if unmatched.sum():
            reasons.append(f"{int(unmatched.sum())} not matching {pattern}")

    failed = int(fail_mask.sum())
    return CheckResult(
        check_name=f"validity_{column}",
        check_type=CheckType.VALIDITY,
        severity=severity,
        dataset="",
        column=column,
        passed=failed == 0,
        rows_checked=int(non_null.sum()),
        rows_failed=failed,
        message="; ".join(reasons) if reasons else "all values valid",
        sample_failures=_sample(series, fail_mask),
    )


# --- 4. REFERENTIAL INTEGRITY ----------------------------------------------


def check_referential(
    df: pd.DataFrame,
    column: str,
    parent_keys: set[str],
    severity: Severity,
    parent_name: str = "parent",
) -> CheckResult:
    """Do foreign keys point at rows that actually exist?

    WHY THIS MATTERS MORE THAN IT LOOKS: an orphaned foreign key does not throw
    an error downstream — it silently VANISHES on an INNER JOIN. Ten thousand
    orphaned transactions do not produce ten thousand errors; they produce a
    revenue report that is quietly short by ten thousand transactions, with
    nothing anywhere indicating a problem. Silent loss is the worst failure
    mode in analytics, which is why this check is nearly always ERROR.
    """
    if column not in df.columns:
        return CheckResult(
            f"referential_{column}",
            CheckType.REFERENTIAL,
            severity,
            "",
            column,
            False,
            len(df),
            len(df),
            f"column {column!r} is missing",
        )

    non_null = df[column].notna()
    orphan_mask = non_null & ~df[column].astype(str).isin(parent_keys)
    failed = int(orphan_mask.sum())

    return CheckResult(
        check_name=f"referential_{column}",
        check_type=CheckType.REFERENTIAL,
        severity=severity,
        dataset="",
        column=column,
        passed=failed == 0,
        rows_checked=int(non_null.sum()),
        rows_failed=failed,
        message=f"{failed:,} rows reference a missing {parent_name}",
        sample_failures=_sample(df[column], orphan_mask),
    )


# --- 5. FRESHNESS ----------------------------------------------------------


def check_freshness(
    df: pd.DataFrame,
    column: str,
    severity: Severity,
    max_age_hours: float = 48.0,
    now: datetime | None = None,
) -> CheckResult:
    """Is the data recent enough to be useful?

    A STALE DATASET PASSES EVERY OTHER CHECK. Structure, keys, ranges, and
    references can all be perfect while the file is three days old because an
    upstream job silently stopped running. Freshness is the only check that
    catches "nothing is wrong with this data except that it is the wrong data".
    """
    if column not in df.columns:
        return CheckResult(
            f"freshness_{column}",
            CheckType.FRESHNESS,
            severity,
            "",
            column,
            False,
            len(df),
            len(df),
            f"column {column!r} is missing",
        )

    now = now or datetime.now(timezone.utc)
    ts = pd.to_datetime(df[column], errors="coerce", utc=True, format="mixed")
    if ts.notna().sum() == 0:
        return CheckResult(
            f"freshness_{column}",
            CheckType.FRESHNESS,
            severity,
            "",
            column,
            False,
            len(df),
            len(df),
            "no parseable timestamps",
        )

    newest = ts.max()
    # .floor('s') avoids a pandas warning about discarding sub-second precision;
    # freshness is measured in hours, so nanoseconds are irrelevant here.
    age_hours = (now - newest.floor("s").to_pydatetime()).total_seconds() / 3600

    return CheckResult(
        check_name=f"freshness_{column}",
        check_type=CheckType.FRESHNESS,
        severity=severity,
        dataset="",
        column=column,
        passed=age_hours <= max_age_hours,
        rows_checked=len(df),
        rows_failed=0 if age_hours <= max_age_hours else len(df),
        message=f"newest record is {age_hours:.1f}h old (max {max_age_hours}h)",
    )


# --- 6. VOLUME ANOMALY -----------------------------------------------------


def check_volume(
    df: pd.DataFrame,
    severity: Severity,
    expected_rows: float | None,
    tolerance_pct: float = 50.0,
    min_rows: int = 1,
) -> CheckResult:
    """Is today's row count plausible against history?

    THE CHECK THAT CATCHES WHAT ROW-LEVEL CHECKS CANNOT.
    Every individual row can be immaculate while the DATASET is broken. If a
    source system silently exports 10% of records, no completeness, validity,
    uniqueness, or referential check will fire — every one of those 10% of rows
    is perfectly formed. Only comparing the count against history notices.

    We compare against a trailing average rather than a hard-coded number
    because real volumes drift (business grows, seasonality). A fixed threshold
    is either too loose to catch anything or too tight to survive a busy week.
    """
    n = len(df)

    if n < min_rows:
        return CheckResult(
            "volume",
            CheckType.VOLUME,
            severity,
            "",
            None,
            False,
            n,
            n,
            f"{n:,} rows is below the absolute minimum of {min_rows:,}",
        )

    if expected_rows is None:
        # No history yet — record the count, do not fail. A first run cannot be
        # anomalous relative to a baseline that does not exist.
        return CheckResult(
            "volume",
            CheckType.VOLUME,
            severity,
            "",
            None,
            True,
            n,
            0,
            f"{n:,} rows (no baseline yet — establishing one)",
        )

    deviation = abs(n - expected_rows) / expected_rows * 100 if expected_rows else 0.0
    passed = deviation <= tolerance_pct

    return CheckResult(
        check_name="volume",
        check_type=CheckType.VOLUME,
        severity=severity,
        dataset="",
        column=None,
        passed=passed,
        rows_checked=n,
        rows_failed=0 if passed else n,
        message=(
            f"{n:,} rows vs baseline {expected_rows:,.0f} "
            f"({deviation:.1f}% deviation, tolerance {tolerance_pct}%)"
        ),
    )


def days_between(a: datetime, b: datetime) -> timedelta:
    """Small helper kept for readability in freshness configuration."""
    return abs(a - b)
