"""Tests for the data-quality framework and reconciliation.

These tests need no database: checks are pure functions over DataFrames, which
was the point of separating "produce a result" from "write it down". Fast tests
get run; slow tests get skipped.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from meridian_pipelines.dq.checks import (
    check_completeness,
    check_freshness,
    check_referential,
    check_uniqueness,
    check_validity,
    check_volume,
)
from meridian_pipelines.dq.runner import failing_row_mask, run_checks
from meridian_pipelines.dq.types import CheckSuiteResult, Severity
from meridian_pipelines.recon import StageTotals, reconcile, totals_from_dataframe


@pytest.fixture
def customers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": ["CUST_00000001", "CUST_00000002", "CUST_00000003"],
            "credit_score": [720, 650, 800],
            "segment": ["mass", "affluent", "private"],
            "annual_income": [50000.0, 90000.0, 300000.0],
        }
    )


@pytest.fixture
def rules() -> dict:
    return {
        "datasets": {
            "customers": {
                "checks": [
                    {"type": "completeness", "column": "customer_id", "severity": "ERROR"},
                    {"type": "uniqueness", "columns": ["customer_id"], "severity": "ERROR"},
                    {
                        "type": "validity",
                        "column": "credit_score",
                        "min": 300,
                        "max": 850,
                        "severity": "ERROR",
                    },
                    {
                        "type": "validity",
                        "column": "segment",
                        "allowed": ["mass", "affluent", "private"],
                        "severity": "ERROR",
                    },
                ]
            }
        }
    }


# --- completeness -----------------------------------------------------------


def test_completeness_passes_on_full_column(customers):
    r = check_completeness(customers, "customer_id", Severity.ERROR)
    assert r.passed and r.rows_failed == 0


def test_completeness_fails_on_null(customers):
    df = customers.copy()
    df.loc[0, "customer_id"] = None
    r = check_completeness(df, "customer_id", Severity.ERROR)
    assert not r.passed and r.rows_failed == 1


def test_completeness_respects_tolerance(customers):
    """A column allowed to be partly null must not fail within tolerance."""
    df = customers.copy()
    df.loc[0, "annual_income"] = None
    strict = check_completeness(df, "annual_income", Severity.WARN, max_null_pct=0.0)
    lenient = check_completeness(df, "annual_income", Severity.WARN, max_null_pct=0.5)
    assert not strict.passed
    assert lenient.passed


def test_missing_column_is_a_failure_not_a_crash(customers):
    """A dropped column must be reported, not raise — the pipeline should keep
    running and report every problem it finds."""
    r = check_completeness(customers, "does_not_exist", Severity.ERROR)
    assert not r.passed and "missing" in r.message


# --- uniqueness -------------------------------------------------------------


def test_uniqueness_passes_on_distinct_keys(customers):
    assert check_uniqueness(customers, ["customer_id"], Severity.ERROR).passed


def test_uniqueness_counts_all_rows_in_a_duplicate_group(customers):
    """keep=False means BOTH copies are reported, because you cannot tell which
    one is the impostor without investigating."""
    df = pd.concat([customers, customers.iloc[[0]]], ignore_index=True)
    r = check_uniqueness(df, ["customer_id"], Severity.ERROR)
    assert not r.passed and r.rows_failed == 2


# --- validity ---------------------------------------------------------------


@pytest.mark.parametrize(
    "column,value",
    [("credit_score", 9999), ("credit_score", 100), ("segment", "platinum")],
)
def test_validity_catches_bad_values(customers, rules, column, value):
    df = customers.copy()
    df.loc[0, column] = value
    spec = next(
        c
        for c in rules["datasets"]["customers"]["checks"]
        if c["type"] == "validity" and c["column"] == column
    )
    r = check_validity(
        df,
        column,
        Severity.ERROR,
        min_value=spec.get("min"),
        max_value=spec.get("max"),
        allowed=spec.get("allowed"),
    )
    assert not r.passed and r.rows_failed == 1


def test_validity_ignores_nulls(customers):
    """Nulls are completeness's job. Conflating them buries the root cause."""
    df = customers.copy()
    df.loc[0, "credit_score"] = None
    r = check_validity(df, "credit_score", Severity.ERROR, min_value=300, max_value=850)
    assert r.passed, "a null must not be reported as an out-of-range value"


def test_validity_pattern(customers):
    df = customers.copy()
    df.loc[0, "customer_id"] = "BADFORMAT"
    r = check_validity(df, "customer_id", Severity.ERROR, pattern=r"^CUST_[0-9]{8}$")
    assert not r.passed and r.rows_failed == 1


# --- referential ------------------------------------------------------------


def test_referential_detects_orphans():
    facts = pd.DataFrame({"account_id": ["A1", "A2", "GHOST"]})
    r = check_referential(facts, "account_id", {"A1", "A2"}, Severity.ERROR, "account")
    assert not r.passed and r.rows_failed == 1
    assert "GHOST" in r.sample_failures


def test_referential_passes_when_all_keys_exist():
    facts = pd.DataFrame({"account_id": ["A1", "A2"]})
    assert check_referential(facts, "account_id", {"A1", "A2", "A3"}, Severity.ERROR).passed


# --- freshness --------------------------------------------------------------


def test_freshness_passes_on_recent_data():
    now = datetime(2024, 3, 15, 12, 0, tzinfo=timezone.utc)
    df = pd.DataFrame({"ts": [now - timedelta(hours=2)]})
    assert check_freshness(df, "ts", Severity.WARN, max_age_hours=48, now=now).passed


def test_freshness_fails_on_stale_data():
    """The only check that catches 'nothing is wrong except it's the wrong day'."""
    now = datetime(2024, 3, 15, 12, 0, tzinfo=timezone.utc)
    df = pd.DataFrame({"ts": [now - timedelta(days=5)]})
    r = check_freshness(df, "ts", Severity.WARN, max_age_hours=48, now=now)
    assert not r.passed


# --- volume -----------------------------------------------------------------


def test_volume_passes_within_tolerance():
    df = pd.DataFrame({"x": range(950)})
    assert check_volume(df, Severity.ERROR, expected_rows=1000, tolerance_pct=20).passed


def test_volume_catches_silent_truncation():
    """Every row perfect, 90% of them missing — no row-level check would fire."""
    df = pd.DataFrame({"x": range(100)})
    r = check_volume(df, Severity.ERROR, expected_rows=1000, tolerance_pct=40)
    assert not r.passed


def test_volume_establishes_baseline_on_first_run():
    """A first run cannot be anomalous relative to a baseline that doesn't exist."""
    df = pd.DataFrame({"x": range(500)})
    r = check_volume(df, Severity.ERROR, expected_rows=None)
    assert r.passed and "baseline" in r.message


# --- severity and blocking --------------------------------------------------


def test_only_errors_block(customers, rules):
    suite = run_checks(customers, "customers", rules)
    assert not suite.should_block

    warn_rules = {
        "datasets": {
            "customers": {
                "checks": [
                    {"type": "validity", "column": "credit_score", "max": 100, "severity": "WARN"}
                ]
            }
        }
    }
    warned = run_checks(customers, "customers", warn_rules)
    assert warned.warnings, "the check should have failed"
    assert not warned.should_block, "a WARN must never block the pipeline"


def test_errors_block(customers, rules):
    df = customers.copy()
    df.loc[0, "segment"] = "platinum"
    assert run_checks(df, "customers", rules).should_block


# --- quarantine splitting ---------------------------------------------------


def test_failing_mask_isolates_only_bad_rows(customers, rules):
    """The whole point of quarantine: bad rows out, good rows still flow."""
    df = customers.copy()
    df.loc[1, "credit_score"] = 9999
    mask = failing_row_mask(df, rules, "customers")
    assert int(mask.sum()) == 1
    assert int((~mask).sum()) == 2


def test_warn_severity_rows_are_not_quarantined(customers):
    """A WARN means 'look at this', not 'reject this'."""
    warn_rules = {
        "datasets": {
            "customers": {
                "checks": [
                    {"type": "validity", "column": "credit_score", "max": 700, "severity": "WARN"}
                ]
            }
        }
    }
    mask = failing_row_mask(customers, warn_rules, "customers")
    assert int(mask.sum()) == 0


# --- reconciliation ---------------------------------------------------------


def test_reconciliation_balances_when_nothing_lost(customers):
    src = totals_from_dataframe(customers, "source", "annual_income")
    whs = totals_from_dataframe(customers, "warehouse", "annual_income")
    assert reconcile("customers", date(2024, 3, 15), [src, whs]).balanced


def test_quarantined_rows_are_an_explained_difference(customers):
    """A difference is not a break if it is accounted for."""
    src = totals_from_dataframe(customers, "source", "annual_income")
    whs = totals_from_dataframe(customers.iloc[1:], "warehouse", "annual_income")
    result = reconcile("customers", date(2024, 3, 15), [src, whs], quarantined=1)
    assert result.balanced and result.row_break == 0


def test_unexplained_loss_is_a_break(customers):
    """The same shaped difference, with nothing accounting for it, must fail."""
    src = totals_from_dataframe(customers, "source", "annual_income")
    whs = totals_from_dataframe(customers.iloc[1:], "warehouse", "annual_income")
    result = reconcile("customers", date(2024, 3, 15), [src, whs], quarantined=0)
    assert not result.balanced and result.row_break == -1


def test_reconciliation_detects_duplication():
    """Losing rows is not the only failure — gaining them is just as wrong."""
    src = StageTotals("source", 100, 1000.0)
    whs = StageTotals("warehouse", 200, 2000.0)
    result = reconcile("x", date(2024, 3, 15), [src, whs])
    assert not result.balanced and result.row_break == 100


# --- suite behaviour --------------------------------------------------------


def test_empty_suite_does_not_block():
    assert not CheckSuiteResult(dataset="none").should_block


def test_unknown_dataset_returns_empty_suite(customers):
    suite = run_checks(customers, "not_configured", {"datasets": {}})
    assert suite.results == [] and not suite.should_block
