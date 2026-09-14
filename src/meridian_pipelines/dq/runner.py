"""Run a suite of checks defined in YAML against a DataFrame.

WHY YAML RATHER THAN CODE?
Data-quality rules are a BUSINESS artifact, not an engineering one. The person
who knows that credit_score must be 300-850, or that a missing account_id is
a crisis while a missing middle name is not, is often an analyst or a data
steward rather than a Python developer. Expressing rules as data means:

  - they are reviewable in a PR by someone who does not read Python
  - adding a rule does not require a code change or a deploy
  - the same rules can be rendered into documentation
  - severity can be tuned per environment without touching logic

This is the same reasoning that put data contracts in JSON back in Sprint 2
(ADR 0003), applied one layer further into the pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .checks import (
    check_completeness,
    check_freshness,
    check_referential,
    check_uniqueness,
    check_validity,
    check_volume,
)
from .types import CheckResult, CheckSuiteResult, Severity

logger = logging.getLogger("meridian_pipelines.dq")


def load_rules(path: str | Path) -> dict[str, Any]:
    """Load the DQ rule file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"DQ rules not found: {path}")
    return yaml.safe_load(path.read_text())


def run_checks(
    df: pd.DataFrame,
    dataset: str,
    rules: dict[str, Any],
    parent_keys: dict[str, set[str]] | None = None,
    expected_rows: float | None = None,
) -> CheckSuiteResult:
    """Execute every configured check for one dataset.

    parent_keys supplies the valid key sets for referential checks — e.g.
    {"account_id": {...all account ids...}}. They are passed in rather than
    looked up here so this module stays free of database dependencies and can
    be tested without any infrastructure.
    """
    dataset_rules = rules.get("datasets", {}).get(dataset)
    if dataset_rules is None:
        logger.warning("no DQ rules defined for dataset %r", dataset)
        return CheckSuiteResult(dataset=dataset)

    parent_keys = parent_keys or {}
    results: list[CheckResult] = []

    for spec in dataset_rules.get("checks", []):
        kind = spec["type"]
        severity = Severity(spec.get("severity", "ERROR"))

        if kind == "completeness":
            result = check_completeness(
                df,
                spec["column"],
                severity,
                max_null_pct=spec.get("max_null_pct", 0.0),
            )
        elif kind == "uniqueness":
            result = check_uniqueness(df, spec["columns"], severity)
        elif kind == "validity":
            result = check_validity(
                df,
                spec["column"],
                severity,
                min_value=spec.get("min"),
                max_value=spec.get("max"),
                allowed=spec.get("allowed"),
                pattern=spec.get("pattern"),
            )
        elif kind == "referential":
            keys = parent_keys.get(spec["column"])
            if keys is None:
                logger.warning(
                    "skipping referential check on %s: no parent keys supplied",
                    spec["column"],
                )
                continue
            result = check_referential(
                df,
                spec["column"],
                keys,
                severity,
                parent_name=spec.get("parent", "parent"),
            )
        elif kind == "freshness":
            result = check_freshness(
                df,
                spec["column"],
                severity,
                max_age_hours=spec.get("max_age_hours", 48.0),
            )
        elif kind == "volume":
            result = check_volume(
                df,
                severity,
                expected_rows=expected_rows,
                tolerance_pct=spec.get("tolerance_pct", 50.0),
                min_rows=spec.get("min_rows", 1),
            )
        else:
            logger.warning("unknown check type %r, skipping", kind)
            continue

        result.dataset = dataset
        results.append(result)

    suite = CheckSuiteResult(dataset=dataset, results=results)
    logger.info(suite.summary())
    return suite


def failing_row_mask(
    df: pd.DataFrame,
    rules: dict[str, Any],
    dataset: str,
    parent_keys: dict[str, set[str]] | None = None,
) -> pd.Series:
    """Boolean mask of rows that fail any ERROR-severity row-level check.

    Used to split a batch into clean rows (which proceed) and quarantined rows
    (which do not). Note it only considers ROW-LEVEL checks — freshness and
    volume are dataset-level properties, so there is no such thing as "the rows
    that caused the volume anomaly"; those either block the whole load or do
    not.
    """
    parent_keys = parent_keys or {}
    dataset_rules = rules.get("datasets", {}).get(dataset, {})
    mask = pd.Series(False, index=df.index)

    for spec in dataset_rules.get("checks", []):
        if Severity(spec.get("severity", "ERROR")) is not Severity.ERROR:
            continue
        kind = spec["type"]

        if kind == "completeness" and spec["column"] in df.columns:
            if spec.get("max_null_pct", 0.0) == 0.0:
                mask |= df[spec["column"]].isna()
        elif kind == "uniqueness" and all(c in df.columns for c in spec["columns"]):
            mask |= df.duplicated(subset=spec["columns"], keep="first")
        elif kind == "validity" and spec["column"] in df.columns:
            col = df[spec["column"]]
            non_null = col.notna()
            if spec.get("min") is not None:
                numeric = pd.to_numeric(col, errors="coerce")
                mask |= (non_null & (numeric < spec["min"])).fillna(False)
            if spec.get("max") is not None:
                numeric = pd.to_numeric(col, errors="coerce")
                mask |= (non_null & (numeric > spec["max"])).fillna(False)
            if spec.get("allowed") is not None:
                mask |= non_null & ~col.astype(str).isin(set(spec["allowed"]))
        elif kind == "referential" and spec["column"] in df.columns:
            keys = parent_keys.get(spec["column"])
            if keys is not None:
                col = df[spec["column"]]
                mask |= col.notna() & ~col.astype(str).isin(keys)

    return mask
