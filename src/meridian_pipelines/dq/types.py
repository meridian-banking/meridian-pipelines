"""Core types for the data-quality framework.

DESIGN PRINCIPLE: a check returns a RESULT, it does not raise and it does not
decide what happens next. Separating "what did we find" from "what do we do
about it" is what makes the framework composable: the same check can block a
load in production and merely warn in a backfill, without changing its code.

SEVERITY is the hinge of the whole design:

    ERROR  the data is wrong in a way that would corrupt downstream numbers.
           Block the load. Quarantine the offending rows.
    WARN   something looks off but is not provably wrong.
           Log it, alert, let the pipeline proceed.

Getting this balance wrong is costly in BOTH directions, and interviewers probe
it. Too many ERRORs and the pipeline halts nightly on trivia, so people start
bypassing it — a control everyone ignores is worse than no control, because it
creates false confidence. Too few and bad data reaches a regulatory filing.

The rule we apply: ERROR when a downstream NUMBER would be wrong. WARN when a
human should look but the arithmetic still holds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class Severity(str, Enum):
    """How seriously to treat a failed check."""

    ERROR = "ERROR"
    WARN = "WARN"


class CheckType(str, Enum):
    """The six categories of data-quality check.

    These categories are near-universal — Great Expectations, Soda, and dbt
    tests all decompose into roughly this set. Knowing the taxonomy is more
    valuable than knowing any one tool's syntax.
    """

    COMPLETENESS = "completeness"  # required fields populated?
    UNIQUENESS = "uniqueness"  # keys actually unique?
    VALIDITY = "validity"  # values in range / allowed set / pattern?
    REFERENTIAL = "referential"  # do foreign keys point at real rows?
    FRESHNESS = "freshness"  # is the data recent enough to be useful?
    VOLUME = "volume"  # is today's row count plausible vs history?


@dataclass
class CheckResult:
    """The outcome of running one check against one dataset.

    Note what is captured beyond pass/fail: the failing ROW COUNT and a small
    SAMPLE of offending keys. "Uniqueness failed" sends someone hunting;
    "uniqueness failed on 3 rows, here are their ids" sends them straight to the
    problem. Diagnostic payload is the difference between a check that is used
    and one that is muted.
    """

    check_name: str
    check_type: CheckType
    severity: Severity
    dataset: str
    column: str | None
    passed: bool
    rows_checked: int
    rows_failed: int
    message: str
    sample_failures: list[Any] = field(default_factory=list)
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def failure_rate(self) -> float:
        if self.rows_checked == 0:
            return 0.0
        return self.rows_failed / self.rows_checked

    def summary(self) -> str:
        status = "PASS" if self.passed else f"FAIL[{self.severity.value}]"
        col = f".{self.column}" if self.column else ""
        return (
            f"{status:12} {self.dataset}{col:24} {self.check_name:28} "
            f"{self.rows_failed:>7,}/{self.rows_checked:<9,} {self.message}"
        )


@dataclass
class CheckSuiteResult:
    """The outcome of running every check for one dataset in one run."""

    dataset: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity is Severity.WARN]

    @property
    def should_block(self) -> bool:
        """Whether the pipeline must stop.

        ONLY errors block. This single property is where the severity design
        turns into behaviour, and keeping it in one place means the blocking
        rule cannot drift between pipelines.
        """
        return len(self.errors) > 0

    def summary(self) -> str:
        passed = sum(1 for r in self.results if r.passed)
        lines = [
            f"Data quality for {self.dataset}: "
            f"{passed}/{len(self.results)} passed, "
            f"{len(self.errors)} error(s), {len(self.warnings)} warning(s)"
        ]
        lines.extend("  " + r.summary() for r in self.results if not r.passed)
        return "\n".join(lines)
