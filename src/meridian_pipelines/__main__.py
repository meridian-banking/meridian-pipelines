"""Command-line interface for the pipelines service.

    python -m meridian_pipelines migrate
    python -m meridian_pipelines check --source-dir ./output/parquet --date 2024-03-15
    python -m meridian_pipelines reconcile --source-dir ./output/parquet --date 2024-03-15
    python -m meridian_pipelines quarantine-report

The `check` command is the one Airflow calls in Sprint 6. Its EXIT CODE is the
contract: 0 means proceed, 1 means stop. That is how a data-quality gate becomes
a real gate rather than a log message everyone scrolls past — the orchestrator
refuses to run downstream tasks when this returns non-zero.
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .dq.runner import failing_row_mask, load_rules, run_checks
from .dq.store import (
    DbConfig,
    connect,
    get_baseline_rows,
    quarantine_rows,
    save_reconciliation,
    save_results,
)
from .recon import reconcile, totals_from_dataframe, totals_from_warehouse

logger = logging.getLogger("meridian_pipelines")

# Which entities we check, their business key, and the parent they reference.
DATASETS = {
    "customers": {"key": "customer_id", "parent_fk": None, "amount": "annual_income"},
    "accounts": {"key": "account_id", "parent_fk": ("customer_id", "customers"), "amount": None},
    "transactions": {
        "key": "transaction_id",
        "parent_fk": ("account_id", "accounts"),
        "amount": "amount",
    },
    "loans": {"key": "loan_id", "parent_fk": ("customer_id", "customers"), "amount": "principal"},
}


def db_config_from_env() -> DbConfig:
    return DbConfig(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5432")),
        database=os.getenv("WAREHOUSE_DB", "meridian"),
        user=os.getenv("WAREHOUSE_USER", "meridian"),
        password=os.getenv("WAREHOUSE_PASSWORD", ""),
    )


def _read(source_dir: Path, entity: str) -> pd.DataFrame | None:
    matches = list(source_dir.glob(f"{entity}/**/*.parquet")) + list(
        source_dir.glob(f"{entity}/*.parquet")
    )
    if not matches:
        logger.warning("no parquet found for %s", entity)
        return None
    return pd.read_parquet(matches[0])


def cmd_migrate(args) -> int:
    """Apply the dq schema migrations."""
    conn = connect(db_config_from_env())
    conn.autocommit = True
    files = sorted(glob.glob(str(Path(args.sql_dir) / "*.sql")))
    with conn.cursor() as cur:
        for path in files:
            logger.info("applying %s", Path(path).name)
            cur.execute(Path(path).read_text())
    conn.close()
    logger.info("applied %d migration(s)", len(files))
    return 0


def cmd_check(args) -> int:
    """Run data-quality checks. EXIT CODE 1 means the pipeline must stop."""
    rules = load_rules(args.rules)
    source = Path(args.source_dir)
    conn = connect(db_config_from_env())
    run_id = args.run_id or f"run_{datetime.now():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    logger.info("data quality run_id=%s", run_id)

    frames: dict[str, pd.DataFrame] = {}
    for entity in DATASETS:
        df = _read(source, entity)
        if df is not None:
            frames[entity] = df

    blocked = False
    for entity, spec in DATASETS.items():
        if entity not in frames:
            continue
        df = frames[entity]

        # Referential checks need the parent's key set.
        parent_keys: dict[str, set[str]] = {}
        if spec["parent_fk"]:
            fk_col, parent_entity = spec["parent_fk"]
            parent_df = frames.get(parent_entity)
            if parent_df is not None:
                parent_key = DATASETS[parent_entity]["key"]
                parent_keys[fk_col] = set(parent_df[parent_key].astype(str))

        baseline = get_baseline_rows(conn, entity)
        suite = run_checks(df, entity, rules, parent_keys, expected_rows=baseline)
        save_results(conn, suite, run_id)

        if suite.should_block:
            blocked = True
            mask = failing_row_mask(df, rules, entity, parent_keys)
            bad = df[mask]
            if not bad.empty:
                reasons = {i: [r.check_name for r in suite.errors] for i in bad.index}
                quarantine_rows(conn, bad, entity, run_id, reasons, spec["key"])
                logger.error(
                    "%s: quarantined %d row(s); %d clean row(s) would proceed",
                    entity,
                    len(bad),
                    len(df) - len(bad),
                )

    conn.close()
    if blocked:
        logger.error("DATA QUALITY GATE FAILED — downstream tasks must not run")
        return 1
    logger.info("data quality gate passed")
    return 0


def cmd_reconcile(args) -> int:
    """Compare source totals against warehouse totals."""
    source = Path(args.source_dir)
    conn = connect(db_config_from_env())
    run_id = args.run_id or f"recon_{datetime.now():%Y%m%d_%H%M%S}"
    all_balanced = True

    targets = {
        "customers": "curated.dim_customer",
        "accounts": "curated.dim_account",
        "transactions": "curated.fact_transactions",
    }

    for entity, table in targets.items():
        df = _read(source, entity)
        if df is None:
            continue
        amount_col = DATASETS[entity]["amount"]
        src = totals_from_dataframe(df, "source", amount_col)

        wh_amount = "amount" if entity == "transactions" else None
        where = (
            "customer_id <> '-1'"
            if entity == "customers"
            else ("account_id <> '-1'" if entity == "accounts" else None)
        )
        whs = totals_from_warehouse(conn, table, wh_amount, where)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM dq.quarantine WHERE dataset = %s AND NOT resolved",
                (entity,),
            )
            quarantined = int(cur.fetchone()[0])

        result = reconcile(entity, args.date, [src, whs], quarantined=quarantined)
        print(result.report())
        print()
        save_reconciliation(
            conn,
            run_id,
            entity,
            args.date,
            "source",
            "warehouse",
            src.rows,
            whs.rows,
            src.amount,
            whs.amount,
            explanation=f"{quarantined} quarantined" if quarantined else None,
            quarantined=quarantined,
        )
        all_balanced &= result.balanced

    conn.close()
    return 0 if all_balanced else 1


def cmd_quarantine_report(args) -> int:
    """Show what is currently sitting in quarantine."""
    conn = connect(db_config_from_env())
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dataset, reason_codes, count(*), min(quarantined_at), max(quarantined_at)
            FROM dq.quarantine
            WHERE NOT resolved
            GROUP BY dataset, reason_codes
            ORDER BY count(*) DESC
        """)
        rows = cur.fetchall()

    if not rows:
        print("Quarantine is empty.")
    else:
        print(f"{'dataset':<16}{'reasons':<44}{'rows':>8}  oldest")
        print("-" * 92)
        for r in rows:
            print(f"{r[0]:<16}{r[1][:42]:<44}{r[2]:>8}  {r[3]:%Y-%m-%d %H:%M}")
    conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="meridian_pipelines")
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    p_mig = sub.add_parser("migrate", help="apply dq schema migrations")
    p_mig.add_argument("--sql-dir", default="sql")
    p_mig.set_defaults(func=cmd_migrate)

    p_chk = sub.add_parser("check", help="run data quality checks (exit 1 = block)")
    p_chk.add_argument("--source-dir", required=True)
    p_chk.add_argument("--rules", default="config/dq_rules.yaml")
    p_chk.add_argument("--run-id", default=None)
    p_chk.set_defaults(func=cmd_check)

    p_rec = sub.add_parser("reconcile", help="compare source and warehouse totals")
    p_rec.add_argument("--source-dir", required=True)
    p_rec.add_argument(
        "--date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(), default=date.today()
    )
    p_rec.add_argument("--run-id", default=None)
    p_rec.set_defaults(func=cmd_reconcile)

    p_q = sub.add_parser("quarantine-report", help="show unresolved quarantined rows")
    p_q.set_defaults(func=cmd_quarantine_report)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s  %(levelname)-7s  %(name)-28s  %(message)s",
        datefmt="%H:%M:%S",
    )
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
