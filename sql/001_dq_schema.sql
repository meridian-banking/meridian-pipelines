-- =============================================================================
-- DATA QUALITY SCHEMA
-- =============================================================================
-- Three tables, each answering a different operational question:
--
--   dq.check_results   "did the data pass its checks, and has that changed?"
--   dq.quarantine      "which exact rows were rejected, and why?"
--   dq.reconciliation  "do warehouse totals equal source totals?"
--
-- WHY PERSIST CHECK RESULTS RATHER THAN JUST LOGGING THEM?
-- Because the valuable signal is usually a TREND, not a single failure. A null
-- rate that creeps from 0.1% to 0.4% to 1.2% over three weeks never trips a
-- threshold on any single day, but the trajectory is the story. You cannot see
-- a trajectory in logs; you can see it in a table.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dq;


-- =============================================================================
-- dq.check_results — one row per check, per dataset, per run
-- =============================================================================
CREATE TABLE IF NOT EXISTS dq.check_results (
    result_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          TEXT        NOT NULL,   -- groups all checks from one pipeline run
    load_id         BIGINT,                 -- links to audit.load_log from Sprint 0
    dataset         TEXT        NOT NULL,
    check_name      TEXT        NOT NULL,
    check_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL CHECK (severity IN ('ERROR', 'WARN')),
    column_name     TEXT,
    passed          BOOLEAN     NOT NULL,
    rows_checked    BIGINT      NOT NULL,
    rows_failed     BIGINT      NOT NULL,
    failure_rate    NUMERIC(9,6),
    message         TEXT,
    sample_failures TEXT,                   -- JSON array of a few offending values
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dq.check_results IS
    'GRAIN: one row per check per dataset per run. Enables trending a metric '
    'over time, which is where most real data-quality signal lives.';

-- Trend queries filter by dataset+check and order by time; index accordingly.
CREATE INDEX IF NOT EXISTS idx_dq_results_trend
    ON dq.check_results (dataset, check_name, executed_at DESC);

-- Operational queries want "what failed in this run".
CREATE INDEX IF NOT EXISTS idx_dq_results_run
    ON dq.check_results (run_id) WHERE NOT passed;


-- =============================================================================
-- dq.quarantine — the rejected rows themselves
-- =============================================================================
-- Rows are stored as JSON rather than in typed columns. That is deliberate:
-- quarantine must accept rows from ANY dataset, including rows that are
-- malformed precisely because they do not fit the expected types. A typed
-- quarantine table would reject the very rows it exists to capture.
CREATE TABLE IF NOT EXISTS dq.quarantine (
    quarantine_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          TEXT        NOT NULL,
    dataset         TEXT        NOT NULL,
    business_key    TEXT,                   -- the natural key, when identifiable
    reason_codes    TEXT        NOT NULL,   -- which checks this row failed
    row_data        JSONB       NOT NULL,   -- the full original row
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved        BOOLEAN     NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,
    resolution_note TEXT
);

COMMENT ON TABLE dq.quarantine IS
    'Rejected rows with reason codes. Rows are JSONB because quarantine must '
    'accept malformed data that would not fit a typed schema.';

CREATE INDEX IF NOT EXISTS idx_dq_quarantine_open
    ON dq.quarantine (dataset, quarantined_at DESC) WHERE NOT resolved;

-- GIN index lets you search inside the JSON payload, e.g. find every
-- quarantined row for a particular customer across all datasets.
CREATE INDEX IF NOT EXISTS idx_dq_quarantine_data
    ON dq.quarantine USING GIN (row_data);


-- =============================================================================
-- dq.reconciliation — source vs warehouse totals
-- =============================================================================
-- THE CONTROL THAT MAKES A BANK TRUST THE WAREHOUSE.
-- Every hop in the pipeline is a chance to lose or duplicate rows. Reconciliation
-- asks the blunt question at each hop: same number of rows? same total amount?
-- In a real bank this is a daily, formal, signed-off process, and an unexplained
-- break is an incident with a named owner — not a ticket for "sometime next
-- sprint".
CREATE TABLE IF NOT EXISTS dq.reconciliation (
    recon_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              TEXT        NOT NULL,
    dataset             TEXT        NOT NULL,
    recon_date          DATE        NOT NULL,
    stage_from          TEXT        NOT NULL,   -- e.g. 'source'
    stage_to            TEXT        NOT NULL,   -- e.g. 'warehouse'
    rows_from           BIGINT      NOT NULL,
    rows_to             BIGINT      NOT NULL,
    amount_from         NUMERIC(20,2),
    amount_to           NUMERIC(20,2),
    row_difference      BIGINT      GENERATED ALWAYS AS (rows_to - rows_from) STORED,
    amount_difference   NUMERIC(20,2) GENERATED ALWAYS AS
                            (COALESCE(amount_to, 0) - COALESCE(amount_from, 0)) STORED,
    balanced            BOOLEAN     NOT NULL,
    explanation         TEXT,
    executed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dq.reconciliation IS
    'Row and amount totals compared across pipeline stages. An unbalanced row '
    'is a break requiring explanation before the data is trusted.';

CREATE INDEX IF NOT EXISTS idx_dq_recon_breaks
    ON dq.reconciliation (recon_date DESC, dataset) WHERE NOT balanced;


-- =============================================================================
-- A convenience view: the latest state of every check
-- =============================================================================
CREATE OR REPLACE VIEW dq.v_latest_check_status AS
SELECT DISTINCT ON (dataset, check_name)
    dataset,
    check_name,
    check_type,
    severity,
    passed,
    rows_failed,
    failure_rate,
    message,
    executed_at
FROM dq.check_results
ORDER BY dataset, check_name, executed_at DESC;

COMMENT ON VIEW dq.v_latest_check_status IS
    'Most recent result for each check. DISTINCT ON is a Postgres feature that '
    'returns the first row per group given an ORDER BY — cleaner than a '
    'ROW_NUMBER() subquery for this exact pattern.';
