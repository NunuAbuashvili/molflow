-- Records the outcome of every run_quality_checks execution (pass or fail).

CREATE TABLE IF NOT EXISTS quality_check_runs (
    id                  SERIAL PRIMARY KEY,
    dataset_id          TEXT NOT NULL,
    dag_run_id          TEXT NOT NULL,
    passed              BOOLEAN NOT NULL,
    failed_check_count  INTEGER NOT NULL DEFAULT 0,
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quality_check_runs_dag_run_id
    ON quality_check_runs (dag_run_id);
