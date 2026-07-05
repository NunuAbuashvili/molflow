-- molflow tracking schema

CREATE TABLE IF NOT EXISTS processed_datasets (
    id                   SERIAL PRIMARY KEY,
    dataset_id           TEXT NOT NULL,
    dag_run_id           TEXT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    molecules_generated  INTEGER,
    error_message        TEXT,
    overwrite            BOOLEAN NOT NULL DEFAULT FALSE,
    started_at           TIMESTAMPTZ NOT NULL,
    finished_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fast lookup for "has this dataset already been processed successfully?"
CREATE INDEX IF NOT EXISTS idx_processed_datasets_dataset_id
    ON processed_datasets (dataset_id, finished_at DESC);
