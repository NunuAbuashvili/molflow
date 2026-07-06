# Molflow

An Airflow-based cheminformatics pipeline for a drug discovery scenario. Scientists upload paired scaffold/R-group CSVs to object storage; the pipeline generates candidate molecules, computes molecular properties, clusters them, runs data quality checks, and reports results back — on a weekly schedule, with full run tracking and a Teams notification per run.

## Architecture

| Component | Role |
|---|---|
| **Apache Airflow 3.2.2** (LocalExecutor) | Orchestration |
| **MinIO** (S3-compatible) | Object storage for input/output CSVs |
| **Postgres 16** | Two logical databases in one container: `airflow` (Airflow's own metadata) and `molflow` (this project's run-tracking tables) |
| **RDKit / pandas / NumPy / scikit-learn** | Molecule generation, property calculation, K-Means clustering |
| **Pandera** | Data quality validation on final results |
| **MS Teams (Adaptive Card webhook)** | One summary notification per DAG run |

All of it runs via Docker Compose, with a custom `Dockerfile` extending `apache/airflow:3.2.2` to add the cheminformatics/notification/quality dependencies.

## Pipeline (`dags/molflow_pipeline.py`)

Runs weekly (`@weekly`), or on manual trigger. Each run:

1. **`discover_new_datasets`** — scans the `raw-data` MinIO bucket for complete `<id>_scaffolds.csv` + `<id>_r_groups.csv` pairs, and filters out datasets already successfully processed (tracked in `processed_datasets`), unless the `overwrite` trigger param is `True`.
2. **`fetch_scaffolds` / `fetch_r_groups`** — read each dataset's SMILES from MinIO (dynamically mapped, one instance per dataset).
3. **`generate`** — combines scaffolds + R-groups into candidate molecules via RDKit (`include/generation.py`).
4. **`properties`** / **`cluster`** — molecular descriptors (MW, logP, TPSA, HBA/HBD, Lipinski Ro5) and K-Means clustering on Morgan fingerprints (`include/properties.py`, `include/clustering.py`).
5. **`merge_and_upload`** — merges properties + cluster assignments, writes `<id>_results.csv` to the `processed-data` bucket, records the outcome (success/failed) to `processed_datasets`.
6. **`run_quality_checks`** — validates the merged results against a Pandera schema (`include/quality_checks.py`): non-null/unique SMILES, valid character set, non-negative physical properties, and a cross-column check that `lipinski_pass` is actually consistent with its source columns. Outcome recorded to `quality_check_runs`.
7. **`notify_run_summary`** — one Adaptive Card message to Teams per run, unconditionally: datasets processed OK vs. failed (and why), and quality checks passed vs. failed (and how many checks).

A bad dataset (unparseable SMILES, missing file, failed check) is recorded and skipped (`AirflowSkipException`), not treated as a pipeline failure — one scientist's bad upload doesn't block anyone else's results in the same run.

## Repository layout

```
molflow/
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .env                          # not committed — see setup below
├── postgres/
│   ├── init-molflow-db.sql       # auto-creates `molflow` DB on a fresh volume only
│   ├── schema.sql                # processed_datasets
│   └── schema_quality_tracking.sql  # quality_check_runs
├── dags/
│   └── molflow_pipeline.py
├── include/
│   ├── generation.py
│   ├── properties.py
│   ├── clustering.py
│   ├── quality_checks.py
│   └── notifications.py
├── plugins/
├── config/
└── logs/
```

## Setup

**1. Environment file**

```bash
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env   # Linux/Mac; use AIRFLOW_UID=50000 on native Windows
```

Fill in `.env`: `POSTGRES_USER`/`PASSWORD`, `MINIO_ROOT_USER`/`PASSWORD`, `_AIRFLOW_WWW_USER_USERNAME`/`PASSWORD`, and generate the two Airflow secrets:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # -> AIRFLOW__CORE__FERNET_KEY
openssl rand -hex 32                                                                          # -> AIRFLOW__API_AUTH__JWT_SECRET
```

**2. Build and initialize**

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

**3. Create the `molflow` database and tables** (one-time; the volume already exists so `init-molflow-db.sql` won't auto-run)

```bash
docker compose exec postgres psql -U airflow -c "CREATE DATABASE molflow;"

docker compose cp postgres/schema.sql postgres:/tmp/schema.sql
docker compose exec postgres psql -U airflow -d molflow -f /tmp/schema.sql

docker compose cp postgres/schema_quality_tracking.sql postgres:/tmp/schema_quality_tracking.sql
docker compose exec postgres psql -U airflow -d molflow -f /tmp/schema_quality_tracking.sql
```

**4. Set the Teams webhook** (as an Airflow Variable, not in `.env` — keeps it masked in the UI/logs)

```bash
docker compose run --rm airflow-cli airflow variables set teams_webhook_secret '<webhook URL>'
```

**5. Verify**

```bash
docker compose run --rm airflow-cli airflow connections get minio_s3
docker compose run --rm airflow-cli airflow connections get molflow_db
docker compose run --rm airflow-cli airflow variables get teams_webhook_secret
```

- Airflow UI: `http://localhost:8080`
- MinIO console: `http://localhost:9001`

## Running it

Upload a `<dataset_id>_scaffolds.csv` and `<dataset_id>_r_groups.csv` pair to the `raw-data` bucket in MinIO — each with a `smiles` column, and each scaffold/R-group SMILES having exactly one `*` attachment point. Then either wait for the weekly schedule or trigger `molflow_pipeline` manually (optionally with `{"overwrite": true}` to reprocess already-completed datasets). Results land in `processed-data` as `<dataset_id>_results.csv`.

## Notes on a few design choices

- **Heavy imports (`rdkit`, `pandas`, `sklearn`, `pandera`) are imported inside task functions, not at module level.** Airflow's DagProcessor re-parses DAG files repeatedly; importing these at the top of the file caused DagBag import timeouts.
- **`.expand()` with multiple keyword arguments computes a cartesian product, not an elementwise zip.** Wherever two independently-mapped upstream lists need pairing back up by dataset (e.g. `fetch_scaffolds` + `fetch_r_groups` before `generate`), a small join task (`zip_generate_inputs`, `zip_merge_inputs`) does that pairing by `dataset_id`, not by list position — position alone isn't safe once any instance can fail or skip.
- **`trigger_rule="all_done"`** is needed anywhere a task sits downstream of a mapped task whose instances can legitimately fail/skip — the default `all_success` rule blocks the *entire* downstream mapped group if even one sibling instance didn't succeed, not just that one dataset's branch.
- **`processed_datasets` and `quality_check_runs` are append-only logs**, not "current state" tables — they preserve history even if a results CSV gets deleted from MinIO after a scientist downloads it, and survive reprocessing/overwrite runs.
