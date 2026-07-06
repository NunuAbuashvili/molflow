"""
Molflow cheminformatics pipeline.

Iteration 2: weekly schedule. Discovers dataset ids present in the MinIO
`raw-data` bucket, filters out ones already successfully processed
(tracked in the `molflow` Postgres database's processed_datasets table,
unless overwrite=True), then processes each new dataset via dynamic task
mapping, with each pipeline step as its own mapped task:
    fetch_scaffolds \\
                      >- generate -> properties \\
    fetch_r_groups   /              -> cluster  >- merge_and_upload

Each dataset's processing attempt is recorded in processed_datasets
(success or failed, with error_message noting which step failed).
"""
import csv
import io
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Callable

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task, Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

S3_CONN_ID = "minio_s3"
POSTGRES_CONN_ID = "molflow_db"
RAW_BUCKET = "raw-data"
PROCESSED_BUCKET = "processed-data"
NUM_CLUSTERS = 5


_REQUIRED_SUFFIXES = {
    "_scaffolds.csv": "scaffolds",
    "_r_groups.csv": "r_groups"
}


def _discover_complete_dataset_ids(keys: list[str]) -> list[str]:
    """Return dataset ids that have both required files."""
    found: dict[str, set[str]] = {}

    for key in keys:
        for suffix, part in _REQUIRED_SUFFIXES.items():
            if key.endswith(suffix):
                found.setdefault(key.removesuffix(suffix), set()).add(part)
                break

    complete_ids = []
    for dataset_id in sorted(found):
        parts = found[dataset_id]
        if parts == set(_REQUIRED_SUFFIXES.values()):
            complete_ids.append(dataset_id)
        else:
            missing = set(_REQUIRED_SUFFIXES.values()) - parts
            logger.warning(
                "Skipping dataset_id '%s' — missing %s file(s).",
                dataset_id, ", ".join(sorted(missing)),
            )

    return complete_ids


def _read_smiles_from_s3(
    hook: S3Hook,
    bucket: str,
    key: str
) -> list[str]:
    """Download a CSV key from S3/MinIO and extract its 'smiles' column."""
    content = hook.read_key(key=key, bucket_name=bucket)
    reader = csv.DictReader(io.StringIO(content))

    if "smiles" not in (reader.fieldnames or []):
        raise ValueError(
            f"'smiles' column not found in {bucket}/{key}. "
            f"Found columns: {reader.fieldnames}"
        )

    return [row["smiles"].strip() for row in reader if row["smiles"].strip()]


def _merge_and_upload(
    hook: S3Hook,
    dataset_id: str,
    properties: list[dict],
    clusters: list[dict]
) -> tuple[str, list[dict]]:
    """
    Merge properties and cluster assignments by SMILES,
    write to a CSV file, and upload it to MinIO.
    """
    cluster_by_smiles = {row["smiles"]: row["cluster"] for row in clusters}
    merged = [
        {**prop_row, "cluster": cluster_by_smiles.get(prop_row["smiles"])}
        for prop_row in properties
    ]

    if not merged:
        raise ValueError("Nothing to write — properties list is empty.")

    merged.sort(key=lambda row: (row["cluster"], row["smiles"]))

    first_row = merged[0]
    ordered_fields = ["smiles", "cluster"] + [
        k for k in first_row if k not in ("smiles", "cluster")
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=ordered_fields)
    writer.writeheader()
    writer.writerows(merged)

    key = f"{dataset_id}_results.csv"
    hook.load_string(
        string_data=buffer.getvalue(),
        key=key,
        bucket_name=PROCESSED_BUCKET,
        replace=True,
    )

    return f"{PROCESSED_BUCKET}/{key}", merged


def _get_successfully_processed_dataset_ids() -> set[str]:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    records = hook.get_records(
        "SELECT DISTINCT dataset_id "
        "FROM processed_datasets "
        "WHERE status = 'success'"
    )
    return {row[0] for row in records}


def _record_processing_result(
    dataset_id: str,
    dag_run_id: str,
    status: str,
    molecules_generated: int | None,
    error_message: str | None,
    overwrite: bool,
    started_at: datetime,
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO processed_datasets
            (
                dataset_id, dag_run_id, status, 
                molecules_generated, error_message, 
                overwrite, started_at
            )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            dataset_id,
            dag_run_id,
            status,
            molecules_generated,
            error_message,
            overwrite,
            started_at,
        ),
    )


def _run_step(
    dataset_id: str,
    dag_run_id: str,
    overwrite: bool,
    step_name: str,
    fn: Callable[..., Any],
    *args,
    **kwargs,
) -> Any:
    """
    Run fn(*args, **kwargs); on failure, record it against dataset_id,
    then raise AirflowSkipException.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        error_message = f"[{step_name}] {e}"
        _record_processing_result(
            dataset_id=dataset_id,
            dag_run_id=dag_run_id,
            status="failed",
            molecules_generated=None,
            error_message=error_message[:2000],
            overwrite=overwrite,
            started_at=datetime.now(timezone.utc),
        )
        raise AirflowSkipException(error_message) from e


@dag(
    dag_id="molflow_pipeline",
    description="molflow cheminformatics pipeline — "
                "weekly, auto-discovers new datasets",
    schedule="@weekly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    params={
        "overwrite": Param(
            default=False,
            type="boolean",
            description="If true, reprocess datasets even if "
                        "already successfully processed.",
        ),
    },
    tags=["molflow", "cheminformatics"],
)
def molflow_pipeline():

    @task
    def discover_new_datasets(**context) -> list[str]:
        """
        List raw-data, find complete dataset pairs,
        drop already-processed ones.
        """
        overwrite = context["params"]["overwrite"]

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        keys = hook.list_keys(bucket_name=RAW_BUCKET) or []
        complete_ids = _discover_complete_dataset_ids(keys)

        if overwrite:
            logger.info(
                "overwrite=True — processing all %d discovered dataset(s).",
                len(complete_ids)
            )
            return complete_ids

        already_processed = _get_successfully_processed_dataset_ids()
        new_ids = [d for d in complete_ids if d not in already_processed]

        logger.info(
            "%d discovered, %d already processed, %d to process this run.",
            len(complete_ids),
            len(complete_ids) - len(new_ids),
            len(new_ids),
        )
        return new_ids

    @task
    def fetch_scaffolds(dataset_id: str, **context) -> dict:
        dag_run_id = context["dag_run"].run_id
        overwrite = context["params"]["overwrite"]
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        smiles = _run_step(
            dataset_id, dag_run_id, overwrite, "fetch_scaffolds",
            _read_smiles_from_s3, hook, RAW_BUCKET, f"{dataset_id}_scaffolds.csv",
        )
        return {"dataset_id": dataset_id, "smiles": smiles}

    @task
    def fetch_r_groups(dataset_id: str, **context) -> dict:
        dag_run_id = context["dag_run"].run_id
        overwrite = context["params"]["overwrite"]
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        smiles = _run_step(
            dataset_id, dag_run_id, overwrite, "fetch_r_groups",
            _read_smiles_from_s3, hook, RAW_BUCKET, f"{dataset_id}_r_groups.csv",
        )
        return {"dataset_id": dataset_id, "smiles": smiles}

    @task(trigger_rule="all_done")
    def zip_generate_inputs(
        scaffold_bundles: list[dict],
        r_group_bundles: list[dict]
    ) -> list[dict]:
        """
        Pair up fetch_scaffolds/fetch_r_groups outputs by dataset_id.
        trigger_rule=all_done lets this run even if some fetch_*
        instances failed.
        """
        scaffolds_by_id = {b["dataset_id"]: b["smiles"] for b in scaffold_bundles}
        r_groups_by_id = {b["dataset_id"]: b["smiles"] for b in r_group_bundles}

        ready_ids = sorted(scaffolds_by_id.keys() & r_groups_by_id.keys())
        incomplete_ids = sorted((scaffolds_by_id.keys() | r_groups_by_id.keys()) - set(ready_ids))

        for dataset_id in incomplete_ids:
            logger.warning(
                "Dataset '%s' won't proceed to generate — fetch_scaffolds and/or "
                "fetch_r_groups failed for it (see its 'failed' row in processed_datasets).",
                dataset_id,
            )

        return [
            {
                "dataset_id": d,
                "scaffold_smiles": scaffolds_by_id[d],
                "r_group_smiles": r_groups_by_id[d],
            }
            for d in ready_ids
        ]

    @task
    def generate(
        dataset_id: str,
        scaffold_smiles: list[str],
        r_group_smiles: list[str],
        **context
    ) -> dict:
        from include.generation import generate_molecules

        dag_run_id = context["dag_run"].run_id
        overwrite = context["params"]["overwrite"]
        molecules = _run_step(
            dataset_id, dag_run_id, overwrite, "generate",
            generate_molecules, scaffold_smiles, r_group_smiles,
        )
        return {"dataset_id": dataset_id, "molecules": molecules}

    @task(trigger_rule="all_done")
    def properties(bundle: dict, **context) -> dict:
        from include.properties import calculate_properties

        if not bundle or "dataset_id" not in bundle:
            raise AirflowSkipException(
                "No bundle from generate — its upstream was skipped or failed."
            )

        dataset_id = bundle["dataset_id"]
        dag_run_id = context["dag_run"].run_id
        overwrite = context["params"]["overwrite"]
        props = _run_step(
            dataset_id, dag_run_id, overwrite, "properties",
            calculate_properties, bundle["molecules"],
        )
        return {"dataset_id": dataset_id, "properties": props}

    @task(trigger_rule="all_done")
    def cluster(bundle: dict, **context) -> dict:
        from include.clustering import cluster_molecules

        if not bundle or "dataset_id" not in bundle:
            raise AirflowSkipException(
                "No bundle from generate — its upstream was skipped or failed."
            )

        dataset_id = bundle["dataset_id"]
        dag_run_id = context["dag_run"].run_id
        overwrite = context["params"]["overwrite"]
        clusters = _run_step(
            dataset_id, dag_run_id, overwrite, "cluster",
            cluster_molecules, bundle["molecules"], num_clusters=NUM_CLUSTERS,
        )
        return {"dataset_id": dataset_id, "clusters": clusters}

    @task(trigger_rule="all_done")
    def zip_merge_inputs(
        props_list: list[dict],
        clusters_list: list[dict]
    ) -> list[dict]:
        """Pair up properties/cluster outputs by dataset_id."""
        props_by_id = {p["dataset_id"]: p["properties"] for p in props_list}
        clusters_by_id = {c["dataset_id"]: c["clusters"] for c in clusters_list}

        ready_ids = sorted(props_by_id.keys() & clusters_by_id.keys())
        incomplete_ids = sorted((props_by_id.keys() | clusters_by_id.keys()) - set(ready_ids))

        for dataset_id in incomplete_ids:
            logger.warning(
                "Dataset '%s' won't be merged — properties and/or cluster step "
                "failed for it (see its 'failed' row in processed_datasets).",
                dataset_id,
            )

        return [
            {"dataset_id": d, "properties": props_by_id[d], "clusters": clusters_by_id[d]}
            for d in ready_ids
        ]

    @task
    def merge_and_upload(
        dataset_id: str,
        properties: list[dict],
        clusters: list[dict],
        **context
    ) -> str:
        dag_run_id = context["dag_run"].run_id
        overwrite = context["params"]["overwrite"]
        started_at = datetime.now(timezone.utc)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        try:
            result_path, merged = _merge_and_upload(
                s3_hook,
                dataset_id,
                properties,
                clusters
            )
            row_count = len(merged)

            _record_processing_result(
                dataset_id=dataset_id,
                dag_run_id=dag_run_id,
                status="success",
                molecules_generated=row_count,
                error_message=None,
                overwrite=overwrite,
                started_at=started_at,
            )
            logger.info(
                "Processed '%s' -> %s (%d molecules).",
                dataset_id, result_path, row_count
            )
            return {
                "dataset_id": dataset_id,
                "result_path": result_path,
                "merged": merged
            }
        except Exception as e:
            error_message = f"[merge_and_upload] {e}"
            _record_processing_result(
                dataset_id=dataset_id,
                dag_run_id=dag_run_id,
                status="failed",
                molecules_generated=None,
                error_message=error_message[:2000],
                overwrite=overwrite,
                started_at=started_at,
            )
            raise AirflowSkipException(error_message) from e

    @task(trigger_rule="all_done")
    def run_quality_checks(bundle: dict, **context) -> dict:
        """
        Validate one dataset's merged results with Pandera
        and record the outcome.
        """
        from include.quality_checks import validate_results

        if not bundle or "dataset_id" not in bundle:
            raise AirflowSkipException(
                "No bundle from merge_and_upload — its upstream was skipped or failed."
            )

        dataset_id = bundle["dataset_id"]
        dag_run_id = context["dag_run"].run_id
        passed, failures = validate_results(bundle["merged"])

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(
            """
            INSERT INTO quality_check_runs (dataset_id, dag_run_id, passed, failed_check_count)
            VALUES (%s, %s, %s, %s)
            """,
            parameters=(dataset_id, dag_run_id, passed, len(failures)),
        )

        if passed:
            logger.info("All quality checks passed for '%s'.", dataset_id)
        else:
            logger.warning("Quality issues for '%s': %s", dataset_id, failures)

        return {"dataset_id": dataset_id, "passed": passed}

    @task(trigger_rule="all_done")
    def notify_run_summary(_quality_results: list, **context) -> None:
        """
        Aggregates processed_datasets and quality_check_runs for a single
        dag_run_id and sends one summary card.
        """
        from airflow.models import Variable

        dag_run_id = context["dag_run"].run_id
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        processing_rows = pg_hook.get_records(
            """
            SELECT dataset_id, status, error_message
            FROM processed_datasets
            WHERE dag_run_id = %s
            ORDER BY dataset_id
            """,
            parameters=(dag_run_id,),
        )

        succeeded = [r for r in processing_rows if r[1] == "success"]
        failed = [r for r in processing_rows if r[1] == "failed"]

        quality_rows = pg_hook.get_records(
            """
            SELECT dataset_id, passed, failed_check_count
            FROM quality_check_runs
            WHERE dag_run_id = %s
            ORDER BY dataset_id
            """,
            parameters=(dag_run_id,),
        )
        quality_passed = [r for r in quality_rows if r[1]]
        quality_failed = [r for r in quality_rows if not r[1]]

        facts = [
            {"title": "Processed successfully", "value": str(len(succeeded))},
            {"title": "Processing failed", "value": str(len(failed))},
            {"title": "Quality checks passed", "value": str(len(quality_passed))},
            {"title": "Quality checks failed", "value": str(len(quality_failed))},
        ]

        body = [
            {
                "type": "TextBlock",
                "text": f"molflow run summary — {dag_run_id}",
                "wrap": True,
                "weight": "Bolder",
                "size": "Medium",
            },
            {"type": "FactSet", "facts": facts},
        ]

        if failed:
            failure_lines = "\n".join(f"- **{ds}**: {err}" for ds, _, err in failed)
            body.append({
                "type": "TextBlock",
                "text": f"**Processing failures:**\n{failure_lines}",
                "wrap": True,
            })

        if quality_failed:
            quality_lines = "\n".join(
                f"- **{ds}**: {count} check(s) failed" for ds, _, count in quality_failed
            )
            body.append({
                "type": "TextBlock",
                "text": f"**Quality issues:**\n{quality_lines}",
                "wrap": True,
            })

        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": body,
                    },
                }
            ],
        }
        from include.notifications import post_to_teams

        post_to_teams(Variable.get("teams_webhook_secret"), payload)
        logger.info(
            "Run summary: %d succeeded, %d failed, %d quality-passed, %d quality-failed.",
            len(succeeded), len(failed), len(quality_passed), len(quality_failed),
        )

    new_dataset_ids = discover_new_datasets()

    scaffolds = fetch_scaffolds.expand(dataset_id=new_dataset_ids)
    r_groups = fetch_r_groups.expand(dataset_id=new_dataset_ids)

    generate_inputs = zip_generate_inputs(scaffolds, r_groups)
    gen_output = generate.expand_kwargs(generate_inputs)

    props_output = properties.expand(bundle=gen_output)
    clusters_output = cluster.expand(bundle=gen_output)

    merge_inputs = zip_merge_inputs(props_output, clusters_output)
    merge_output = merge_and_upload.expand_kwargs(merge_inputs)
    quality_output = run_quality_checks.expand(bundle=merge_output)
    notify_run_summary(quality_output)


molflow_pipeline()
