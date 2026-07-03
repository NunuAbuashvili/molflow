"""
Molflow cheminformatics pipeline.

Currently (iteration 1): manually triggered, takes a `dataset_id` parameter,
reads the matching <dataset_id>_scaffolds.csv and <dataset_id>_r_groups.csv
from the MinIO `raw_data` bucket, runs generate -> properties -> cluster,
merges the results, and uploads <dataset_id>_results.csv to `processed-data`.

Intermediate data is passed via XCom (small-data assumption for this
iteration). A later iteration will move intermediate data to MinIO and
add a weekly schedule + overwrite parameter.
"""
import csv
import io
import logging
import sys
from datetime import datetime

from airflow.sdk import dag, task, Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

S3_CONN_ID = "minio_s3"
RAW_BUCKET = "raw-data"
PROCESSED_BUCKET = "processed-data"
NUM_CLUSTERS = 5


def _read_smiles_from_s3(
    hook: S3Hook,
    bucket: str,
    key: str
) -> list[str]:
    """Download a CSV key from S3/MinIO and extract its 'smiles' column."""
    if not hook.check_for_key(key, bucket_name=bucket):
        raise FileNotFoundError(
            f"Expected '{key}' in bucket '{bucket}' but it wasn't found. "
            f"Check that dataset_id is correct and both files were uploaded."
        )

    content = hook.read_key(key=key, bucket_name=bucket)
    reader = csv.DictReader(io.StringIO(content))

    if "smiles" not in (reader.fieldnames or []):
        raise ValueError(
            f"'smiles' column not found in {bucket}/{key}. "
            f"Found columns: {reader.fieldnames}"
        )

    return [row["smiles"].strip() for row in reader if row["smiles"].strip()]


@dag(
    dag_id="molflow_pipeline",
    description="Molflow cheminformatics pipeline — "
                "generate, calculate properties, cluster",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    params={
        "dataset_id": Param(
            default=None,
            type="string",
            description="Dataset id - expects <dataset_id>_scaffolds.csv and "
                        "<dataset_id>_r_groups.csv in the raw-data bucket.",
        ),
    },
    tags=["molflow", "cheminformatics"],
)
def molflow_pipeline():
    @task
    def get_dataset_id(**context) -> str:
        dataset_id = context["params"].get("dataset_id")
        if not dataset_id:
            raise ValueError(
                "dataset_id is required. Trigger this DAG with config "
                '{"dataset_id": "<id>"} — expects <id>_scaffolds.csv and '
                "<id>_r_groups.csv in the raw-data bucket."
            )
        return dataset_id

    @task
    def fetch_scaffolds(dataset_id: str) -> list[str]:
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        return _read_smiles_from_s3(
            hook,
            RAW_BUCKET,
            f"{dataset_id}_scaffolds.csv"
        )

    @task
    def fetch_r_groups(dataset_id: str) -> list[str]:
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        return _read_smiles_from_s3(
            hook,
            RAW_BUCKET,
            f"{dataset_id}_r_groups.csv"
        )

    @task
    def generate(
        scaffold_smiles: list[str],
        r_group_smiles: list[str]
    ) -> list[str]:
        from include.generation import generate_molecules

        return generate_molecules(scaffold_smiles, r_group_smiles)

    @task
    def properties(molecules: list[str]) -> list[dict]:
        from include.properties import calculate_properties

        return calculate_properties(molecules)

    @task
    def cluster(molecules: list[str]) -> list[dict]:
        from include.clustering import cluster_molecules

        return cluster_molecules(molecules, num_clusters=NUM_CLUSTERS)

    @task
    def merge_and_upload(
        dataset_id: str,
        properties: list[dict],
        clusters: list[dict],
        **context
    ) -> str:
        """
        Merge properties and cluster assignments by SMILES, write CSV,
        and upload to MinIO.
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
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        hook.load_string(
            string_data=buffer.getvalue(),
            key=key,
            bucket_name=PROCESSED_BUCKET,
            replace=True,
        )

        result_path = f"{PROCESSED_BUCKET}/{key}"
        logger.info(
            "Uploaded %d rows to %s",
            len(merged), result_path
        )
        return result_path

    dataset_id = get_dataset_id()
    scaffolds = fetch_scaffolds(dataset_id)
    r_groups = fetch_r_groups(dataset_id)
    molecules = generate(scaffolds, r_groups)

    props = properties(molecules)
    clusters = cluster(molecules)
    merge_and_upload(dataset_id, props, clusters)


molflow_pipeline()
