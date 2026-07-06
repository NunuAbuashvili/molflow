"""
Data quality checks for molflow's merged results, using Pandera.
"""
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, Check, DataFrameSchema


RESULTS_SCHEMA = DataFrameSchema(
    {
        "smiles": Column(
            str,
            [
                Check(lambda series: series.str.len() > 0,
                error="smiles must not be empty"),
                Check.str_matches(r"^[A-Za-z0-9@+\-\[\]()=#$:./\\%]+$"),
            ],
            unique=True,
            nullable=False
        ),
        "cluster": Column(int, Check.ge(0), nullable=False),
        "mol_weight": Column(float, Check.gt(0), nullable=False),
        "log_p": Column(float, nullable=False),
        "tpsa": Column(float, Check.ge(0), nullable=False),
        "hba": Column(int, Check.ge(0), nullable=False),
        "hbd": Column(int, Check.ge(0), nullable=False),
        "rotatable_bonds": Column(int, Check.ge(0), nullable=False),
        "aromatic_rings": Column(int, Check.ge(0), nullable=False),
        "lipinski_pass": Column(bool, nullable=False)
    },
    checks=[
        Check(
            lambda df: df["lipinski_pass"] == (
                (df["mol_weight"] <= 500)
                & (df["log_p"] <= 5)
                & (df["hba"] <= 10)
                & (df["hbd"] <= 5)
            ),
            error="lipinski_pass inconsistent with mol_weight/log_p/hba/hbd",
            name="lipinski_consistency",
        ),
    ],
    strict=True,  # No unexpected extra columns
)


def validate_results(merged: list[dict]) -> tuple[bool, list[dict]]:
    """
    Validate merged rows (the same list merge_and_upload builds and writes
    to CSV) against RESULTS_SCHEMA.

    Returns:
        (passed, failures)
        - passed: True if every check passed.
        - failures: list of dicts, one per violation, shaped as
          {"check": <str>, "detail": <str>}, one per violation
          — plugs directly into notify_teams_quality_issue's
          failed_checks parameter.
    """
    df = pd.DataFrame(merged)

    try:
        RESULTS_SCHEMA.validate(df, lazy=True)
        return True, []
    except pa.errors.SchemaErrors as exc:
        failures = []

        for row in exc.failure_cases.to_dict(orient="records"):
            column = row.get("column")
            check_label = row["check"] if pd.isna(column) else f"{column}.{row['check']}"
            failures.append({
                "check": check_label,
                "detail": f"row {row['index']}: {row['failure_case']!r}",
            })
        return False, failures
