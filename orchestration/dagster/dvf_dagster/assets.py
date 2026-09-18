"""RETL0-48: Dagster asset definitions for the DVF pipeline.

Each asset shells out to the exact same CLI entrypoint already built and
tested (src/ingestion/*.py, src/silver/run_dvf_silver.py,
src/gold/run_gold.py) rather than reimplementing any pipeline logic --
Dagster orchestrates the existing, already-verified scripts.

None of these assets return real data through Dagster's IO manager --
they only produce side effects (files in MinIO, rows in Postgres).
Dependencies are therefore declared via `deps=[...]` on the decorator,
not as function parameters: a function parameter would tell Dagster to
load the upstream asset's materialized *value*, which doesn't exist
here since these assets return None.

Dependencies: silver_dvf depends on all 5 Bronze assets (it reads from all
of them); gold_price_aggregates depends on silver_dvf (it reads only from
Silver). Bronze sources are otherwise independent of each other.
"""
import datetime
import subprocess

from dagster import AssetExecutionContext, asset

PROJECT_ROOT = "/opt/dvf-etl-project"
YEAR = "2024"
DEPT = "75"
PUBLICATION = "2026-04"


def _run(cmd: list, context: AssetExecutionContext) -> None:
    context.log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.stdout:
        context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr}")


@asset
def dvf_bronze(context: AssetExecutionContext) -> None:
    _run(["python", "src/ingestion/download_dvf.py", "--year", YEAR, "--dept", DEPT], context)
    _run(["python", "src/ingestion/upload_to_minio.py", "--year", YEAR, "--dept", DEPT], context)


@asset(deps=[dvf_bronze])
def ban_bronze(context: AssetExecutionContext) -> None:
    _run(["python", "src/ingestion/enrichment/ban_ingest.py", "--year", YEAR, "--dept", DEPT], context)


@asset
def dpe_bronze(context: AssetExecutionContext) -> None:
    _run(["python", "src/ingestion/enrichment/dpe_ingest.py", "--dept", DEPT], context)


@asset
def filosofi_bronze(context: AssetExecutionContext) -> None:
    _run(["python", "src/ingestion/enrichment/filosofi_ingest.py"], context)


@asset
def geo_bronze(context: AssetExecutionContext) -> None:
    _run(["python", "src/ingestion/enrichment/geo_ingest.py", "--dept", DEPT], context)


@asset(deps=[dvf_bronze, ban_bronze, dpe_bronze, filosofi_bronze, geo_bronze])
def silver_dvf(context: AssetExecutionContext) -> None:
    today = datetime.date.today().isoformat()
    _run(
        [
            "python", "-m", "src.silver.run_dvf_silver",
            "--year", YEAR, "--dept", DEPT, "--publication", PUBLICATION,
            "--dpe-extraction-date", today, "--geo-extraction-date", today,
        ],
        context,
    )


@asset(deps=[silver_dvf])
def gold_price_aggregates(context: AssetExecutionContext) -> None:
    _run(["python", "-m", "src.gold.run_gold"], context)