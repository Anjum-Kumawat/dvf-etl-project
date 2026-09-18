"""RETL0-48/49: Dagster asset definitions for the DVF pipeline, with
department-partitioned incremental materialization for RETL0-49.

Each asset shells out to the exact same CLI entrypoint already built and
tested (src/ingestion/*.py, src/silver/run_dvf_silver.py,
src/gold/run_gold.py) rather than reimplementing any pipeline logic --
Dagster orchestrates the existing, already-verified scripts.

None of these assets return real data through Dagster's IO manager --
they only produce side effects (files in MinIO, rows in Postgres).
Dependencies are therefore declared via `deps=[...]` on the decorator,
not as function parameters.

Incremental strategy (RETL0-49): partitioned by department (75/92/93/94,
matching bulk_ingest.py's real existing scope and the department axis of
the Bronze object-key structure documented in
docs/ingestion/bronze-object-paths.md). Each department is independently
materializable; Dagster tracks per-partition materialization history, so
re-running one department does not require touching the others. Year is
NOT a second partition dimension in this iteration -- see
docs/ingestion/incremental-ingestion-strategy.md for the full rationale
and the known limitation this leaves.

filosofi_bronze stays unpartitioned: it is a single national file with no
department axis in the source itself. gold_price_aggregates stays
unpartitioned: it aggregates the entire Silver table regardless of which
department partition triggered the run.
"""
import datetime
import subprocess

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    schedule,
)

PROJECT_ROOT = "/opt/dvf-etl-project"
YEAR = "2024"
PUBLICATION = "2026-04"

DEPARTMENTS = ["75", "92", "93", "94"]
department_partitions = StaticPartitionsDefinition(DEPARTMENTS)


def _run(cmd: list, context: AssetExecutionContext) -> None:
    context.log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.stdout:
        context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr}")


@asset(partitions_def=department_partitions)
def dvf_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/download_dvf.py", "--year", YEAR, "--dept", dept], context)
    _run(["python", "src/ingestion/upload_to_minio.py", "--year", YEAR, "--dept", dept], context)


@asset(partitions_def=department_partitions, deps=[dvf_bronze])
def ban_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/enrichment/ban_ingest.py", "--year", YEAR, "--dept", dept], context)


@asset(partitions_def=department_partitions)
def dpe_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/enrichment/dpe_ingest.py", "--dept", dept], context)


@asset
def filosofi_bronze(context: AssetExecutionContext) -> None:
    _run(["python", "src/ingestion/enrichment/filosofi_ingest.py"], context)


@asset(partitions_def=department_partitions)
def geo_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/enrichment/geo_ingest.py", "--dept", dept], context)


@asset(
    partitions_def=department_partitions,
    deps=[dvf_bronze, ban_bronze, dpe_bronze, filosofi_bronze, geo_bronze],
)
def silver_dvf(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    today = datetime.date.today().isoformat()
    _run(
        [
            "python", "-m", "src.silver.run_dvf_silver",
            "--year", YEAR, "--dept", dept, "--publication", PUBLICATION,
            "--dpe-extraction-date", today, "--geo-extraction-date", today,
        ],
        context,
    )


@asset(deps=[silver_dvf])
def gold_price_aggregates(context: AssetExecutionContext) -> None:
    _run(["python", "-m", "src.gold.run_gold"], context)


dvf_pipeline_job = define_asset_job(
    name="dvf_pipeline_job",
    selection=[
        dvf_bronze,
        ban_bronze,
        dpe_bronze,
        filosofi_bronze,
        geo_bronze,
        silver_dvf,
        gold_price_aggregates,
    ],
)


@schedule(
    job=dvf_pipeline_job,
    cron_schedule="0 6 1 4,10 *",  # 06:00 on 1 April and 1 October -- DVF's real
    # half-yearly publication cadence (see docs/ingestion/bronze-dvf-procedure.md)
    default_status=DefaultScheduleStatus.STOPPED,
)
def dvf_publication_schedule(context):
    """Materializes every department partition for the current publication.

    Stopped by default so it doesn't fire against the real data.gouv.fr
    endpoint unattended in a dev/demo environment -- turn on from the
    Dagster UI's Automation tab, or trigger a one-off run manually to
    demonstrate it.
    """
    for dept in DEPARTMENTS:
        yield RunRequest(partition_key=dept)