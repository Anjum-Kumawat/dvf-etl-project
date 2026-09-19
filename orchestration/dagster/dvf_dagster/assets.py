"""RETL0-48/49/50: Dagster asset definitions for the DVF pipeline, with
department-partitioned incremental materialization (RETL0-49) and retry
policies for transient failures (RETL0-50). See sensors.py for the
failure-alerting half of RETL0-50.
"""
import datetime
import subprocess

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    RetryPolicy,
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

# RETL0-50: Bronze assets call external APIs (data.gouv.fr, BAN/IGN, ADEME
# DPE, INSEE Filosofi, geo.api.gouv.fr) and MinIO -- all genuinely
# transient failure surfaces (timeouts, rate limits, momentary network
# blips). Safe to retry automatically: every Bronze ingestion script is
# already checksum-gated (skips/re-uploads based on a sha256 comparison,
# established since the original DVF ingestion ticket), so re-running one
# after a transient failure is idempotent, not just "probably fine."
NETWORK_RETRY_POLICY = RetryPolicy(max_retries=3, delay=30)

# Silver/Gold failures are more often deterministic (a real HARD FAIL
# quality-check violation, a code bug) than transient, so retrying
# aggressively just wastes ~3 minutes re-running Spark to hit the same
# failure again. A single retry still covers genuine infra hiccups
# (Postgres/MinIO not yet reachable, a momentary JVM issue) without
# masking a real failure behind repeated identical retries. Also safe to
# retry: silver_dvf's write is DELETE-then-append per partition (RETL0-49),
# so a retry after a failed write starts from a clean slate for that
# department rather than compounding partial state.
COMPUTE_RETRY_POLICY = RetryPolicy(max_retries=1, delay=60)


def _run(cmd: list, context: AssetExecutionContext) -> None:
    context.log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.stdout:
        context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr}")


@asset(partitions_def=department_partitions, retry_policy=NETWORK_RETRY_POLICY)
def dvf_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/download_dvf.py", "--year", YEAR, "--dept", dept], context)
    _run(["python", "src/ingestion/upload_to_minio.py", "--year", YEAR, "--dept", dept], context)


@asset(partitions_def=department_partitions, deps=[dvf_bronze], retry_policy=NETWORK_RETRY_POLICY)
def ban_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/enrichment/ban_ingest.py", "--year", YEAR, "--dept", dept], context)


@asset(partitions_def=department_partitions, retry_policy=NETWORK_RETRY_POLICY)
def dpe_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/enrichment/dpe_ingest.py", "--dept", dept], context)


@asset(retry_policy=NETWORK_RETRY_POLICY)
def filosofi_bronze(context: AssetExecutionContext) -> None:
    _run(["python", "src/ingestion/enrichment/filosofi_ingest.py"], context)


@asset(partitions_def=department_partitions, retry_policy=NETWORK_RETRY_POLICY)
def geo_bronze(context: AssetExecutionContext) -> None:
    dept = context.partition_key
    _run(["python", "src/ingestion/enrichment/geo_ingest.py", "--dept", dept], context)


@asset(
    partitions_def=department_partitions,
    deps=[dvf_bronze, ban_bronze, dpe_bronze, filosofi_bronze, geo_bronze],
    retry_policy=COMPUTE_RETRY_POLICY,
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


@asset(deps=[silver_dvf], retry_policy=COMPUTE_RETRY_POLICY)
def gold_price_aggregates(context: AssetExecutionContext) -> None:
    _run(["python", "-m", "src.gold.run_gold"], context)


dvf_pipeline_job = define_asset_job(
    name="dvf_pipeline_job",
    selection=[
        dvf_bronze, ban_bronze, dpe_bronze, filosofi_bronze, geo_bronze,
        silver_dvf, gold_price_aggregates,
    ],
)


@schedule(
    job=dvf_pipeline_job,
    cron_schedule="0 6 1 4,10 *",
    default_status=DefaultScheduleStatus.STOPPED,
)
def dvf_publication_schedule(context):
    for dept in DEPARTMENTS:
        yield RunRequest(partition_key=dept)