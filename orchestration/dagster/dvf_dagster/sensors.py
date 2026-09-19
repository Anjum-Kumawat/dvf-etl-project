"""RETL0-50: Dagster failure alerting.

Alert channel decision: this project has no Slack webhook or email/SMTP
credentials configured anywhere, and RETL0-54 requires the whole repo to
run on a machine that's never seen the project via `docker compose up`
alone -- introducing a third-party alert channel would mean either
fabricating credentials (not possible) or adding a setup step the README
would need to document just for this ticket. So alerting here is
Dagster-native: on any dvf_pipeline_job run failure, pipeline_failure_sensor
below writes a structured record (run id, job name, failed step, error
message, timestamp) to a `pipeline_alerts` table in the same Postgres
instance every other layer already uses, and logs it to the run's own
event log for visibility in the Dagster UI. No new infrastructure,
containers, or secrets required -- reproducible on a fresh clone exactly
like everything else.

default_status=DefaultSensorStatus.RUNNING so the sensor is live as soon
as the Dagster webserver starts, the same way retries in assets.py apply
automatically -- neither requires remembering to flip a toggle in the UI.
"""
import os

import psycopg2
from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "dvf")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "dvf")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "dvf")

CREATE_ALERTS_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    failed_step TEXT,
    error_message TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _write_alert(run_id: str, job_name: str, failed_step: str, error_message: str) -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_ALERTS_TABLE)
            cur.execute(
                "INSERT INTO pipeline_alerts (run_id, job_name, failed_step, error_message) "
                "VALUES (%s, %s, %s, %s)",
                (run_id, job_name, failed_step, error_message),
            )
        conn.commit()
    finally:
        conn.close()


def _extract_failure_info(context: RunStatusSensorContext, run_id: str):
    """Best-effort extraction of the failed step and error message via
    instance.all_logs(), the lower-level event-log query documented across
    Dagster versions for this exact purpose. Wrapped defensively: an
    earlier version of this function called
    context.get_step_failure_events(), which turned out not to exist on
    RunStatusSensorContext in this Dagster version (1.9.9) -- confirmed via
    a real AttributeError in the daemon logs that silently killed every
    sensor tick for a failed run without ever writing an alert. If this
    extraction ever breaks again on a version detail, the alert should
    still be written with a generic message rather than the sensor
    crashing and alerting nobody."""
    try:
        step_failure_logs = context.instance.all_logs(run_id, of_type=DagsterEventType.STEP_FAILURE)
        if step_failure_logs:
            event = step_failure_logs[0].dagster_event
            failed_step = event.step_key or "unknown"
            error_info = event.event_specific_data.error if event.event_specific_data else None
            error_message = (error_info.message if error_info else "")[:2000]
            return failed_step, error_message
    except Exception as exc:
        return "unknown", f"Could not extract step failure details: {exc}"
    return "unknown", "Run failed with no step failure events (e.g. a run worker crash)."


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    default_status=DefaultSensorStatus.RUNNING,
)
def pipeline_failure_sensor(context: RunStatusSensorContext):
    """Fires on ANY run failure in this code location (including after
    retries are exhausted) -- not scoped to dvf_pipeline_job specifically.
    That matters in practice: materializing assets via the Dagster UI's
    "Materialize selected" button (how nearly every materialization in
    this project has actually been run) launches an ad-hoc implicit job
    (__ASSET_JOB), not dvf_pipeline_job. An earlier version of this sensor
    was scoped to monitored_jobs=[dvf_pipeline_job] and, when tested
    against a real induced failure, never fired -- confirmed via the
    run's own log showing job_name="__ASSET_JOB". Left unscoped here so a
    failure is caught regardless of how the run was launched. Writes a
    row to pipeline_alerts and logs an error-level message so a failure
    is visible in the Dagster UI without needing an external alert
    channel."""
    run_id = context.dagster_run.run_id
    job_name = context.dagster_run.job_name

    failed_step, error_message = _extract_failure_info(context, run_id)

    context.log.error(
        f"Pipeline failure alert: run={run_id} job={job_name} "
        f"step={failed_step} error={error_message}"
    )

    _write_alert(run_id, job_name, failed_step, error_message)