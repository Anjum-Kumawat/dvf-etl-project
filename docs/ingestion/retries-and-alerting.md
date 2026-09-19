# Dagster Retries & Failure Alerting (RETL0-50)

**Ticket:** RETL0-50 · **Prerequisite:** RETL0-48

## Decision: Dagster-native, no third-party alert channel

RETL0-50's own ticket text doesn't specify an alert channel (unlike most
other tickets in this project, which include a Need/AC/Evidence triple).
This project has no Slack webhook or email/SMTP credentials configured
anywhere, and RETL0-54 requires the whole repo to run on a machine that's
never seen the project via `docker compose up` alone, following only the
README. Adding a Slack or email integration here would mean either
fabricating credentials (not possible) or documenting a manual setup step
just for this one ticket -- so alerting is implemented as Dagster-native:
retries handle transient failures automatically, and a failure sensor
writes a structured record to Postgres (a `pipeline_alerts` table) and
logs it, both requiring no new infrastructure or secrets.

## Retries

Two `RetryPolicy` tiers, both defined in `orchestration/dagster/dvf_dagster/assets.py`:

- **`NETWORK_RETRY_POLICY`** (`max_retries=3, delay=30`) -- applied to all
  five Bronze assets (`dvf_bronze`, `ban_bronze`, `dpe_bronze`,
  `filosofi_bronze`, `geo_bronze`). Each calls an external API
  (data.gouv.fr, BAN/IGN, ADEME, INSEE, geo.api.gouv.fr) or MinIO -- real
  transient failure surfaces (timeouts, rate limits, momentary network
  blips). Safe to retry: every Bronze ingestion script is already
  checksum-gated (skips or re-uploads based on a sha256 comparison,
  established since the original DVF ingestion ticket), so re-running one
  after a transient failure doesn't duplicate or corrupt anything.
- **`COMPUTE_RETRY_POLICY`** (`max_retries=1, delay=60`) -- applied to
  `silver_dvf` and `gold_price_aggregates`. These fail more often for
  deterministic reasons (a real HARD FAIL quality-check violation, a code
  bug) than transient ones, so retrying aggressively just wastes ~3
  minutes re-running Spark to hit the same failure again. One retry still
  covers a genuine infra hiccup (Postgres/MinIO not yet reachable) without
  masking a real failure behind repeated identical retries. Also safe to
  retry: `silver_dvf`'s write is DELETE-then-append per department
  partition (RETL0-49), so a retry after a failed write starts from a
  clean slate for that partition rather than compounding partial state.

## Failure alerting

`orchestration/dagster/dvf_dagster/sensors.py` defines
`pipeline_failure_sensor`, a `run_status_sensor` watching
`dvf_pipeline_job` for `DagsterRunStatus.FAILURE` (fires after retries are
exhausted, not on each individual retry attempt). On failure, it:

1. Extracts the failed step and error message from the run's own
   step-failure events.
2. Logs an error-level message to the run's event log, visible in the
   Dagster UI's run view.
3. Writes a row (run id, job name, failed step, error message, timestamp)
   to a `pipeline_alerts` table in the `dvf` Postgres database, creating
   the table on first use if it doesn't exist yet.

The sensor has `default_status=DefaultSensorStatus.RUNNING`, so it's live
as soon as the Dagster webserver starts -- no manual toggle needed in the
UI, the same way the retry policies above apply automatically.

## Querying alerts

```sql
SELECT run_id, job_name, failed_step, occurred_at, left(error_message, 200)
FROM pipeline_alerts
ORDER BY occurred_at DESC;
```

## Verification

TODO: verify against a real failure, not just code review. Plan: with
`silver_dvf` and `gold_price_aggregates` selected for re-materialization,
stop the MinIO container mid-run (`docker stop dvf-etl-project-minio-1`)
to cause a genuine S3A connection failure when Spark tries to read Bronze
data -- this happens before `silver_dvf`'s Postgres DELETE, so no data for
the department being materialized is at risk. Confirm the run retries
once per `COMPUTE_RETRY_POLICY`, fails again with MinIO still down, and is
marked FAILED; confirm `pipeline_failure_sensor` fires and writes a row to
`pipeline_alerts` with the real error message; restart MinIO and confirm
via `psql` that the department's existing Silver row count is unaffected
by the failed attempt. Replace this section with the real results once
run.