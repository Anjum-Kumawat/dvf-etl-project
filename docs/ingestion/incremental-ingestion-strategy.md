# Incremental Ingestion Strategy

RETL0-49 asked to wire in the incremental-ingestion strategy documented in
`architecture-v0.md`. That file does not exist anywhere in this repository.
The closest existing reference, `docs/ingestion/bronze-object-paths.md`,
explicitly flags the strategy as undecided:

> Whether publication version should become an explicit part of the key...
> is an open architecture decision for later incremental-ingestion work,
> not decided here.

This document is that decision, made now, and the Dagster asset graph in
`orchestration/dagster/dvf_dagster/assets.py` implements it.

## Partition dimension: department

The DVF Bronze/Silver chain (`dvf_bronze`, `ban_bronze`, `dpe_bronze`,
`geo_bronze`, `silver_dvf`) is partitioned by department, using the same
four departments `src/ingestion/bulk_ingest.py` already covers:
`75`, `92`, `93`, `94`. This mirrors the department axis of the real
Bronze object-key structure (`dvf/<year>/<department>.csv.gz`, per
`docs/ingestion/bronze-object-paths.md`).

Each department is an independent Dagster partition. Materializing one
department's partition does not require touching the others, and Dagster
tracks per-partition materialization history in its own UI. This is the
actual mechanism of "incremental": if only one department's data changes,
only that partition needs to be re-run.

## What is NOT partitioned, and why

- **`filosofi_bronze`** is unpartitioned. Filosofi's INSEE source is a
  single national file (`FILO2021_DISP_COM.csv` inside one zip); there is
  no per-department object at the source, so no partition axis exists for
  it to key off of. Every partition's Silver run reads from the same
  materialized Filosofi snapshot.
- **`gold_price_aggregates`** is unpartitioned. It reads the entire
  `silver_dvf` Postgres table (`SELECT * FROM silver_dvf`, no department
  filter) to compute municipality/department aggregates, so it always
  needs to re-run against the full table regardless of which department
  partition most recently changed.

## Known limitation: year is not a second partition dimension

`YEAR` remains a fixed module constant (`"2024"`) rather than a second
partition dimension. Making both year and department independently
partitionable would require Dagster's `MultiPartitionsDefinition` plus
explicit partition mapping for the assets that only vary along one of the
two axes (`dpe_bronze`/`geo_bronze` have no year axis at source;
`filosofi_bronze` has neither). That is a real, buildable extension, but
was scoped out here given the time remaining before the project deadline.
Revisiting this is the natural next step if the project continues past
RETL0-54.

## Schedule: DVF's real publication cadence

DVF is published twice yearly, in April and October
(`docs/ingestion/bronze-dvf-procedure.md`). The `dvf_publication_schedule`
Dagster schedule fires at 06:00 on 1 April and 1 October
(`cron_schedule="0 6 1 4,10 *"`) and materializes all four department
partitions of `dvf_pipeline_job`, since a new DVF publication means every
department's Bronze/Silver/Gold data is eligible for a refresh.

The schedule is defined with `default_status=DefaultScheduleStatus.STOPPED`
so it does not fire unattended against the real data.gouv.fr endpoint in a
development environment. It can be turned on from the Dagster UI's
Automation tab, or triggered manually for a one-off demonstration run,
without changing its defined cadence.

## Idempotency already in place

The incremental strategy above is safe to re-run more often than strictly
necessary because every Bronze ingestion script is already idempotent:
`src/ingestion/upload_to_minio.py` (and the equivalent logic in the
enrichment ingestion scripts) compares a SHA-256 checksum against what is
already stored in MinIO and skips the transfer when nothing changed. A
partition can be re-materialized on demand without wasting bandwidth or
creating duplicate objects.