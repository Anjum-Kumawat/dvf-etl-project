# DVF ETL Pipeline — French Real Estate Market

End-to-end data engineering pipeline on DVF (Demandes de Valeurs Foncières) and enrichment
open data sources, following a Bronze / Silver / Gold medallion architecture, orchestrated
with Dagster, quality-checked with a custom Bronze/Silver rules engine, and served through
Metabase — with a predictive model planned as a downstream stage of the same pipeline (see
Sprint plan below; not yet built).

## Architecture

```
Sources (DVF csv.gz, BAN/ADEME/INSEE/geo APIs)
        |  Python (ingestion, src/ingestion/)
        v
   MinIO: Bronze (raw data)
        |  PySpark (cleaning, enrichment, joins, src/silver/)
        v
   PostgreSQL: Silver (clean data) / Gold (analytical tables, src/gold/)
        |
        v
   Metabase (dashboards)

   Dagster (orchestration/dagster/) orchestrates the whole pipeline: a webserver for the
   UI/API and a separate daemon process for sensors and schedules, both required.
   Quality is checked at the Silver layer by src/silver/dvf_quality_checks.py — HARD FAIL
   rules block a materialization (postal/BAN-name/Filosofi/geo coverage floors, known
   nature_mutation categories); WARNING rules (BAN/DPE join coverage) log real regional
   variance without blocking. See docs/governance/quality-rules.md for the full rule set
   and the real multi-department evidence behind each threshold.
```

## Repository layout

```
src/
  ingestion/            DVF downloader, MinIO upload, checksum/validation utilities
    enrichment/          BAN / DPE (ADEME) / Filosofi (INSEE) / geo.api.gouv.fr clients
  silver/                PySpark cleaning, dedup, multi-source joins, quality checks
  gold/                  Analytical aggregate tables (municipality/department/quarter)
  common/                Shared Spark session setup
orchestration/
  dagster/               Dagster assets, sensors, schedules, Dockerfile (webserver + daemon
                         share this image; see docker-compose.yml for both services)
docs/
  governance/            Quality rules, data dictionaries, lineage, GDPR, retention
  ingestion/             Ingestion procedures, object paths, incremental strategy, retries
  sprint_plan.md         Full sprint-by-sprint ticket breakdown
scripts/                 Ad-hoc diagnostic scripts used to investigate real data issues
                         (kept for reproducibility of past investigations, not part of the
                         pipeline itself)
tests/                   pytest suite, mirrors src/ (ingestion/, silver/, gold/)
ml/                      Predictive-model workstream (features/models/evaluation) — planned,
                         not yet built; see docs/sprint_plan.md backlog
.github/
  ISSUE_TEMPLATE/        Ticket template (Owner / Reviewer / Reproducer)
```

Note: `ingestion/`, `transformation/silver/`, `transformation/gold/`, and
`quality/great_expectations/` at the repo root are empty placeholders left over from
Sprint 0 planning — real code lives under `src/` as shown above. `analytics/metabase/`
is also empty: dashboards are not exported to files, they live in Metabase's own
application database (see Getting started below for how that's persisted).

## Team & roles

| Person | Primary responsibility |
|---|---|
| P1 | Ingestion and infrastructure |
| P2 | Transformation and data modeling |
| P3 | Data quality and orchestration |
| P4 | Analytics, documentation and business interpretation |
| P5 | Data governance, enrichment mapping and predictive modeling |

Full role breakdown, sprint-by-sprint workload, and the ticket review workflow are in
[`CONTRIBUTING.md`](./CONTRIBUTING.md).

## Getting started

```bash
git clone <this-repo-url>
cd dvf-etl-project
docker compose up
```

This starts six services:

| Service | URL / access |
|---|---|
| MinIO console | http://localhost:9001 (minioadmin / minioadmin) |
| PostgreSQL | localhost:5434, db `dvf`, user/pass `dvf`/`dvf` |
| Dagster webserver (UI) | http://localhost:3000 |
| Dagster daemon | background only — required for sensors/schedules to run, no UI of its own |
| Metabase | http://localhost:3001 — first run walks you through creating an admin account and connecting a database; point it at PostgreSQL using host `postgres` (the Docker service name, not `localhost`), port `5432`, db `dvf`, user/pass `dvf`/`dvf` |

To run the pipeline: open the Dagster UI, go to **Assets**, select all assets (or a
department partition under **Overview → Partitions**), and click **Materialize
selected**. Assets are partitioned by department (`75`, `92`, `93`, `94`); Bronze assets
retry automatically on transient network failures, Silver/Gold retry once on infra
hiccups. A failed run — after retries are exhausted — is written to a `pipeline_alerts`
table in Postgres (see docs/ingestion/retries-and-alerting.md); check
**Deployment → Daemons** in the UI if sensors show "Not running."

Metabase's own application data (saved questions, dashboards) persists in a named Docker
volume (`metabase_data`) via `MB_DB_FILE`, so it survives `docker compose down` / `up` —
but not `docker compose down -v`, which deletes volumes.

## Sprint plan

See [`docs/sprint_plan.md`](./docs/sprint_plan.md) for the full 8-sprint breakdown, including
where the predictive-model workstream plugs into the pipeline each sprint.

## Testing

Bronze ingestion is covered by an automated test suite using **pytest**.

Run the complete suite:
python -m pytest tests/ -v

Tests are isolated with `tmp_path`/`monkeypatch` fixtures and a fake S3
client (`tests/ingestion/fake_s3.py`) — no live MinIO connection or network
access is required to run them, and no real DVF archives are committed to
the repository.
