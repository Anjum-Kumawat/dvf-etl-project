# DVF ETL Pipeline — French Real Estate Market

End-to-end data engineering pipeline on DVF (Demandes de Valeurs Foncières) and enrichment
open data sources, following a Bronze / Silver / Gold medallion architecture, orchestrated
with Dagster, quality-checked with Great Expectations, and served through Metabase — with a
predictive model built as a downstream stage of the same pipeline (not a separate project).

## Architecture

```
Sources (DVF csv.gz, BAN/ADEME/INSEE/geo APIs)
        |  Python (ingestion)
        v
   MinIO: Bronze (raw data)
        |  PySpark (cleaning, enrichment, joins)
        v
   PostgreSQL: Silver (clean data) / Gold (analytical tables)
        |                                  \
        v                                   v
   Metabase (dashboards)          ML feature pipeline -> model
                                          |
                                          v
                              Predictions written back to Postgres
                                   (surfaced in Metabase)

   Dagster orchestrates the whole pipeline, including the ML asset.
   Great Expectations controls quality at every layer.
```

## Repository layout

```
ingestion/            Python ingestion scripts
  dvf/                DVF csv.gz downloader -> MinIO Bronze
  apis/               BAN / ADEME / INSEE Filosofi / geo.api.gouv.fr clients
transformation/        PySpark jobs
  silver/             Cleaning, typing, dedup, multi-source joins
  gold/               Analytical aggregate tables (municipality/dept/quarter)
orchestration/
  dagster/            Dagster assets, schedules, resources
quality/
  great_expectations/ Expectation suites, validation reports
ml/
  features/           Feature engineering spec + code (built on Gold tables)
  models/             Training code, serialized models
  evaluation/          Evaluation reports, backtests
analytics/
  metabase/           Dashboard definitions / exported questions
docs/                 Architecture doc, data dictionary, source contracts, ops guide
.github/
  ISSUE_TEMPLATE/     Ticket template (Owner / Reviewer / Reproducer)
```

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

Services once running: MinIO console, PostgreSQL, Dagster UI, Metabase. See
`docs/ops_guide.md` (to be written in Sprint 7) for full instructions.

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