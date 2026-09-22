\# System Architecture



\*\*Scope:\*\* DVF + 4 enrichment sources, 4 pilot departments (75, 92, 93, 94), year 2024.

See `docs/sprint\_plan.md` for how this scope was reached sprint by sprint.



This document describes how the pieces of this pipeline fit together and why they were

chosen. It does not repeat what's already covered elsewhere in detail — see "Related

documents" at the end for the data dictionaries, quality rules, and lineage docs this

one links out to instead of duplicating.



\## 1. High-level data flow



```

Sources                          Bronze                  Silver              Gold           Serving

\--------                         ------                  ------              ----           -------

DVF (data.gouv.fr)        \\                        /

BAN (IGN geocoding API)    \\                      /

DPE (ADEME API)             >--  MinIO (S3A)  -->  PySpark  -->  PostgreSQL  -->  PySpark  -->  PostgreSQL  -->  Metabase

Filosofi (INSEE zip)       /     bucket: bronze     clean,       silver\_dvf       aggregate     gold\_price\_\*    dashboards

geo.api.gouv.fr (comm.)   /                         dedup,                       (municip./     \_quarter

&#x20;                                                    enrich,                      dept x

&#x20;                                                    quality-                     quarter)

&#x20;                                                    gate

```



Everything above is orchestrated by Dagster (`orchestration/dagster/`), which is the only

thing that actually invokes the ingestion, transformation, and aggregation code — none of

`src/ingestion/`, `src/silver/`, or `src/gold/` schedule themselves.



\## 2. Component inventory



| Component | Technology | Real configuration | Role |

|---|---|---|---|

| Object storage | MinIO (`quay.io/minio/minio`) | Ports 9000 (S3 API) / 9001 (console); bucket `bronze`, auto-created by a one-shot `createbuckets` service (`quay.io/minio/mc`) | Bronze layer — raw, uncleaned source archives, keyed by `<dataset>/publication=.../year=.../department=...` |

| Orchestrator | Dagster | Webserver (port 3000) + a separate `dagster-daemon` process, both built from `orchestration/dagster/Dockerfile`, both bind-mounting the whole repo (`./:/opt/dvf-etl-project`) so asset-code changes are picked up on the next "Reload definitions" without a rebuild — a rebuild is only needed when the Dockerfile or `requirements-dagster.txt` changes | Runs every ingestion/transform/aggregate step as a Dagster asset; the daemon evaluates the schedule and failure sensor (neither runs without it) |

| Transformation engine | PySpark, `local\[\*]` master (`src/common/spark\_session.py`) | Reads Bronze via the S3A connector (`hadoop-aws:3.4.2`), writes to PostgreSQL via JDBC (`postgresql:42.7.4`), both resolved from Maven at run time | Silver cleaning/dedup/joins/quality checks; Gold aggregation |

| Warehouse | PostgreSQL 16 | Port 5434 (host) / 5432 (container), db `dvf`, user/pass `dvf`/`dvf` | Holds `silver\_dvf`, both Gold tables, and `pipeline\_alerts` (failure log) — one database for both layers, not a separate warehouse per layer |

| BI / serving | Metabase | Port 3001; app data persisted via `MB\_DB\_FILE` to a named volume (`metabase\_data`), connects to the same Postgres instance as a data source | Dashboards (RETL0-51/52/53): price/m² trends, transaction volumes, price x DPE cross-analysis |



Real end-to-end data volume at this scope: 194,911 Silver rows across the 4 departments

(67072 + 51795 + 36353 + 39691), 571 Gold municipality-quarter rows across 143 communes,

16 Gold department-quarter rows.



\## 3. Orchestration design



Dagster models the pipeline as 7 assets (`orchestration/dagster/dvf\_dagster/assets.py`):

`dvf\_bronze`, `ban\_bronze`, `dpe\_bronze`, `filosofi\_bronze`, `geo\_bronze`, `silver\_dvf`,

`gold\_price\_aggregates`. `silver\_dvf` depends on all 5 Bronze assets; `gold\_price\_aggregates`

depends on `silver\_dvf`.



\*\*Partitioning.\*\* `dvf\_bronze`, `ban\_bronze`, `dpe\_bronze`, `geo\_bronze`, and `silver\_dvf`

are partitioned by department (`StaticPartitionsDefinition(\["75", "92", "93", "94"])`), so

one department can be re-materialized without touching the others. `filosofi\_bronze` is

unpartitioned (INSEE's source is one national file, no per-department object exists to key

off of) and `gold\_price\_aggregates` is unpartitioned (it reads the whole `silver\_dvf` table

with no department filter, so it always recomputes against everything). `YEAR` is a fixed

module constant (`"2024"`), not a second partition dimension — a deliberate scope decision,

documented with the reasoning in `docs/ingestion/incremental-ingestion-strategy.md`.



\*\*Retries.\*\* Bronze assets use `NETWORK\_RETRY\_POLICY` (3 retries, 30s delay) since they call

external APIs and are checksum-gated, so a retry is always safe. Silver/Gold use

`COMPUTE\_RETRY\_POLICY` (1 retry, 60s delay) since their failures are more often deterministic

(a real quality-check HARD FAIL, a code bug) than transient.



\*\*Schedule.\*\* `dvf\_publication\_schedule` fires at 06:00 on 1 April and 1 October

(`cron\_schedule="0 6 1 4,10 \*"`), materializing all 4 department partitions — DVF's real

half-yearly publication cadence. It ships with `default\_status=DefaultScheduleStatus.STOPPED`

so it never fires unattended against the real data.gouv.fr endpoint in a dev environment; it

can be switched on from the Dagster UI's Automation tab for a live demonstration.



\*\*Failure alerting.\*\* `pipeline\_failure\_sensor` (`sensors.py`) fires on any run failure in

the code location — deliberately unscoped to a specific job, since materializing assets from

the UI's "Materialize selected" button (how nearly every run in this project has actually

been launched) creates an implicit `\_\_ASSET\_JOB`, not `dvf\_pipeline\_job`; an earlier,

job-scoped version of this sensor was tested against a real induced failure and never fired.

It writes a row (run id, job, failed step, error message) to `pipeline\_alerts` in Postgres —

a Dagster-native alert channel chosen specifically because this project has no Slack/email

credentials to fabricate and needed to stay reproducible on a machine that has never seen the

project before (RETL0-54's constraint).



\## 4. Data quality architecture



Quality is enforced at the Silver layer by `src/silver/dvf\_quality\_checks.py` — a hand-written

rule engine, not a third-party framework. (The `quality/great\_expectations/` folder at the

repo root is an empty Sprint 0 placeholder with no code in it; see `README.md`'s Repository

Layout note.) Rules are split into two severities:



\- \*\*HARD FAIL\*\* rules block a Silver materialization outright: known `nature\_mutation`/

&#x20; `type\_local` categories, `date\_mutation` within 2024, positive `valeur\_fonciere`, no exact

&#x20; duplicate rows, and Filosofi/geo join coverage floors (both required at 100% — both sources

&#x20; are deterministic joins on commune code, so anything less than 100% signals a real bug, not

&#x20; natural variance).

\- \*\*WARNING\*\* rules log real data variance without blocking: missing `valeur\_fonciere`,

&#x20; missing surface on built properties, outlier value bands, BAN geocoding confidence, and

&#x20; BAN/DPE join coverage floors (90% / 70% — these depend on external services having a match

&#x20; for a given address, which varies legitimately by department).



Full rule list, real per-department evidence, and the reasoning behind each threshold is in

`docs/governance/quality-rules.md`.



\## 5. Incremental ingestion strategy



Summarized here; full design and a real dry-run verification are in

`docs/ingestion/incremental-ingestion-strategy.md`.



\- \*\*Department is the partition axis\*\* Dagster tracks materialization history against.

\- \*\*Bronze keys are publication-scoped\*\*: `<dataset>/publication=<label>/year=<year>/department=<dept>/...`

&#x20; (`docs/ingestion/bronze-object-paths.md`), so a new DVF publication writes new objects

&#x20; rather than overwriting the previous one — Bronze retains every publication ever ingested.

\- \*\*Silver supersedes by department only\*\*, not by publication: `run\_dvf\_silver.py` does

&#x20; `DELETE FROM silver\_dvf WHERE code\_departement = %s` then appends, so the most recently

&#x20; materialized publication for a department is what `silver\_dvf` reflects.

\- This was verified with a real forced `publication=2026-10` dry run for department 75:

&#x20; `silver\_dvf`'s dept-75 row count stayed at 67072 (replaced in place, not duplicated) while

&#x20; 92/93/94 stayed byte-for-byte unchanged, and Bronze ended up holding both the `2026-04` and

&#x20; `2026-10` objects for dept 75 side by side.



\## 6. Technology choices and rationale



| Choice | Why |

|---|---|

| Dagster over a plain cron/script scheduler | Asset-based model matches the Bronze/Silver/Gold shape directly; per-partition materialization history, retry policies, and failure sensors are built in rather than hand-rolled |

| PySpark for Silver/Gold, `local\[\*]` master | Gives the team production-representative transformation patterns (DataFrame API, testable join/quality functions) even though the real row counts here (max 67,072 rows in one partition) don't require distributed compute. `local\[\*]` is a deliberate scope choice for this pilot, not a demonstration of horizontal scalability — see Known Limitations. |

| One PostgreSQL instance for both Silver and Gold | Simpler than standing up a separate warehouse for a 4-department pilot; Metabase then only needs one data source connection |

| MinIO (S3-compatible) rather than a cloud object store | Mirrors a real S3-based Bronze layer without needing cloud credentials or billing for local dev; ingestion code already speaks the S3A/boto3 API, so pointing at real AWS S3 later is a credentials/endpoint change, not a rewrite |

| Metabase for BI | Free, self-hosted, fast to stand up via Docker, sufficient for the dashboards this project needed |

| Hand-written quality rules over Great Expectations | The repo has an empty `quality/great\_expectations/` placeholder from Sprint 0 planning that was never built out; quality checks ended up as plain Spark/Python functions instead, documented rule by rule in `docs/governance/quality-rules.md` |



\## 7. Deployment topology and its limits



Everything runs as a single `docker compose up` on one host — there is no cloud deployment,

no Kubernetes, no multi-node Spark cluster. This is a deliberate scope boundary for an

academic pilot, not an oversight, but it should be named plainly:



\- All credentials are hardcoded defaults (`minioadmin`/`minioadmin`, `dvf`/`dvf`) with no

&#x20; secrets manager — fine for local dev/demo, not how this would be deployed for real data.

\- No TLS between services (`fs.s3a.connection.ssl.enabled=false`, plain HTTP to MinIO).

\- No authentication in front of the Dagster or Metabase web UIs.

\- A production version of this pipeline would need, at minimum: a managed object store with

&#x20; real IAM, a managed or clustered Spark runtime, secrets injected via environment/vault

&#x20; rather than compose defaults, and network-level access control in front of both UIs.



\## 8. Known architectural limitations



\- Scope is 4 departments (75, 92, 93, 94), not all of Île-de-France or France.

\- `YEAR` is a fixed constant, not a partition dimension — see section 3.

\- Gold is a full overwrite on every run, not incremental, deliberately (cheap to rebuild from

&#x20; Silver at this row-count scale; see `docs/governance/gold-data-dictionary.md`).

\- Spark runs single-JVM/local, not distributed — see section 6.

\- No secrets management or TLS — see section 7.

\- The predictive-model workstream (RETL0-9) is a design spec only

&#x20; (`docs/ml/retl0-9-predictive-model-spec.md`); no model is trained or served.

\- `ingestion/`, `transformation/silver/`, `transformation/gold/`, `quality/great\_expectations/`,

&#x20; and `analytics/metabase/` at the repo root are empty Sprint 0 placeholders — real code lives

&#x20; under `src/`, and Metabase's dashboards live in its own application database, not as files.



\## Related documents



\- `docs/governance/quality-rules.md` — full quality rule list and evidence

\- `docs/governance/silver-data-dictionary.md`, `docs/governance/gold-data-dictionary.md`,

&#x20; `docs/governance/gold-lineage.md` — column-level detail for Silver and Gold

\- `docs/ingestion/bronze-object-paths.md`, `docs/ingestion/incremental-ingestion-strategy.md`,

&#x20; `docs/ingestion/reproducibility-pass.md`, `docs/ingestion/retries-and-alerting.md` —

&#x20; ingestion mechanics, incremental design, fresh-clone verification, failure handling

\- `docs/governance/gdpr-analysis.md`, `docs/governance/retention-versioning.md` — data

&#x20; governance

\- `docs/ml/retl0-9-predictive-model-spec.md` — predictive model design (not yet built)

\- `docs/sprint\_plan.md` — full sprint-by-sprint ticket breakdown



