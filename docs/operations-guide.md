\# Operations Guide



Practical runbook for running, monitoring, and troubleshooting this pipeline day to day.

See `docs/architecture.md` for \*why\* things are built this way; this document is about

\*how to operate\* what's already built.



\## 1. Starting and stopping the stack



```bash

docker compose up -d

docker compose ps

```



`docker compose ps` should show `minio`, `postgres`, `dagster`, `dagster-daemon`, and

`metabase` all `Up` (and `createbuckets` as `Exited` — it's a one-shot init container,

exiting after it creates the `bronze` bucket is correct, not a failure).



```bash

docker compose down

```



Stops everything but \*\*keeps\*\* volumes (`postgres\_data`, `metabase\_data`, and the bind-mounted

`./data` for MinIO) — Silver/Gold data, Metabase dashboards, and Bronze objects all survive.



```bash

docker compose down -v

```



Also \*\*deletes\*\* those volumes. Only use this deliberately (e.g. to re-run the fresh-clone

reproducibility test from `docs/ingestion/reproducibility-pass.md`) — it wipes Silver, Gold,

every Metabase dashboard/question, and every Bronze object.



\## 2. Running the pipeline



\*\*Via the Dagster UI\*\* (the normal way): open `http://localhost:3000`, go to \*\*Assets\*\*,

select the assets or a department partition (\*\*Overview → Partitions\*\*), click

\*\*Materialize selected\*\*.



\*\*Via the schedule\*\*: `dvf\_publication\_schedule` fires automatically on 1 April / 1 October

if enabled from the \*\*Automation\*\* tab — it ships `STOPPED` by default (see

`docs/architecture.md` §3 for why).



\*\*Via direct script invocation\*\* (for testing/backfilling a specific publication without

waiting for real calendar time — used for the incremental-publication dry run in

`docs/ingestion/incremental-ingestion-strategy.md`):



```bash

docker compose exec -w /opt/dvf-etl-project dagster python src/ingestion/download\_dvf.py --year 2024 --dept 75 --publication 2026-10

docker compose exec -w /opt/dvf-etl-project dagster python src/ingestion/upload\_to\_minio.py --year 2024 --dept 75 --publication 2026-10

docker compose exec -w /opt/dvf-etl-project dagster python src/ingestion/enrichment/ban\_ingest.py --year 2024 --dept 75 --publication 2026-10

docker compose exec -w /opt/dvf-etl-project dagster python -m src.silver.run\_dvf\_silver --year 2024 --dept 75 --publication 2026-10 --dpe-extraction-date <date> --geo-extraction-date <date>

```



`dpe\_bronze`/`geo\_bronze`/`filosofi\_bronze` aren't publication-scoped (see

`docs/architecture.md` §3), so an existing extraction date can be reused — list what's

available first:



```bash

docker compose exec -w /opt/dvf-etl-project dagster python -c "import sys; sys.path.insert(0, 'src/ingestion'); from upload\_to\_minio import get\_client, BUCKET; c = get\_client(); resp = c.list\_objects\_v2(Bucket=BUCKET); \[print(o\['Key']) for o in resp.get('Contents', \[]) if 'extraction\_date' in o\['Key']]"

```



\## 3. After changing code



`dagster` and `dagster-daemon` bind-mount the whole repo (`./:/opt/dvf-etl-project`), so a

change to asset code, ingestion scripts, or Silver/Gold transforms is picked up as soon as

you reload — \*\*no rebuild needed\*\*:



1\. Open `http://localhost:3000` → \*\*Deployment\*\* → \*\*Code locations\*\*.

2\. Click \*\*Reload\*\* on `orchestration.dagster.dvf\_dagster.definitions`.

3\. Confirm status stays \*\*Loaded\*\* (green) with no red error banner. A banner means a real

&#x20;  import or syntax error — read the traceback, it names the file and line.



A rebuild \*\*is\*\* needed when `orchestration/dagster/Dockerfile` or

`orchestration/dagster/requirements-dagster.txt` changes (new system package, new Python

dependency):



```bash

docker compose build dagster dagster-daemon

docker compose up -d dagster dagster-daemon

```



\## 4. Monitoring for failures



\*\*Dagster UI, Deployment → Daemons\*\*: `Sensors`, `Scheduler`, and `Backfill` should all show

`Running`. If any show `Not running`, the `dagster-daemon` container isn't up — check

`docker compose ps` and `docker compose logs dagster-daemon`.



\*\*`pipeline\_alerts` table\*\* (written by `pipeline\_failure\_sensor` on any run failure,

regardless of how the run was launched):



```bash

docker compose exec postgres psql -U dvf -d dvf -c "SELECT run\_id, job\_name, failed\_step, occurred\_at FROM pipeline\_alerts ORDER BY occurred\_at DESC LIMIT 10;"

```



\*\*Run logs\*\*: click into any run in the Dagster UI's \*\*Runs\*\* tab for the full step-by-step

log, including which quality rule failed if it was a HARD FAIL (`docs/governance/quality-rules.md`).



\## 5. Real operational issues found on this project, and their fixes



These were all found and fixed during real testing (RETL0-54), not anticipated in advance —

kept here so they don't get rediscovered the hard way again.



\- \*\*Metabase dashboards silently reset to empty after restoring a backup.\*\* Cause: restored

&#x20; H2 database files owned by `root:root`, but the `metabase` process runs as user `metabase`

&#x20; with no write access — it silently initializes a fresh empty database instead of erroring.

&#x20; Fix: after any `docker cp` into the Metabase container, `docker compose exec metabase chown

&#x20; metabase:metabase /metabase-data/metabase.db.mv.db` (and `chmod 644`).

\- \*\*`minio`/`mc` image pulls failing with "repository does not exist".\*\* MinIO delisted

&#x20; `minio/minio` and `minio/mc` from Docker Hub in October 2025. Use `quay.io/minio/minio` and

&#x20; `quay.io/minio/mc` (already the default in `docker-compose.yml` — only relevant if you ever

&#x20; see this error after manually editing the compose file).

\- \*\*`NoSuchBucket` on every Bronze upload, only on a genuinely fresh clone.\*\* The `bronze`

&#x20; bucket was created manually early in the project and never automated. Already fixed via the

&#x20; `createbuckets` one-shot service — if this reappears, check that `dagster`/`dagster-daemon`'s

&#x20; `depends\_on` still has `createbuckets: condition: service\_completed\_successfully`, not just

&#x20; `service\_started`.

\- \*\*CI failing with "No module named pytest" or a cache-path error.\*\* There are two

&#x20; requirements files with non-overlapping content: `requirements.txt` (has `pytest`) and

&#x20; `orchestration/dagster/requirements-dagster.txt` (has `pyspark`/`dagster`, not `pytest`).

&#x20; CI must install both.



\## 6. Backups



\*\*PostgreSQL\*\* (Silver, Gold, `pipeline\_alerts`):



```bash

docker compose exec postgres pg\_dump -U dvf -d dvf -F c -f /tmp/dvf\_backup.dump

docker compose cp postgres:/tmp/dvf\_backup.dump ./dvf\_backup.dump

```



Restore into a fresh instance:



```bash

docker compose cp ./dvf\_backup.dump postgres:/tmp/dvf\_backup.dump

docker compose exec postgres pg\_restore -U dvf -d dvf -c /tmp/dvf\_backup.dump

```



\*\*Metabase\*\* (dashboards/questions, stored in its own H2 file via `MB\_DB\_FILE`):



```bash

docker compose exec metabase test -f /metabase-data/metabase.db.mv.db \&\& echo "exists"

docker compose cp metabase:/metabase-data/metabase.db.mv.db ./metabase-backup/metabase.db.mv.db

```



Restore requires copying the file back in and fixing ownership (see the known issue above).



\*\*MinIO (Bronze)\*\*: already on the host filesystem via the bind mount (`./data:/data` in

`docker-compose.yml`) — back up that folder directly, no container command needed.



\## Related documents



\- `docs/architecture.md` — system design and rationale

\- `docs/ingestion/reproducibility-pass.md` — the real fresh-clone test these fixes came from

\- `docs/ingestion/retries-and-alerting.md` — retry policy and sensor design detail

\- `docs/governance/quality-rules.md` — what a HARD FAIL/WARNING in the logs actually means



