\# RETL0-54: Final Reproducibility Pass



\*\*Ticket:\*\* RETL0-54 · \*\*Requires:\*\* fresh clone, `docker compose up`, full pipeline

run, following only the README.



\## Method



Rather than review the README by eye, this ticket was verified against real ground

truth throughout:



1\. `git ls-files` to get the actual tracked repository layout, rather than trusting

&#x20;  the README's own description of itself.

2\. A genuine fresh clone into a separate folder (`dvf-etl-project-freshtest`),

&#x20;  `docker compose up`, and materializing all 4 department partitions (75/92/93/94)

&#x20;  via the Dagster UI, following only what the README said — no shortcuts from

&#x20;  anything learned earlier in this project.

3\. Every fix below was re-tested against a real failure before being considered

&#x20;  done, not just read back for plausibility.



This surfaced four real, previously-invisible gaps. None were visible from the

existing (long-running) dev deployment, because that deployment had accumulated

state (a manually-created MinIO bucket, cached Docker images, cached Ivy/Maven

jars) that masked all four.



\## Finding 1: README repository layout was stale



The README described `ingestion/`, `transformation/silver/`, `transformation/gold/`,

and `quality/great\_expectations/` as where the code lives — a Sprint 0 plan that was

abandoned early. Real code lives under `src/` (`src/ingestion/`, `src/silver/`,

`src/gold/`, `src/common/`); the old directories are empty `.gitkeep` placeholders.

The README also claimed quality checks use Great Expectations; they don't — a custom

rules engine (`src/silver/dvf\_quality\_checks.py`) was built instead, documented in

`docs/governance/quality-rules.md`. The README's "Getting started" section also

pointed to `docs/ops\_guide.md`, a file that was never written (and referenced a

"Sprint 7" numbering this project doesn't use).



\*\*Fix:\*\* README rewritten — layout section matches real `git ls-files` output, the

Great Expectations claim replaced with what was actually built, and the dead

ops\_guide.md link replaced with real inline setup steps (verified against how every

materialization in this project has actually been run).



\## Finding 2: Metabase had no persistence — real dashboards were one `docker compose down` from gone



`docker-compose.yml`'s `metabase` service had no volume. Metabase's own application

data (saved questions, dashboards — including the three just built for

RETL0-51/52/53) lived only inside the running container's embedded H2 database.

`docker compose down` (not just `stop`) would have destroyed all of it.



\*\*Fix:\*\* added `MB\_DB\_FILE` + a named `metabase\_data` volume.



\*\*A second real bug surfaced while fixing the first:\*\* recreating the container with

the new volume triggered a fresh, empty database. The existing 3 dashboards / 6

questions were backed up (`docker cp` of the H2 files) before recreation and

restored after — but the restore silently failed the first time. Real cause,

confirmed via `docker exec ... ls -la`: the restored files were owned `root:root`

at `0755`, but the `metabase` process runs as the `metabase` user, which had

read-only access — H2 needs write access just to open its own database file, so it

silently fell back to initializing a new one rather than erroring loudly. Fixed with

`chown`/`chmod` after restore; confirmed all 6 questions and 3 dashboards were back

and, this time, durably persisted (verified by a full `docker compose down` / `up`

cycle afterward with data intact).



\## Finding 3: the `bronze` MinIO bucket was never created by anything automated



The fresh-clone test failed immediately on every Bronze asset upload

(`dvf\_bronze`, `ban\_bronze`, `geo\_bronze`, `filosofi\_bronze`) with

`botocore.errorfactory.NoSuchBucket`. Root cause: nothing in the ingestion scripts,

Dockerfile, or `docker-compose.yml` ever creates the `bronze` bucket — it existed in

the long-running dev deployment purely because someone created it manually, very

early in the project, and it had been sitting in the bind-mounted `./data` folder

ever since. A genuinely fresh clone has no bucket and no automated way to get one.



\*\*Fix:\*\* added a `createbuckets` service — a one-shot `mc` container that waits for

MinIO to be reachable (`until mc alias set ...; do sleep 1; done`), then creates the

bucket idempotently (`mc mb --ignore-existing`) and exits. `dagster` and

`dagster-daemon` now depend on it with `condition: service\_completed\_successfully`,

not just `service\_started`, since a one-shot container that has merely \*started\*

hasn't necessarily finished. Verified against a real race: the first run of

`createbuckets` genuinely hit MinIO before it was ready (`connection refused` in its

own logs) and the retry loop caught it correctly before succeeding.



\## Finding 4: MinIO's images were removed from Docker Hub



Wiring up `createbuckets` above surfaced a second, unrelated real failure: `docker

pull minio/mc` returned `pull access denied ... repository does not exist`. This

wasn't a typo — MinIO stopped publishing container images to Docker Hub in October

2025 and delisted the `minio/minio` and `minio/mc` repositories there entirely. The

existing `minio:` service (`image: minio/minio`) had been working throughout this

entire project only because the image was already cached locally on this machine

from early setup, months ago — on a genuinely fresh machine with no cache it would

fail identically.



\*\*Fix:\*\* both `minio:` and `createbuckets:` now pull from `quay.io/minio/...`

(MinIO's own registry, still serving these images), not Docker Hub.



\## Verification



\- Full pipeline re-run from a genuinely fresh clone (new folder, no cache reused

&#x20; except base OS layers), all 4 departments, following only the README:

&#x20; Silver row counts matched the existing deployment exactly —

&#x20; 75→67072, 92→51795, 93→36353, 94→39691 (194,911 total).

\- `python -m pytest tests/ -v` → 108 passed, on the fresh clone.

\- Real data in the long-running dev deployment (1.7 GiB / 27 objects across

&#x20; `dvf/ban/dpe/filosofi/geo`, and all 3 Metabase dashboards / 6 questions) confirmed

&#x20; untouched after applying all four fixes there.

\- `createbuckets`' retry-until-ready behavior and the `service\_completed\_successfully`

&#x20; dependency were both exercised against a real timing race, not just read for

&#x20; plausibility.



\## Known characteristic, not a bug



The very first Spark job run on a freshly-built container needs to resolve and

download `hadoop-aws`'s dependency tree via Ivy, including a \~640MB AWS SDK bundle

jar from Maven Central. On the fresh-clone test this took long enough on the first

attempt (\~23 minutes) to exceed `COMPUTE\_RETRY\_POLICY`'s single retry — but the

partial download was cached, so the retry (department 94, `silver\_dvf`) succeeded

quickly once already-warm. Worth knowing if a first run appears to hang: it's likely

this download, not a stall, and subsequent partitions/runs on the same container are

fast.



