# Bronze DVF Ingestion Procedure

Complete, reproducible procedure for Bronze DVF ingestion. Covers only
Bronze — Silver, Gold, and predictive-model steps are not implemented yet
and are not described here.

## 1. Prerequisites

- Git repository cloned locally
- Python 3.11+ with `pip install -r requirements.txt` (installs `requests`,
  `boto3`, `pytest`)
- Docker Desktop installed and running
- Docker Compose (bundled with Docker Desktop)

## 2. Start MinIO
docker compose up -d minio


## 3. Verify MinIO

docker compose ps minio


Expected: a `minio` service listed as running, with ports `9000` (S3 API)
and `9001` (web console) mapped.

## 4. Verify connection

python src/ingestion/minio_client.py


Expected output: `['bronze']` — confirms boto3 can reach MinIO and the
`bronze` bucket exists.

## 5. Download a DVF partition

python src/ingestion/download_dvf.py --year 2024 --dept 75

## 6. Local raw path

Downloaded files land at:

`data_raw/<year>/<department>.csv.gz`

See `docs/ingestion/bronze-object-paths.md` for the full path convention.

## 7. Source validation

Every download is validated before being accepted, in `src/ingestion/validation.py`:

- **HTTP validation** — `response.raise_for_status()` catches 404/500-class
  errors; the command exits with a clear `ERROR:` message and non-zero
  status
- **Non-empty check** — a zero-byte or missing file fails validation
- **Gzip integrity check** — the file is opened and fully read as a gzip
  stream; a truncated or corrupt archive fails validation and is deleted
  before it can reach Bronze

## 8. Checksum

- **Calculation:** SHA-256, computed by `src/ingestion/checksum.py`
  (`compute_sha256`), over the raw `.csv.gz` file without modifying it
- **Where stored:** as MinIO object metadata on the uploaded object itself
  (not a local manifest file) — retrievable via `head_object`
- **Why needed:** it is the deterministic fingerprint used to decide
  whether a re-run should upload, skip, or treat the source as changed —
  see Section 9

## 9. Upload / idempotency
python src/ingestion/upload_to_minio.py --year 2024 --dept 75


Expected outcomes, printed clearly to the terminal:

- **`Uploaded: ...`** — no object existed yet at that key; file uploaded
  with its checksum stored as metadata
- **`Skipped: ... unchanged`** — an object already exists and its stored
  checksum matches the local file's current checksum; nothing is
  transferred
- **`Changed: ... checksum differed ... re-uploaded`** — an object exists
  but its stored checksum differs from the local file; the new version is
  uploaded, overwriting the object at the same key

## 10. Expanded-scope ingestion

python src/ingestion/bulk_ingest.py

Loops over years `2023`/`2024` and departments `75`/`92`/`93`/`94` (defined
at the top of `bulk_ingest.py`), calling `download_dvf.py` and
`upload_to_minio.py` for each combination. Stops immediately on the first
failure; partitions already processed before the failure remain in Bronze.
Edit the `YEARS`/`DEPARTMENTS` lists to extend scope — do not edit the
ingestion scripts themselves.

## 11. MinIO paths

Full bucket/object-key/prefix convention, terminology, and all current
partitions are documented in
[`docs/ingestion/bronze-object-paths.md`](./bronze-object-paths.md).

## 12. Automated tests
python -m pytest tests/ -v

Framework: **pytest**. Suite lives under `tests/ingestion/`, is isolated
with `tmp_path`/`monkeypatch` fixtures and a fake S3 client
(`tests/ingestion/fake_s3.py`) — no live MinIO connection, network access,
or committed DVF archives are required to run it.

## 13. Rerun behavior

- **Object missing:** uploaded, printed as `Uploaded`
- **Object exists, same checksum:** skipped, printed as `Skipped`,
  no transfer occurs
- **Object exists, checksum changed:** re-uploaded, printed as `Changed`,
  overwrites the object at the same key
- **A source fails mid multi-partition run** (`bulk_ingest.py`): the loop
  stops immediately at the failing department; every partition processed
  before the failure stays uploaded in Bronze; nothing after the failure
  point is attempted in that run

## 14. Troubleshooting

- **Docker/MinIO not running:** connection errors to `localhost:9000` —
  run `docker compose up -d` and confirm with `docker ps`
- **Port 9000 unavailable:** another process is bound to it, or MinIO
  failed to start — check `docker ps` / `docker logs minio`
- **Invalid credentials:** all scripts use `minioadmin` / `minioadmin`
  against the local dev MinIO instance — confirm `docker-compose.yml`
  hasn't been changed
- **HTTP 404/500 from data.gouv.fr:** wrong `--year`/`--dept` combination,
  or the source is temporarily unavailable — the script exits with a clear
  `ERROR:` line, no partial file is left in Bronze
- **Corrupt gzip archive:** caught by `validate_archive` before upload —
  the file is deleted locally, command exits non-zero with the reason
  printed
- **Missing local file when uploading:** `upload_to_minio.py` exits with
  `ERROR: local file not found` — run the download step first

## 15. Git exclusions

`data/` and `data_raw/` are excluded from Git (see `.gitignore`). Raw DVF
archives and MinIO's local bind-mount data are large, regenerable from the
source, and not meaningful to version — committing them would bloat the
repository for no benefit. Only `data_raw/manifest.json`-style structured
metadata (where applicable) is tracked; the checksum-bearing MinIO object
metadata is the actual source of truth for idempotency, not a local file.
