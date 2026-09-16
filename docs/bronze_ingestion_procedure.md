# Bronze DVF Ingestion Procedure

## Prerequisites

- Docker Desktop running
- MinIO, PostgreSQL, Metabase containers up: `docker compose up -d`
- `bronze` bucket exists in MinIO (`http://localhost:9001`, login
  `minioadmin` / `minioadmin`)
- Python dependencies installed: `pip install -r requirements.txt`

## Running a single ingestion
python -m src.ingestion.dvf --year 2024 --department 75

This downloads the DVF file for that year/department, validates the HTTP
response and archive integrity, computes a SHA-256 checksum, and records
the result in `data_raw/manifest.json`.

## What happens on success

- File saved to `data_raw/{year}/{department}.csv.gz`
- Manifest entry created/updated with checksum, size, timestamp, and
  `status: "success"`
- Log line: `Download succeeded and archive validated: ... (sha256=...)`

## What happens on failure

- **HTTP error** (e.g. wrong department code): logged as
  `Download failed: HTTP {code} for {url}`, script exits non-zero, no
  manifest entry written for that attempt
- **Corrupt/truncated archive:** logged as
  `Archive integrity check failed for {path}: {reason}`, manifest records
  `status: "failed_corrupt"`
- **Size mismatch** (incomplete download): manifest records
  `status: "failed_size_mismatch"`

## Idempotency

Running the same `--year`/`--department` combination again does **not**
re-download by default — it checks the manifest, and if the local file's
current checksum still matches the recorded one, it skips with:
```
Already ingested and unchanged: {path} (skipping download)
```
To force a re-download regardless (e.g. to pick up a new source
publication):
python -m src.ingestion.dvf --year 2024 --department 75 --force

If the local file is missing, corrupted, or its checksum no longer matches
the manifest, the script re-downloads automatically even without `--force`.

## Uploading to MinIO Bronze
python src/ingestion/upload_to_minio.py --year 2024 --department 75

Uploads the local file to `bronze/dvf/{year}/{department}.csv.gz`,
unchanged. Verifies the upload by reading the object size back from MinIO.
Re-running with the same year/department overwrites the existing object at
the same key rather than creating a duplicate (S3-compatible storage keys
are unique per path).

## Bulk ingestion (multiple years/departments)
python src/ingestion/bulk_ingest.py

Loops over the `YEARS`/`DEPARTMENTS` lists defined at the top of that file,
calling `dvf.py` and `upload_to_minio.py` for each combination in turn.
Stops immediately on the first failure — departments already processed
before the failure remain uploaded; nothing after the failure point runs.
Edit the `YEARS`/`DEPARTMENTS` lists to extend scope; do not edit the
ingestion scripts themselves.

## Troubleshooting

- **Connection errors to `localhost:9000`:** MinIO isn't running — check
  `docker ps`, run `docker compose up -d` if missing
- **`ConnectionResetError` / `ConnectionClosedError`:** same cause as
  above, MinIO container not up
- **`main` branch missing expected scripts after a fresh clone:** confirm
  you're on the latest `main` (`git pull`) — see
  `docs/architecture/architecture-v0.md` for the full pipeline design
