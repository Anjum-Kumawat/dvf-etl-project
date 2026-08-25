# Bronze Object Path Convention

## DVF (main source)
```
dvf/{year}/{department}.csv.gz
```
Examples:
```
dvf/2023/75.csv.gz
dvf/2024/92.csv.gz
```
- `{year}` — transaction year, e.g. `2024`
- `{department}` — French department code, e.g. `75`, zero-padded where
  the department code itself has a leading zero (e.g. `01`, `06`)
- File is stored unchanged, still gzip-compressed, exactly as downloaded

## Why this structure

- **Extensible without renaming:** adding a new year or department is a new
  path, not a rename of an existing object — matches the incremental
  ingestion requirement
- **Listable by prefix:** `dvf/2024/` lists every department ingested for
  that year; `dvf/` lists every year ever ingested
- **One file, one object:** no ambiguity about which version is current —
  see `docs/architecture/architecture-v0.md` for how re-runs are made
  idempotent via the checksum manifest, rather than by renaming files
  (`75.csv.gz`, `75_final.csv.gz`, etc., which this convention avoids)

## Local mirror (data_raw/)

The local pre-upload staging area mirrors the same structure:
```
data_raw/{year}/{department}.csv.gz
data_raw/manifest.json
```
`manifest.json` is the only tracked file under `data_raw/` — see
`src/ingestion/dvf.py` and `docs/architecture/architecture-v0.md` for the
manifest schema (checksum, size, timestamp, status) that backs idempotent
ingestion.

## Enrichment sources (future)

Not yet implemented, but the same principle applies once enrichment
ingestion starts — one path segment per source, one per partition key
relevant to that source (see `docs/source_contracts.md` for what each
source's natural partition key would be, e.g. BAN has none since it's a
live geocoding API, not a batch file; Filosofi is annual, so
`filosofi/{year}/...`).