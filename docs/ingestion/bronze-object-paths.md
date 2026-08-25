# Bronze Object Path Convention

## Current local raw convention

`data_raw/<year>/<department>.csv.gz`

Examples:

`data_raw/2023/75.csv.gz`
`data_raw/2024/94.csv.gz`

## Current Bronze convention

Bucket: `bronze`

Object key: `dvf/<year>/<department>.csv.gz`

Full logical examples — all eight current partitions:

`bronze/dvf/2023/75.csv.gz`
`bronze/dvf/2023/92.csv.gz`
`bronze/dvf/2023/93.csv.gz`
`bronze/dvf/2023/94.csv.gz`
`bronze/dvf/2024/75.csv.gz`
`bronze/dvf/2024/92.csv.gz`
`bronze/dvf/2024/93.csv.gz`
`bronze/dvf/2024/94.csv.gz`

## Terminology

**Bucket** — the top-level MinIO container: `bronze`

**Object key** — the complete object name inside the bucket:
`dvf/2024/75.csv.gz`

**Prefix** — a folder-like portion used for listing related objects:
`dvf/2024/`

## Design rationale

The structure organizes raw objects by dataset, then year, then department.
This allows additional departments and years to be added without inventing
names such as `75_new.csv.gz`, `75_final.csv.gz`, or `75_v2.csv.gz` — each
new partition is a new key at a predictable path, never a rename of an
existing object. This also makes prefix-based listing meaningful:
`dvf/2024/` lists every department ingested for 2024; `dvf/` lists every
year ever ingested.

## Bronze rules

- Raw source archives remain compressed — never decompressed in Bronze
- Raw source data is not cleaned, typed, or transformed
- Department is represented consistently (no leading-zero ambiguity)
- Year is represented consistently
- Generated/raw objects (`data/`, `data_raw/`) are not committed to Git

## Future consideration: publication versioning

Publication/source versioning is **not** currently encoded in the object
key. DVF is published on a half-yearly cadence (April and October), and a
later publication for the same year/department will currently overwrite
the previous one at the same key — see `src/ingestion/upload_to_minio.py`
for the checksum-based logic that governs when this happens.

This document does not change the existing key structure. Whether
publication version should become an explicit part of the key (e.g.
`dvf/publication=2026-04/2024/75.csv.gz`) is an open architecture decision
for later incremental-ingestion work, not decided here.