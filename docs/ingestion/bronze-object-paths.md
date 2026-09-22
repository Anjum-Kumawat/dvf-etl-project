# Bronze Object Path Convention

## Current local raw convention

`data_raw/<publication>/<year>/<department>.csv.gz`

Examples:

`data_raw/2026-04/2024/75.csv.gz`
`data_raw/2026-10/2024/75.csv.gz`

## Current Bronze convention

Bucket: `bronze`

Object key: `dvf/publication=<publication>/year=<year>/department=<department>/<department>.csv.gz`

Full logical example, department 75 (the only department a second
publication has been forced against so far, via the incremental-publication
dry run documented in `docs/ingestion/incremental-ingestion-strategy.md`):

`bronze/dvf/publication=2026-04/year=2024/department=75/75.csv.gz`
`bronze/dvf/publication=2026-10/year=2024/department=75/75.csv.gz`

The same `publication=<publication>/year=<year>/department=<department>/`
structure applies to the BAN enrichment source, which is keyed per DVF
partition since it geocodes addresses extracted from that DVF file:

`bronze/ban/publication=2026-04/year=2024/department=75/75.csv`

**Historical objects, not part of the current convention:**
`bronze/dvf/2023/75.csv.gz` and `bronze/dvf/2024/75.csv.gz` (and the
equivalent for 92/93/94) were written before the publication-scoped key
structure above was decided and implemented (RETL0-49). They remain in the
bucket as leftover objects from that earlier convention, not because
anything still writes to that path.

## Terminology

**Bucket** — the top-level MinIO container: `bronze`

**Object key** — the complete object name inside the bucket:
`dvf/publication=2026-04/year=2024/department=75/75.csv.gz`

**Prefix** — a folder-like portion used for listing related objects:
`dvf/publication=2026-04/year=2024/`

## Design rationale

The structure organizes raw objects by dataset, then publication, then
year, then department. This allows additional departments, years, and DVF
publications to be added without inventing names such as `75_new.csv.gz`,
`75_final.csv.gz`, or `75_v2.csv.gz` — each new partition is a new key at a
predictable path, never a rename of an existing object. This also makes
prefix-based listing meaningful: `dvf/publication=2026-04/` lists every
year/department ingested for that publication;
`dvf/publication=2026-04/year=2024/` narrows to one year.

## Bronze rules

- Raw source archives remain compressed — never decompressed in Bronze
- Raw source data is not cleaned, typed, or transformed
- Department is represented consistently (no leading-zero ambiguity)
- Year is represented consistently
- Generated/raw objects (`data/`, `data_raw/`) are not committed to Git

## Publication versioning — decided (RETL0-9 follow-up)

Earlier drafts of this document left publication/source versioning as an
open question, noting that a later publication for the same year/department
would overwrite the previous one at the same key. That question is now
decided: the object key includes an explicit `publication=<label>` segment
(see "Current Bronze convention" above), so a new DVF publication writes to
a new key rather than overwriting the previous one. `silver_dvf` is what
supersedes the older publication for a department — via a
`DELETE FROM silver_dvf WHERE code_departement = %s` followed by an append,
keyed by department only, not by publication — while Bronze itself retains
every publication ever ingested. See
`docs/ingestion/incremental-ingestion-strategy.md` for the full design and
a real dry-run verification of this behavior.