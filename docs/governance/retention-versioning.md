# Bronze Retention and Versioning Policy

## Problem
DVF source files are published at files.data.gouv.fr/geo-dvf/latest/csv/... under a
"latest" folder. Because the folder name never changes, re-running ingestion after
DGFiP's next publication cycle downloads a materially different file but silently
overwrites the previous version's Bronze object at the same key.

## Decision
Bronze object keys now include a publication identifier:
`dvf/publication={YYYY-MM}/year={year}/department={department}/{department}.csv.gz`.
`{YYYY-MM}` is `-04` or `-10` each year (DVF's twice-yearly publication months),
computed automatically from the ingestion date by `current_publication()` in
`src/ingestion/paths.py`. Each publication cycle now produces new, distinct Bronze
objects instead of overwriting prior ones.

## Retention
All publications are retained indefinitely. Compressed department-level DVF files are
a few MB to tens of MB each; even full history back to 2019 for the pilot departments
is a trivial amount of object storage. There's no cost-driven reason to prune, and
downstream layers benefit from being able to reprocess against any historical
publication. Revisit if the pilot scope expands to nationwide, multi-year coverage.

## Consequences
- `local_path()` and `object_key()` now accept an optional `publication` argument,
  defaulting to the current publication when omitted — existing call sites are
  unaffected.
- Bronze objects uploaded before this change exist under the old key format
  (`dvf/{year}/{dept}.csv.gz`) and are not migrated or deleted; they're an artifact of
  the pre-governance ingestion phase.
- Silver/Gold ingestion must take the publication it's building from as an explicit
  parameter, rather than implicitly assuming "whatever's newest."

## Decided by
Anjum Kumawat, 16 September 2026, per RETL0-91.
