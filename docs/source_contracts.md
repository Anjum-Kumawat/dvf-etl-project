# Bronze Ingestion Contracts — Enrichment Sources

One entry per enrichment source, following the template in
`docs/source_contracts_template.md`. Verified against source documentation
as of August 2026.

## BAN (Base Adresse Nationale) — geocoding

- **URL:** `https://data.geopf.fr/geocodage/search/` (single address),
  `https://data.geopf.fr/geocodage/batch/` (bulk)
- **⚠ Breaking change already happened:** the old endpoint
  `api-adresse.data.gouv.fr` (used in the original project brief) was
  decommissioned end of January 2026 and no longer resolves. All ingestion
  code must target the new Géoplateforme URL above.
- **License:** Etalab Open License (Licence Ouverte)
- **Update frequency:** continuous — BAN is a live address database
  maintained by IGN
- **Format:** REST API, JSON/GeoJSON responses
- **Volume:** per-request for single geocoding; batch endpoint for bulk CSV
  upload
- **Known quality issues:** fuzzy/best-effort address matching; rural or
  informally-formatted addresses may geocode poorly or not at all
- **Rate limit:** ~50 requests/second per IP (batch endpoint recommended for
  volume)
- **Owner:** IGN (Institut national de l'information géographique et
  forestière), via the Géoplateforme
- **Breaking-change risk going forward:** moderate — this is a service that
  already broke once during this project's timeline; monitor IGN's
  Géoplateforme changelog

## ADEME DPE (energy performance diagnostics)

- **URL:** dataset page `https://data.ademe.fr/datasets/dpe03existant`;
  API via Data Fair, e.g.
  `https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines`
  (filterable by postal code, surface, energy rating, period)
- **License:** Licence Ouverte / Etalab (standard for data.ademe.fr)
- **Update frequency:** updated regularly via the Data Fair platform; no
  fixed published cadence
- **Format:** REST JSON API (Data Fair), bulk CSV export also available
- **Volume:** 12M+ DPE records, covering diagnostics from July 2021 onward
  only (this dataset excludes the pre-reform DPE methodology)
- **Known quality issues:** DPE values are diagnostician-reported and can be
  inconsistent across providers; dataset only covers post-July-2021 reform,
  so historical joins before that date aren't possible with this source
- **Owner:** ADEME (Agence de la transition écologique)
- **Breaking-change risk:** moderate — Data Fair-hosted APIs can change
  dataset IDs/schema between catalog updates; no explicit stability
  guarantee published

## INSEE Filosofi (income / standard of living)

- **URL:** `https://www.insee.fr/fr/statistiques/6036907` (as referenced in
  the original brief); more recent vintage at
  `https://www.insee.fr/fr/statistiques/8229323` (2021, commune/IRIS level)
- **License:** INSEE open license (Licence Ouverte)
- **Update frequency:** annual, but with a 2-3 year publication lag (latest
  available vintage as of writing: 2021)
- **Format:** downloadable files — historically CSV/XLSX, newer
  distributions in Parquet; **not** a live REST API, batch download only
- **Volume:** municipality-level and 200m-grid ("carroyage") tables
- **Known quality issues:** small-municipality figures are suppressed
  ("secret statistique") below population thresholds — expect nulls for
  small communes; format has changed across vintages (CSV/XLSX → Parquet),
  so ingestion code must not hardcode one format
- **Owner:** INSEE
- **Breaking-change risk:** moderate — file format instability across
  vintages is a known, already-observed issue, not hypothetical

## geo.api.gouv.fr (administrative divisions)

- **URL:** `https://geo.api.gouv.fr/` — key endpoints: `/communes`,
  `/departements`, `/epcis`, `/regions`
- **License:** not explicitly published in available documentation —
  **unconfirmed**; treat as Etalab-equivalent pending verification, do not
  assume commercial-use clearance without checking further
- **Update frequency:** continuous, reflects INSEE's official geographic
  code (COG) updates
- **Format:** REST API, JSON or GeoJSON, WGS-84 coordinates
- **Volume:** small reference/lookup payloads, not bulk data
- **Known quality issues:** none significant identified
- **Owner:** data.gouv.fr / Etalab, built on INSEE's COG
- **Breaking-change risk:** low — stable public reference API; changelog
  tracked at the `datagouv/api-geo` GitHub repo

## Summary for Bronze ingestion design

| Source | Access pattern | Stability |
|---|---|---|
| BAN | Live REST API (geocoding) | **Already broke once this project** — use new URL |
| ADEME DPE | REST API (Data Fair) + bulk CSV | Moderate risk, schema could shift |
| INSEE Filosofi | Batch file download only | Format has already changed across vintages |
| geo.api.gouv.fr | Live REST API (reference data) | Low risk, stable |

Only BAN and ADEME DPE need live API clients with retry/error handling.
Filosofi is a periodic batch file fetch, not a polling API. geo.api.gouv.fr
is low-maintenance reference data, safe to cache aggressively.