# Bronze Ingestion Contracts — Enrichment Sources

One section per enrichment source. Verified against source documentation as
of August 2026. This is a preparation/contract document — it does not
implement any ingestion client.

## Bronze principle

Bronze must preserve source data as received wherever technically possible.
Cleaning, normalization and business joins belong to later Silver
processing — nothing in this document implies a Silver transformation.

---

## BAN (Base Adresse Nationale)

- Official source name: Base Adresse Nationale (BAN)
- Source/provider: IGN, via the Géoplateforme (previously hosted directly
  under data.gouv.fr/Etalab)
- Access URL/API endpoint: `https://data.geopf.fr/geocodage/search/`
  (single address), `https://data.geopf.fr/geocodage/batch/` (bulk)
- **Note:** the old endpoint `api-adresse.data.gouv.fr` was decommissioned
  end of January 2026 and no longer resolves — use the URL above
- Access method: REST API, GET for single search, file upload (POST) for
  batch geocoding
- Response format: JSON / GeoJSON
- Expected update frequency: continuous — BAN is a live address database
- Geographic granularity: address/parcel point level (returns lat/lon plus
  the containing commune)
- Important request parameters: `q` (address query string), `postcode`,
  `citycode`, `limit`; batch endpoint takes a CSV of addresses as input
- License: Etalab Open License (Licence Ouverte)
- Expected Bronze representation: raw JSON response per geocoding batch
  run, stored unchanged
- Proposed Bronze object-key convention: `ban/{ingestion_date}/batch_{n}.json`
  — no natural year/department split since this is request-driven, not a
  periodic published dataset; partition by ingestion run date instead
- Known access limitations: ~50 requests/second/IP rate limit on the
  single-search endpoint (use batch for volume); this service already broke
  once during this project's timeline — monitor for further changes
- Source identifiers useful for joins: `citycode` (INSEE commune code) —
  direct join key to DVF's commune code field

## ADEME DPE (energy performance diagnostics)

- Official source name: DPE — Logements existants (post-July 2021)
- Source/provider: ADEME (Agence de la transition écologique)
- Access URL/API endpoint: dataset page
  `https://data.ademe.fr/datasets/dpe03existant`; API via Data Fair, e.g.
  `https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines`
  (confirm exact dataset ID at ingestion time — catalog IDs can shift)
- Access method: REST API (Data Fair, query-parameter filtered); bulk CSV
  export also available
- Response format: JSON (API) or CSV (bulk export)
- Expected update frequency: updated regularly, no fixed published cadence
- Geographic granularity: per-dwelling record, includes commune, postal
  code, address (not pre-geocoded to lat/lon)
- Important request parameters: `code_postal`,
  `code_insee_commune_actualise`, `surface_habitable_logement`,
  `etiquette_dpe`, `date_etablissement_dpe`
- License: Licence Ouverte / Etalab
- Expected Bronze representation: raw JSON pages or CSV extract per
  ingestion run, stored unchanged
- Proposed Bronze object-key convention:
  `dpe/{extraction_date}/page_{n}.json` — no natural year/department split
  since this is a rolling dataset filtered by request; partition by
  extraction date
- Known access limitations: dataset only covers diagnostics from July 2021
  onward (post-reform methodology) — no historical DPE data before that
  date is available from this source; diagnostician-reported values can be
  inconsistent; Data Fair dataset ID/schema can shift between catalog
  updates
- Source identifiers useful for joins: `code_insee_commune_actualise`
  (INSEE commune code) — direct join key to DVF's commune code;
  `numero_dpe` as the unique DPE record id; address fields as a fallback
  join path via BAN geocoding

## INSEE Filosofi (income / standard of living)

- Official source name: Filosofi (Fichier localisé social et fiscal) —
  Revenus, pauvreté et niveau de vie
- Source/provider: INSEE
- Access URL: `https://www.insee.fr/fr/statistiques/8229323` (2021
  commune/IRIS vintage — the most recent available as of writing); grid
  ("carroyage") data at
  `https://www.data.gouv.fr/datasets/revenus-pauvrete-et-niveau-de-vie-donnees-carroyees`
- Access method: batch file download only — **not** a live REST API
- Response/file format: historically CSV/XLSX; more recent vintages
  distributed in Parquet
- Expected update frequency: annual, but with a 2-3 year publication lag
  (latest available vintage as of writing: 2021)
- Geographic granularity: commune level, IRIS level for communes with
  ≥5,000 inhabitants, and a 200m grid ("carroyage") for finer resolution
- Important request parameters: none — static file download, but the
  correct vintage year/file/sheet must be selected manually per release
- License: INSEE open license (Licence Ouverte)
- Expected Bronze representation: raw downloaded file stored unchanged, in
  whatever format that vintage was published (CSV/XLSX/Parquet)
- Proposed Bronze object-key convention:
  `filosofi/{reference_year}/commune.{ext}` — partitioned by the income
  reference year, matching DVF's year-based partitioning
- Known access limitations: small-commune values are suppressed for
  privacy ("secret statistique") — expect nulls below population
  thresholds; file format has already changed across vintages (CSV/XLSX to
  Parquet), so ingestion code must not hardcode one format; significant lag
  between reference year and publication means the latest DVF year may not
  have a matching Filosofi vintage yet
- Source identifiers useful for joins: commune code (`CODGEO`) — direct
  join key to DVF's commune code field

## geo.api.gouv.fr (administrative divisions)

- Official source name: API Découpage administratif
- Source/provider: data.gouv.fr / Etalab, built on INSEE's official
  geographic code (COG)
- Access URL/API endpoint: `https://geo.api.gouv.fr/communes` (also
  `/departements`, `/epcis`, `/regions`)
- Access method: REST API, GET requests
- Response format: JSON or GeoJSON
- Expected update frequency: continuous, reflects INSEE COG updates
- Geographic granularity: commune / EPCI / department / region reference
  records — administrative boundary lookup data, not property-level
- Important request parameters: `code` (commune/department code), `nom`
  (name search), `fields` (which fields to return), `format=json|geojson`,
  `geometry` (contour vs. centre point)
- License: not explicitly published in available documentation —
  **unconfirmed**; treat as Etalab-equivalent pending verification, do not
  assume commercial-use clearance without checking further
- Expected Bronze representation: raw JSON response snapshot per reference
  pull — this is low-volume reference data, reasonable to refresh
  periodically (e.g. monthly) rather than per DVF ingestion run
- Proposed Bronze object-key convention:
  `geo_admin/{snapshot_date}/communes.json` (and equivalent files for
  departements/epcis/regions) — partitioned by snapshot date since this is
  periodically-refreshed reference data, not tied to DVF year/department
- Known access limitations: none significant identified — stable, low-risk
  public reference API; changelog tracked at the `datagouv/api-geo` GitHub
  repo
- Source identifiers useful for joins: `code` (INSEE commune code) — direct
  join key to DVF's commune code; also `codeDepartement`, `codeRegion`,
  `codesPostaux` for cross-referencing at other grains

## Summary

| Source | Access pattern | Natural partition key |
|---|---|---|
| BAN | Live REST API (geocoding) | Ingestion run date (request-driven) |
| ADEME DPE | REST API (Data Fair) + bulk CSV | Extraction date (rolling dataset) |
| INSEE Filosofi | Batch file download only | Reference year (annual, lagged) |
| geo.api.gouv.fr | Live REST API (reference data) | Snapshot date (low-frequency refresh) |

Only BAN and ADEME DPE need live API clients with retry/error handling.
Filosofi is a periodic batch file fetch, not a polling API. geo.api.gouv.fr
is low-maintenance reference data, safe to cache aggressively.