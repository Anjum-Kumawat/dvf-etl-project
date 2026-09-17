# Silver DVF Data Dictionary & Column-Level Lineage

**Ticket:** RETL0-94 · **Owner:** Anjum Kumawat (Data Engineering) · **Scope:** dept 75, 2024 publication, pilot geography

## Overview

- Table: `public.silver_dvf` (PostgreSQL, database `dvf`)
- Row count: 67,072 (department 75, 2024 publication `2026-04`)
- Built by `src/silver/run_dvf_silver.py`, which orchestrates, in order: typing
  (RETL0-40) -> deduplication (RETL0-41) -> BAN join (RETL0-42) -> DPE join
  (RETL0-43) -> Filosofi join (RETL0-44) -> geo join (RETL0-45) -> quality
  checks (RETL0-11, gates the write) -> PostgreSQL write (`mode=overwrite`).
- 70 columns total: 39 DVF core columns, 11 BAN, 7 DPE, 7 Filosofi, 6 geo.

## Lineage summary (Bronze -> Silver)

| Source | Bronze path (MinIO, S3A) | Silver module(s) | Join key | Row relationship | Measured coverage |
|---|---|---|---|---|---|
| DVF | `bronze/dvf/publication=.../year=.../department=.../{dept}.csv.gz` | `dvf_schema.py`, `dvf_transform.py`, `dvf_dedup.py` | -- (base table) | 1 row per disposition line, post-dedup | 100% (base; 7,728/74,800 exact duplicates removed) |
| BAN | `bronze/ban/publication=.../year=.../department=.../{dept}.csv` | `dvf_ban_join.py` | `(adresse_numero, adresse_nom_voie, code_postal, code_commune)` | many DVF rows -> at most 1 BAN row (left join) | 99.8% (66,934 / 67,072) |
| DPE | `bronze/dpe/extraction_date=.../department=.../{dept}.jsonl` | `dvf_dpe_join.py` | `ban_result_id = identifiant_ban`, after reducing DPE to one most-recent record per address | many DVF rows -> at most 1 DPE summary row (left join) | 94.9% (63,668 / 67,072) |
| Filosofi | `bronze/filosofi/vintage=2021/communes.zip` (extracted from the zip at run time) | `dvf_filosofi_join.py` | `code_commune` | many DVF rows -> exactly 1 commune row (left join) | 100% (67,072 / 67,072) |
| geo.api.gouv.fr | `bronze/geo/extraction_date=.../department=.../{dept}.json` | `dvf_geo_join.py` | `parent_insee_commune(code_commune)` (derived; see below) | many DVF rows -> exactly 1 commune row (left join) | 100% (67,072 / 67,072) |

Every join above is a **left join**: DVF is always the left side, so a Silver
row is never dropped for lacking an enrichment match. Unmatched rows carry
NULLs in that source's columns instead. Row-count conservation across every
join stage is a HARD FAIL quality rule (RETL0-93, rule 4.2) checked on every
pipeline run.

## 1. DVF core columns (39 columns)

Full business definitions and Bronze-layer null-rate profiling for these
columns already exist in `docs/governance/bronze-data-dictionary.md`
(RETL0-90) -- not repeated here. This section documents only what Silver
adds on top: explicit typing (see `src/silver/dvf_schema.py` for the raw
Bronze read schema and `src/silver/dvf_transform.py` for the cast logic).

| Silver type | Columns |
|---|---|
| `text` (kept as string -- identifiers with meaningful leading zeros or free text, never numeric quantities) | `id_mutation`, `nature_mutation`, `adresse_suffixe`, `adresse_nom_voie`, `adresse_code_voie`, `code_postal`, `code_commune`, `nom_commune`, `code_departement`, `ancien_code_commune`, `ancien_nom_commune`, `id_parcelle`, `ancien_id_parcelle`, `numero_volume`, `lot1_numero`..`lot5_numero`, `code_type_local`, `type_local`, `code_nature_culture`, `nature_culture`, `code_nature_culture_speciale`, `nature_culture_speciale` |
| `integer` | `numero_disposition`, `adresse_numero`, `nombre_lots`, `nombre_pieces_principales` |
| `double precision` (comma decimal separator normalized to a dot before casting) | `valeur_fonciere`, `lot1_surface_carrez`..`lot5_surface_carrez`, `surface_reelle_bati`, `surface_terrain`, `longitude`, `latitude` |
| `date` | `date_mutation` |

Blank strings (`""`) in the raw Bronze CSV are normalized to SQL NULL before
any cast, uniformly across all 39 columns (`dvf_transform.py`).

## 2. BAN columns (11 columns, RETL0-42)

Source: IGN Géoplateforme batch geocoding of unique DVF addresses
(`src/ingestion/enrichment/ban_ingest.py`). `id` in the Bronze CSV is an
artifact of that single extraction run and is not carried into Silver; the
join key is the 4-column address tuple instead.

| Column | Type | Bronze source field | Description |
|---|---|---|---|
| `ban_longitude` | double | `longitude` | BAN's independently geocoded longitude -- kept alongside DVF's own `longitude` as a deliberate cross-check, not a replacement |
| `ban_latitude` | double | `latitude` | Same, for latitude |
| `ban_result_score` | double | `result_score` (rounded to 4dp at ingestion to stabilize checksums -- RETL0-36) | Match confidence, 0-1. Rule 3.4 (RETL0-93) flags rows below 0.5 |
| `ban_result_label` | text | `result_label` | Human-readable matched address, e.g. "4 Villa Perreur 75020 Paris" |
| `ban_result_id` | text | `result_id` | IGN's unique BAN address identifier, e.g. "75120_7288_00004" -- this is the join key DPE uses in section 3 below |
| `ban_result_housenumber` | text | `result_housenumber` | Matched house number |
| `ban_result_street` | text | `result_street` | Matched street name |
| `ban_result_postcode` | text | `result_postcode` | Matched postal code |
| `ban_result_city` | text | `result_city` | Matched city name |
| `ban_result_citycode` | text | `result_citycode` | Matched INSEE/fiscal commune code per BAN (a cross-check against DVF's own `code_commune`) |
| `ban_result_status` | text | `result_status` | `"ok"` for a real match; used as the matched/unmatched indicator column throughout the pipeline and in RETL0-93's join-coverage check |

**Limitation:** 138 Silver rows have no BAN match. Root cause: `ban_ingest.py`'s
`extract_unique_addresses()` skips any DVF row with a blank street name (pure
land/culture parcels with no building address) -- these were never sent to
the geocoding API at all, not a geocoding failure.

## 3. DPE columns (7 columns, RETL0-43)

Source: ADEME's public DPE dataset (`src/ingestion/enrichment/dpe_ingest.py`).

| Column | Type | Bronze source field | Description |
|---|---|---|---|
| `dpe_numero_dpe` | text | `numero_dpe` | Unique diagnostic report ID -- kept for traceability back to the specific DPE record chosen |
| `dpe_etiquette_dpe` | text | `etiquette_dpe` | Energy performance label, A (best) to G (worst) |
| `dpe_etiquette_ges` | text | `etiquette_ges` | Greenhouse-gas emissions label, A to G |
| `dpe_surface_habitable_logement` | double | `surface_habitable_logement` | Habitable surface area of the diagnosed dwelling (m²) |
| `dpe_type_batiment` | text | `type_batiment` | Building type per ADEME (e.g. "appartement") |
| `dpe_periode_construction` | text | `periode_construction` | Construction period bracket (e.g. "avant 1948") |
| `dpe_date_etablissement_dpe` | date | `date_etablissement_dpe` | Date the diagnostic was issued -- the field `most_recent_per_address()` sorts on to pick one record per address |

**Important limitation (found before writing this join, not assumed):**
department 75 has 845,261 DPE records across only 70,777 unique `identifiant_ban`
values -- a BAN address ID identifies a building entrance, and DPE diagnostics
exist per DWELLING UNIT within it (the busiest single address in dept 75 has
3,621 separate DPE records). DVF/BAN data carries no per-unit key (floor,
door number) that would let this pipeline pick the exact diagnostic for the
specific unit sold in a given mutation. These 7 columns should therefore be
read as **the most recent known energy profile for the building**, not as
certified for the specific unit in that Silver row. See
`src/silver/dvf_dpe_join.py`'s module docstring and
`scripts/dpe_duplicate_check.py` for the full investigation.

## 4. Filosofi columns (7 columns, RETL0-44)

Source: INSEE Filosofi 2021 vintage, disposable-income indicators
(`src/ingestion/enrichment/filosofi_ingest.py`,
`FILO2021_DISP_COM.csv` specifically -- one of six files in the archive; the
other five (DEC, two \*_PAUVRES, two TRDECILES variants) are out of MVP scope).

| Column | Type | Bronze source field | Description |
|---|---|---|---|
| `filosofi_nb_menages` | integer | `NBMEN21` | Number of tax households in the commune |
| `filosofi_nb_personnes` | integer | `NBPERS21` | Number of persons in the commune |
| `filosofi_revenu_median` | double | `Q221` | Median disposable income per consumption unit (€/year) -- the headline indicator |
| `filosofi_gini_index` | double | `GI21` | Gini coefficient of disposable income (0 = perfect equality, 1 = maximal inequality) |
| `filosofi_s80_s20_ratio` | double | `S80S2021` | Ratio of income held by the top 20% to the bottom 20% |
| `filosofi_interdecile_ratio` | double | `RD` | D9/D1 interdecile ratio |
| `filosofi_pct_revenu_social` | double | `PPSOC21` | % of disposable income from social benefits |

**Limitation (data freshness, not a pipeline defect):** 2021 is the last
vintage INSEE has published -- production of the 2022 vintage was cancelled
because the abolition of France's *taxe d'habitation* broke Filosofi's method
for linking tax households to a dwelling, and INSEE judged the replacement
sourcing statistically unreliable. This income data will run 2-3 years stale
relative to the 2024 DVF transactions it's joined against. Documented in
`filosofi_ingest.py`'s module docstring; belongs in the final report's
limitations section.

**Correction to an earlier assumption:** prior documentation (`geo_ingest.py`,
RETL0-39) suggested Paris's fiscal arrondissement codes would need mapping to
the canonical INSEE commune code here too. Checked directly against the real
file before writing this join: Filosofi already lists Paris by arrondissement
(`75101`..`75120`, each its own row), so no such mapping was needed for this
source -- see the mismatch that *does* apply, in section 5.

## 5. geo.api.gouv.fr columns (6 columns, RETL0-45)

Source: geo.api.gouv.fr administrative reference data
(`src/ingestion/enrichment/geo_ingest.py`).

| Column | Type | Bronze source field | Description |
|---|---|---|---|
| `geo_commune_nom` | text | `communes[].nom` | Official commune name |
| `geo_commune_population` | bigint | `communes[].population` | Commune population |
| `geo_epci_nom` | text | `communes[].epci.nom` | Name of the commune's EPCI (inter-communal body), e.g. "Métropole du Grand Paris" |
| `geo_centre_longitude` | double | `communes[].centre.coordinates[0]` | Commune centroid longitude (whole-commune centroid, not per-arrondissement) |
| `geo_centre_latitude` | double | `communes[].centre.coordinates[1]` | Commune centroid latitude |
| `geo_region_nom` | text | `region.nom` | Region name, e.g. "Île-de-France" |

**Real join-key mismatch (confirmed, not assumed):** geo.api.gouv.fr returns
exactly ONE commune for `codeDepartement=75` -- code `75056` ("Paris",
population 2,103,778) -- it has no knowledge of the fiscal arrondissement
codes (`75101`-`75120`) that DVF/BAN/DPE/Filosofi all use. Paris, Lyon, and
Marseille are each a single INSEE commune split into fiscal arrondissements
only for tax/electoral purposes. The join therefore uses a derived key,
`parent_insee_commune()`, mapping `751xx -> 75056`, `692xx -> 69123` (Lyon),
`132xx -> 13055` (Marseille) -- the latter two are documented but not present
in this project's dept-75 pilot data. Every dept-75 row resolves to the same
single geo row, which is why coverage is 100% and why all 6 columns carry
identical values across every arrondissement (verified via `psql` -- see
RETL0-45).

## Consolidated known limitations

| # | Limitation | Source | Severity |
|---|---|---|---|
| 1 | 7,728 exact full-row duplicates existed in typed Bronze data before dedup (10.5%) | DVF (DGFiP export quirk) | Fixed by RETL0-41; documented as a real finding, not silently absorbed |
| 2 | 138 rows have no BAN match (land parcels with no street address) | BAN | Expected, by design of `extract_unique_addresses()` |
| 3 | DPE columns are building-level, not verified unit-level | DPE | MVP-scope approximation, documented in `dvf_dpe_join.py` |
| 4 | Filosofi income data is the 2021 vintage (2-3 years stale vs. 2024 transactions) | Filosofi | INSEE-side data availability limitation, not fixable by this project |
| 5 | geo.api.gouv.fr columns are identical across all Paris arrondissements (city-wide, not per-arrondissement) | geo | Inherent to the source; derived parent-commune join is the correct handling, not a workaround for a defect |

See `docs/governance/quality-rules.md` (RETL0-93) for the quantitative
quality rules built on top of this data, and `src/silver/dvf_quality_checks.py`
(RETL0-11) for their implementation.
