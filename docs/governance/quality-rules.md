# Silver DVF Quality Rules

**Ticket:** RETL0-93 · **Owner:** Anjum Kumawat (Data Engineering) · **Scope:** dept 75, 2024 publication, dept-75 pilot

## Purpose

Per the Data Governance epic, quality rules are defined here, as business
statements grounded in real observed data, before RETL0-11 implements any
automated check against them. Every threshold below was measured directly
against the actual `silver_dvf` table (67,072 rows, department 75, 2024) --
none are assumed or copied from generic data-quality checklists. Queries
used to produce these numbers are reproducible via `psql` against the
running `dvf` database.

Each rule has a **severity**:
- **HARD FAIL** -- pipeline should stop / row should be rejected. Reserved
  for invariants that currently hold with zero exceptions in real data, so
  any violation signals a genuine upstream problem (wrong file, broken
  join, parsing bug), not a normal edge case.
- **WARNING** -- row is flagged, not dropped. Used where real data shows a
  small but legitimate population of edge cases that deserve review, not
  exclusion.

## 1. Completeness

### 1.1 valeur_fonciere presence
99.9% non-null (66,979 / 67,072). The 93 nulls are a real, small population
in the source DVF export (not caused by our pipeline). **WARNING**: flag
rows with a null valeur_fonciere for manual review; do not drop them, since
`Echange` (exchange) mutations can legitimately lack a sale price.

### 1.2 Built-property surface
Rows where `type_local` is `Appartement` or `Maison` should have
`surface_reelle_bati > 0`. Measured: 2 violations out of 31,728 such rows
(0.006%). **WARNING**, not HARD FAIL -- the population is real but tiny,
consistent with a rare DGFiP data gap rather than a pipeline defect.

## 2. Validity (categorical / format)

### 2.1 nature_mutation must be a known category
Observed values in 67,072 rows: `Vente` (65,946), `Echange` (599),
`Vente en l'état futur d'achèvement` (334), `Adjudication` (177),
`Vente terrain à bâtir` (16). **HARD FAIL** if any row has a
`nature_mutation` outside this set of 5 -- DGFiP publishing an unseen
category should stop the pipeline for review, not pass silently.

### 2.2 type_local must be a known category or null
Observed: `Appartement` (31,591), `Dépendance` (30,215),
`Local industriel. commercial ou assimilé` (4,538), NULL (591, legitimate
for pure land/culture parcels with no building), `Maison` (137).
**HARD FAIL** if any row has a non-null `type_local` outside this set.

### 2.3 code_postal format
Must match `^75[0-9]{3}$` for this pilot scope. Measured: 0 violations across
all 67,072 rows. **HARD FAIL** on any violation -- this would indicate the
department filter broke somewhere upstream, not a legitimate edge case.

### 2.4 date_mutation range
Must fall within `[2024-01-01, 2024-12-31]` for the 2024/dept-75 publication
currently loaded. Measured range: `2024-01-02` to `2024-12-31`, exactly as
expected. **HARD FAIL** on any row outside this range -- would indicate the
wrong Bronze file was read or a date-parsing bug in RETL0-40's typing.

## 3. Range / outlier detection

### 3.1 valeur_fonciere must be positive
Measured: 0 rows with `valeur_fonciere <= 0` (min observed = 1). **HARD
FAIL** on any zero or negative value -- unlike a low value, this has zero
legitimate justification and zero real occurrences, so any future
occurrence is a parsing or source defect.

### 3.2 Low-value outliers
358 rows (0.5%) have `valeur_fonciere < €1,000`. Some are legitimate
symbolic transfers between family members (a well-documented French
property-law practice, "vente à l'euro symbolique"); others may be
data-entry artifacts or partial-lot price allocations. **WARNING**: flag
for review, do not exclude -- cannot distinguish the two cases from this
column alone without inspecting notarial context we don't have.

### 3.3 High-value outliers
2,405 rows (3.6%) exceed €10,000,000; the single maximum is €255,000,000.
Plausible for Paris commercial/portfolio real estate, not implausible on
its face. **WARNING**: flag values above €10M for review before use in any
downstream statistical model (e.g. hedonic pricing), since a handful of
extreme values can distort regression coefficients even when individually
legitimate.

### 3.4 BAN geocoding confidence
Of 66,918 BAN-matched rows, 173 (0.26%) have `ban_result_score < 0.5`.
**WARNING**: rows below this threshold should not be trusted for precise
spatial analysis (e.g. distance calculations, map plotting) even though
they remain usable for non-spatial analysis. Threshold of 0.5 follows the
IGN Géoplateforme's own documented convention for "likely match" vs.
"uncertain match."

## 4. Consistency (pipeline invariants)

### 4.1 No exact full-row duplicates
RETL0-41 removed 7,728 exact duplicates (74,800 -> 67,072). **HARD FAIL**
if any exact full-row duplicate is found in the final Silver table -- this
is a regression check on the dedup step itself, not a new discovery.

### 4.2 Row-count conservation across left joins
Each of the four enrichment joins (BAN, DPE, Filosofi, geo) is a left join
against a Silver DVF dataframe and must not change the row count. Verified
manually at every stage during RETL0-42 through RETL0-45 (67,072 in,
67,072 out, every time). **HARD FAIL** if any join stage's output row
count differs from its input -- would indicate an enrichment source
stopped being unique on its join key (e.g. BAN or Filosofi somehow
returning duplicate rows per key), silently multiplying Silver rows.

## 5. Join / enrichment coverage minimums

These are regression floors for future runs (new departments, new
publications), not fixed expectations that every source must hit 100%:

| Source    | Current match rate | Floor        | Rationale |
|-----------|--------------------|--------------|-----------|
| BAN       | 99.8% (66,934/67,072) | >= 99%     | Only rows with no street address (land parcels) legitimately fail to geocode; a drop below 99% would suggest an API or extraction problem. |
| DPE       | 94.9% (63,668/67,072) | >= 90%     | No firm 100% ceiling expected -- DPE coverage genuinely varies by building age and whether a diagnostic has ever been filed. A floor, not a target. |
| Filosofi  | 100% (67,072/67,072)  | = 100% for dept 75 | All 20 Paris arrondissements are present in Filosofi; any drop indicates a broken commune-code filter or a missing vintage. |
| geo       | 100% (67,072/67,072)  | = 100% for dept 75 | Every dept-75 code_commune resolves to 75056 via the documented mapping; any drop indicates a broken mapping function. |

## Out of scope for this MVP

Row-level cross-field consistency checks (e.g. surface_reelle_bati
plausible relative to nombre_pieces_principales), DPE label plausibility
checks, and outlier detection using department-wide statistical methods
(e.g. IQR-based) are not covered here -- flagged as follow-up work if time
allows, not silently dropped.