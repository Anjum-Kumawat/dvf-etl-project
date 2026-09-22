# RETL0-9: Predictive Model — Design Specification

**Ticket:** RETL0-9 (backlog) · **Status:** unlocked per ticket's own gate — "remains in
the product backlog until the complete data engineering pipeline is running end to
end, documented and reproducible through Metabase delivery," which RETL0-54 closed
out.

Per the ticket's own framing, predictive modeling is optional and not part of the
core delivery path. This document satisfies the ticket's literal "Done when" bar: it
specifies the 8 required elements below, grounded in the real data currently in
`silver_dvf` (verified via psql against the live Postgres instance, not assumed). It
does not implement a model — that would be new scope beyond what this ticket asks
for.

## 1. Prediction target

**Price per square metre** (`valeur_fonciere / surface_reelle_bati`), not raw
`valeur_fonciere`. Two reasons: it's comparable across property sizes (a 20m² studio
and a 120m² house have wildly different raw prices for reasons a model shouldn't
have to relearn), and it's consistent with how this project already frames price
everywhere else — the Gold tables (`gold_price_by_municipality_quarter`,
`gold_price_by_department_quarter`) and all three Metabase dashboards already use
price-per-m² as the unit of comparison.

## 2. Eligible property types

Two filters, both applied to `silver_dvf`:

- `nature_mutation = 'Vente'` only. Real distribution (all 4 departments, 2024):
  Vente 168,313 · Vente en l'état futur d'achèvement (VEFA, off-plan) 24,757 ·
  Echange 957 · Adjudication 657 · Expropriation 156 · Vente terrain à bâtir 71.
  VEFA (off-plan new-builds) is excluded — it's priced against a future
  completion date, a different regime than a standard resale, and mixing the two
  would blur what the model is actually learning. Echange, Adjudication and
  Expropriation are non-arm's-length or forced transactions, not representative
  market prices.
- `type_local IN ('Appartement', 'Maison')` only. Real distribution across all
  `Vente` rows: Dépendance 84,124 (parking spots, cellars — no living surface, not
  a "property" in the sense being priced) · Appartement 70,325 · blank/unclassified
  21,361 · Local industriel/commercial 9,962 (different market entirely) · Maison
  9,139.

**Net eligible population: 75,169 rows** (66,141 apartments + 9,028 houses),
verified via a real query against the live table.

## 3. Candidate features

Checked real non-null coverage over the 75,169 eligible rows before proposing
anything, rather than assuming a column is usable:

| Feature | Coverage | Notes |
|---|---|---|
| `surface_reelle_bati` | 100% (75,166/75,169) | living surface, m² |
| `nombre_pieces_principales` | 100% | room count |
| `type_local` | 100% | categorical, 2 levels here |
| `code_commune` / `geo_commune_nom` | 100% | 143 distinct communes across the 4 departments |
| `ban_latitude` / `ban_longitude` | 99.8% (75,019/75,169) | point-level geocoding — enables distance-to-Paris-center or similar spatial features |
| `dpe_etiquette_dpe` | 93.9% (apartments) / 80.3% (houses) | energy label, A–G categorical; matches this project's existing WARNING-level coverage floor (70%), so a model should treat "missing DPE" as its own category rather than dropping ~10–20% of rows |
| `geo_commune_population` | 100% | commune-level density proxy |
| `filosofi_revenu_median` | 100% | commune-level median household income — a real proxy for neighborhood price level |
| `filosofi_gini_index`, `filosofi_s80_s20_ratio` | 100% | inequality measures, secondary candidates |
| `date_mutation` | 100% | sale date — source for a month/quarter seasonality feature |
| `nombre_lots` | high | co-property size signal |

**Explicitly excluded:** `ban_result_score`, `ban_result_status`, and other
`ban_result_*` join-quality fields — these describe how well the BAN enrichment
matched, not anything about the property itself, and including them would let the
model pick up on enrichment-pipeline artifacts rather than real signal.

## 4. Training period

**2024-01-02 through 2024-09-30** (first three quarters).

Real constraint driving this: the dataset covers exactly one calendar year
(`MIN(date_mutation) = 2024-01-02`, `MAX(date_mutation) = 2024-12-31` — verified),
so there's no multi-year history to build a conventional backtest across years. A
chronological in-year split is the only sound option.

## 5. Test period

**2024-10-01 through 2024-12-31** (final quarter, ~25% of the year by time).

A strictly chronological split — not random k-fold — because this is time-ordered
real estate data: a random split would let information about a given month's
neighborhood price level leak between train and test (e.g., two nearby sales one
week apart landing on opposite sides of a random split), overstating how well the
model would actually perform when used going forward. A time-based holdout also
matches how the model would really be used: predicting near-future prices from
past ones.

## 6. Evaluation metrics

- **MAE** (mean absolute error, in €/m²) — primary metric, easy to interpret
  ("the model is off by €X/m² on average"), robust to a handful of outlier sales.
- **RMSE** — penalizes large individual misses more heavily; worth tracking
  alongside MAE since a model that's usually close but occasionally very wrong is a
  different (worse) failure mode than one that's consistently a little off.
- **MAPE** (mean absolute percentage error) — price levels vary a lot across these
  4 departments (75 is a different market than 93), so a percentage-based metric is
  more comparable across the full eligible population than a raw € figure.
- **R²** — standard baseline sanity check for how much variance is explained.

## 7. Leakage risks

- **DPE timing.** `dpe_date_etablissement_dpe` is the date the energy diagnostic
  was produced, which is not guaranteed to precede `date_mutation` — DPE joins in
  this pipeline are matched by nearest available record, not filtered to
  pre-sale-only. Using DPE label as a feature without checking
  `dpe_date_etablissement_dpe <= date_mutation` risks leaking a diagnostic that
  didn't exist yet at time of sale. Any implementation must filter on this before
  using DPE as a feature, or accept and document the risk explicitly if it can't be
  cleanly filtered.
- **Filosofi vintage.** `filosofi_revenu_median` and related fields are a fixed
  2021-vintage INSEE snapshot (established earlier in this project, see
  `docs/governance/quality-rules.md`), not a per-transaction figure. Safe to use —
  it predates all 2024 sales — but must be documented as a static neighborhood
  characteristic, not a live signal, since it won't reflect any income change
  between 2021 and 2024.
- **High-cardinality commune memorization.** Using `code_commune` directly as a
  categorical feature (143 levels) risks the model just memorizing each commune's
  average price rather than learning generalizable structure, especially for
  communes with few sales. Including `geo_commune_population` and
  `filosofi_revenu_median` as smoother, continuous proxies alongside (or instead
  of) a raw commune dummy mitigates this.
- **Enrichment-quality fields as features.** Covered in section 3 — `ban_result_*`
  quality/status fields must be excluded, not used as predictors.
- **Train/test chronology.** Covered in sections 4–5 — a random split would leak
  temporal/neighborhood signal across the boundary; the chronological split avoids
  this by construction.

## 8. Baseline model

Two tiers, both simple and standard:

- **Naive baseline (the bar any real model must clear):** predict, for each test-set
  sale, that commune's median price-per-m² from the training period. This costs
  nothing to compute and represents "what you'd guess with the Metabase dashboards
  already built in RETL0-51 and no model at all." Any trained model that doesn't
  beat this on MAE isn't worth deploying.
- **Reference model:** gradient-boosted trees (e.g. LightGBM or XGBoost) on the
  features in section 3. Chosen over plain linear regression as the first real
  baseline because it handles the categorical features (`type_local`,
  `dpe_etiquette_dpe`, commune) and likely non-linear relationships (e.g. surface
  vs. price is rarely linear) without manual feature engineering, and is what a
  team would realistically reach for first in practice.

## Not in scope for this document

Per this ticket's actual "Done when" criteria, the above is the full deliverable.
Feature engineering code, model training, evaluation runs, a predictions table in
Postgres, and a Metabase view of predicted-vs-actual are not part of RETL0-9 as
written, and were not built as part of closing this ticket.
