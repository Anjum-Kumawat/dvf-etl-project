# Gold Layer Column-Level Lineage

**Ticket:** RETL0-95 · **Owner:** Anjum Kumawat (Data Engineering) · **Scope:** dept 75, 2024

## Overview

Two Gold tables, both built by `src/gold/run_gold.py` from `silver_dvf`
(PostgreSQL), via `src/gold/price_aggregates.py` (RETL0-46) and
`src/gold/department_rollup.py` (RETL0-47). Both share the exact same
filtering and mutation-level reduction logic (`_filtered_priced()` in
`price_aggregates.py`) -- `department_rollup.py` imports and reuses it
rather than reimplementing it, so the two tables cannot drift apart.

| Table | Grain | Row count |
|---|---|---|
| `gold_price_by_municipality_quarter` | (code_commune, quarter) | 80 (20 communes x 4 quarters) |
| `gold_price_by_department_quarter` | (code_departement, quarter) | 4 (1 department x 4 quarters) |

## Shared pipeline (applies to every column below)

1. **Row-level filter** on `silver_dvf`: `nature_mutation = 'Vente'` AND
   `surface_reelle_bati IS NOT NULL AND > 0` AND
   `valeur_fonciere IS NOT NULL AND > 0`.
2. **Mutation-level reduction** (`_mutation_level()`): group the filtered
   rows by `(id_mutation, code_commune, code_departement, date_mutation)`
   and reduce to one row per mutation:
   `valeur_fonciere = FIRST(valeur_fonciere)`,
   `surface_reelle_bati = SUM(surface_reelle_bati)`.

   **Why this step exists (the critical finding behind RETL0-46/47):** DVF
   repeats the full transaction value on EVERY disposition row of a
   multi-lot mutation (a single sale spanning several units), rather than
   allocating it. Confirmed against real data: mutation `2024-1202784` in
   dept 75 has 40 disposition rows, each carrying the identical
   `valeur_fonciere = 94000000`, with individual `surface_reelle_bati`
   from 6 to 950 sqm. Computing price per sqm per disposition row (before
   this fix) produced values as high as ~15,700,000 EUR/sqm and inflated
   transaction counts by treating one real sale as 40. Summing surface
   across a mutation's rows before dividing, and keeping the value once,
   is the only correct approach.
3. **Derived columns**, computed once per mutation:
   `price_per_sqm = valeur_fonciere / surface_reelle_bati`;
   `quarter = CONCAT(YEAR(date_mutation), '-Q', QUARTER(date_mutation))`.
4. **Final aggregation**: `GROUP BY (code_commune, quarter)` for
   `gold_price_by_municipality_quarter`, or
   `GROUP BY (code_departement, quarter)` for
   `gold_price_by_department_quarter`.

## Column-level lineage

| Gold column | Source column(s) | Filters applied | Transformation / aggregation |
|---|---|---|---|
| `code_commune` (municipality table only) | `silver_dvf.code_commune` | none beyond the shared row filter | Group-by key, passed through unchanged |
| `code_departement` (department table only) | `silver_dvf.code_departement` | none beyond the shared row filter | Group-by key, passed through unchanged |
| `quarter` | `silver_dvf.date_mutation` | none beyond the shared row filter | `CONCAT(YEAR(date_mutation), '-Q', QUARTER(date_mutation))`, computed at mutation grain (step 3), then group-by key (step 4) |
| `nb_transactions` | `silver_dvf.id_mutation` (implicitly, via the mutation-level reduction) | shared row filter + mutation-level reduction (step 2) | `COUNT(*)` over the mutation-level rows within the group -- counts actual transactions, not disposition lines |
| `median_price_per_sqm` | `silver_dvf.valeur_fonciere`, `silver_dvf.surface_reelle_bati`, `silver_dvf.id_mutation` | shared row filter + mutation-level reduction (step 2) | `price_per_sqm = valeur_fonciere / surface_reelle_bati` at mutation grain (step 3), then `PERCENTILE_APPROX(price_per_sqm, 0.5)` within the group (step 4). Spark has no exact median aggregate; `percentile_approx` is acceptable at this row-count scale. |
| `avg_price_per_sqm` | same as `median_price_per_sqm` | same as `median_price_per_sqm` | `AVG(price_per_sqm)` within the group. Known to be more outlier-sensitive than the median -- see Known Limitations below. |
| `median_valeur_fonciere` | `silver_dvf.valeur_fonciere`, `silver_dvf.id_mutation` | shared row filter + mutation-level reduction (step 2, `FIRST(valeur_fonciere)`) | `PERCENTILE_APPROX(valeur_fonciere, 0.5)` within the group, computed on the one value per mutation (not re-summed or re-divided) |
| `avg_valeur_fonciere` | same as `median_valeur_fonciere` | same as `median_valeur_fonciere` | `AVG(valeur_fonciere)` within the group |
| `avg_surface_reelle_bati` | `silver_dvf.surface_reelle_bati`, `silver_dvf.id_mutation` | shared row filter + mutation-level reduction (step 2, `SUM(surface_reelle_bati)`) | `AVG(surface_reelle_bati)` within the group, computed on the per-mutation SUMMED surface (a multi-lot mutation's total floor area), not the individual disposition-row surfaces |

## Known limitations (carried from Silver, still apply at Gold)

Per `docs/governance/silver-data-dictionary.md` (RETL0-94), every Gold row
inherits whatever Silver limitations apply to the underlying rows -- Gold
adds no new enrichment, only aggregation. Most relevant here:

- **`avg_price_per_sqm` is outlier-sensitive.** RETL0-93's quality rules
  flagged 2,405 Silver rows (3.6%) with `valeur_fonciere > EUR 10,000,000`.
  Even after the multi-lot mutation fix, a handful of very high-value
  mutations can still pull a quarter's average upward. `median_price_per_sqm`
  is the more robust statistic and should be the primary reported figure on
  dashboards (RETL0-51); `avg_price_per_sqm` is retained for completeness
  and for comparison, not as the headline number.
- **`nb_transactions` reflects only the filtered, mutation-reduced
  population** -- it is not a full count of every DVF disposition row for
  that commune/quarter (`silver_dvf` has more rows than distinct mutations,
  by design; see RETL0-41's dedup and RETL0-46's mutation-level reduction).
- **Both tables cover dept 75 only** (this project's pilot scope) --
  `gold_price_by_department_quarter` currently rolls up to a single
  department because only one department has been ingested, not because
  the logic is dept-75-specific.