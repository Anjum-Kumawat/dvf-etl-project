\# Gold DVF Data Dictionary



\*\*Tickets:\*\* RETL0-46 (municipality grain), RETL0-47 (department grain) ·

\*\*Scope:\*\* departments 75, 92, 93, 94, 2024 publication



\## Overview



Two Gold fact tables, both built by `src/gold/run\_gold.py` from `silver\_dvf`:



\- `public.gold\_price\_by\_municipality\_quarter` — grain: one row per

&#x20; `(code\_commune, quarter)`

\- `public.gold\_price\_by\_department\_quarter` — grain: one row per

&#x20; `(code\_departement, quarter)`



Both are produced by the exact same shared pipeline

(`src/gold/price\_aggregates.py`'s `\_filtered\_priced()` and `\_aggregate()`,

reused as-is by `src/gold/department\_rollup.py` for the department grain) —

the two tables only differ in their final `groupBy` column, so they stay

consistent by construction and a fix to the shared logic can't drift between

them.



\## Lineage: Silver → Gold



1\. \*\*Filter\*\* `silver\_dvf` to `nature\_mutation = 'Vente'` (excludes

&#x20;  Echange, Adjudication, Vente terrain à bâtir — atypical price

&#x20;  formation, not ordinary market sales), and to rows with

&#x20;  `surface\_reelle\_bati` and `valeur\_fonciere` both non-null and `> 0`

&#x20;  (required to compute a price per m² at all).

2\. \*\*Reduce to mutation level\*\* (`\_mutation\_level()`): group the filtered

&#x20;  Silver rows by `id\_mutation`, summing `surface\_reelle\_bati` and keeping

&#x20;  `valeur\_fonciere` once per mutation. See "Real finding" below for why

&#x20;  this step exists — it's not a cosmetic choice.

3\. \*\*Compute\*\* `price\_per\_sqm = valeur\_fonciere / surface\_reelle\_bati` and

&#x20;  `quarter` (e.g. `"2024-Q1"`, from `date\_mutation`'s year and quarter) at

&#x20;  the mutation level.

4\. \*\*Aggregate\*\* by the table's grain columns, computing the 8 output

&#x20;  columns below.

5\. \*\*Write\*\* to PostgreSQL, `mode=overwrite` (full rebuild on every Gold

&#x20;  run, not incremental — Gold is small enough, and derived entirely from

&#x20;  Silver, that a full rebuild each time is simpler and safer than tracking

&#x20;  incremental Gold state).



\## Columns (identical shape in both tables)



| Column | Type | Description |

|---|---|---|

| `code\_commune` (municipality table only) | text | INSEE commune code — grain key |

| `code\_departement` (department table only) | text | INSEE department code — grain key |

| `quarter` | text | e.g. `"2024-Q1"` — grain key, derived from `date\_mutation` |

| `nb\_transactions` | bigint, not null | Count of \*\*mutations\*\* (real sales), not disposition rows, in this commune/department and quarter after filtering — see "Real finding" below for why this distinction matters |

| `median\_price\_per\_sqm` | double | `percentile\_approx(price\_per\_sqm, 0.5)` across mutations in the group — Spark has no exact median aggregate; approximate is an accepted tradeoff at this row-count scale |

| `avg\_price\_per\_sqm` | double | Mean of `price\_per\_sqm` across mutations in the group |

| `median\_valeur\_fonciere` | double | `percentile\_approx(valeur\_fonciere, 0.5)` — the raw transaction value, not per m² |

| `avg\_valeur\_fonciere` | double | Mean of `valeur\_fonciere` |

| `avg\_surface\_reelle\_bati` | double | Mean of the mutation-level (summed) built surface |



\## Real finding that shaped this table's design



Confirmed against real data before finalizing the aggregation logic, not

assumed: DVF repeats the \*\*full\*\* transaction value (`valeur\_fonciere`) on

\*\*every\*\* disposition row of a multi-lot mutation (a single sale spanning

several apartments/commercial units/dependencies), rather than allocating it

across rows. Real example found in department 75: mutation `2024-1202784`

has 40 disposition rows, each carrying the identical

`valeur\_fonciere = 94,000,000`, with individual `surface\_reelle\_bati` ranging

6–950 m². Computing `price\_per\_sqm` per disposition row (using that row's own

small surface against the full transaction value) produced nonsensical

results up to \~15,700,000 €/m², and inflated `nb\_transactions` by counting

one real sale as 40 separate transactions.



The fix — `\_mutation\_level()` — sums surface across all qualifying

disposition rows of a mutation first, and keeps the transaction value once,

before `price\_per\_sqm` or `nb\_transactions` is ever computed. This is why

`nb\_transactions` should be read as "real sales", and why it will not match

a naive `COUNT(\*)` on the filtered Silver rows for communes/quarters with

multi-lot mutations.



\## Known limitations



| # | Limitation | Severity |

|---|---|---|

| 1 | `price\_per\_sqm` blends all lot types within a mutation. If a single mutation includes, say, an apartment plus a parking spot (both with non-null `surface\_reelle\_bati`), their surfaces are summed and priced together — `type\_local` is not part of the filter or the grain, so Gold cannot separate "pure apartment" mutations from mixed ones. `type\_local`-level analysis (e.g. RETL0-53's price × DPE dashboard) queries `silver\_dvf` directly instead, at the disposition-row level, for this reason. | Real characteristic of this design, not a defect — documented so it isn't mistaken for one |

| 2 | Median is `percentile\_approx`, not an exact median | Accepted tradeoff at this project's row-count scale |

| 3 | Pilot scope is 4 departments (75, 92, 93, 94), not all of Île-de-France or France | Scope decision from Sprint 0, not a Gold-layer limitation specifically |

| 4 | Full overwrite on every Gold run, not incremental | Deliberate — Gold is cheap to fully rebuild from Silver each time, avoiding the complexity of tracking incremental Gold state on top of Silver's own department-partitioned incremental writes |



See `docs/governance/silver-data-dictionary.md` for the Silver-layer

dictionary these tables are built from, and

`docs/governance/quality-rules.md` for the quality rules gating what reaches

`silver\_dvf` in the first place.

