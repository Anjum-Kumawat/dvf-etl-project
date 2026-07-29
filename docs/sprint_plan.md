# Sprint Plan (8 sprints, 5 people)

Layers are sequential (Silver needs Bronze, Gold needs Silver, orchestration/ML need Gold),
so the sprint cadence is the backbone. Roles determine who leads each sprint's deliverable,
not a separate parallel timeline.

| Sprint | P1 Ingestion/Infra | P2 Transform/Model | P3 Quality/Orchestration | P4 Analytics/Docs | P5 Governance/Enrichment/ML |
|---|---|---|---|---|---|
| **0 (W1)** | Docker Compose skeleton, repo setup | Explore DVF fields, sketch Silver/Gold schema | Research GE vs native checks, quality strategy draft | Architecture diagram, help fix pilot scope/granularity, README skeleton | Draft source contracts (license, update cadence per source), propose ML use case for sign-off |
| **1 (W2)** | DVF downloader -> MinIO Bronze | Document DVF quality issues, cleaning approach | First GE checks on raw Bronze | Document Bronze process, start data dictionary | Finalize DVF source contract, begin enrichment-field mapping spec |
| **2 (W3)** | Build BAN/ADEME/Filosofi/geo API clients | Map enrichment fields into planned Silver schema | Extend checks to enrichment sources, plan incremental logic | Update data dictionary, API access notes | Own: enrichment-field mapping doc, source contracts for the 4 APIs, start join-key analysis |
| **3 (W4)** | Support ingestion fixes, stub Dagster assets | Lead: PySpark cleaning (typing, dedup, outliers) | GE checks on cleaned Silver | Document cleaning rules, update architecture doc | Finalize join-key analysis (match-rate validation), dictionary governance pass on Silver |
| **4 (W5)** | Prep Postgres/MinIO connections for Dagster | Lead: multi-source joins into Silver | Validate join quality, missing/mismatched keys | Silver data dictionary, first dashboard sketches | Support joins using validated keys, draft KPI definitions |
| **5 (W6)** | Support Dagster asset prep | Co-lead: Gold aggregate tables | Co-lead: full GE suite Silver+Gold | Gold data dictionary, dashboard mockups | Lead: finalize KPIs, define ML feature set from Gold tables, feature-engineering spec |
| **6 (W7)** | Support Dagster infra/config | Debug transformations, support feature pipeline | Lead: Dagster assets/scheduling/incremental ingestion, add model-training asset to DAG | Ops guide, scheduling/monitoring docs | Lead: build/train predictive model as a Dagster asset on Gold features, write predictions to Postgres |
| **7 (W8)** | Final Docker Compose polish, demo env | Final transformation edge cases | Final quality report, confirm model asset runs in monitored DAG | Lead: 3+ Metabase dashboards (incl. predictions vs actuals), final report, guides, defense prep | Predictive-model evaluation (accuracy, backtesting), document limitations, feed business interpretation to P4 |

## Key rule for the predictive model

The model must consume only Gold-layer tables and write predictions back into Postgres as a
Dagster asset downstream of Gold — scheduled, monitored, and quality-checked like every other
Gold table. It is not a standalone notebook or side project.

## Sprint 0 decisions (settle before Sprint 1)

- Pilot geographic scope (e.g. Ile-de-France vs whole of France)
- Gold table granularity (municipality / department / quarter)
- Great Expectations vs native PySpark assertions
- Initial predictive-model use case (e.g. price/m² forecast, transaction volume forecast)
