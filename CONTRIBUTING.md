# Contributing & Team Workflow

## Roles

| Person | Primary responsibility |
|---|---|
| P1 | Ingestion and infrastructure |
| P2 | Transformation and data modeling |
| P3 | Data quality and orchestration |
| P4 | Analytics, documentation and business interpretation |
| P5 | Data governance, enrichment mapping and predictive modeling |

Roles define primary ownership, not exclusive scope. Everyone can and should touch other
parts of the pipeline, especially in early sprints when their own lane has no work yet.

P5 owns: source contracts, data dictionary governance, enrichment-field mapping, join-key
analysis, KPI definitions, ML feature definitions, predictive-model evaluation.

## Branching

- `main` — always deployable, protected, merges only via reviewed PR
- `feature/<sprint>-<short-description>` — one branch per ticket
- PRs reference the ticket number and list Owner / Reviewer / Reproducer in the description

## Ticket workflow

Every ticket has three named people, never the same person twice:

- **Primary Owner** — does the work, opens the PR
- **Reviewer** — checks logic/code before merge (see pairing table below)
- **Reproducer** — independently re-runs the result from a clean checkout
  (`git clone` + `docker compose up` + re-execute the asset/notebook) and confirms it
  matches before the ticket can move to Done

Board columns: `To Do -> In Progress -> Review -> Repro -> Done`. A ticket cannot skip
Repro, even under deadline pressure — this is the check that catches "works on my machine"
issues and undocumented manual steps, which is exactly what the project's reproducibility
success criteria are graded on.

### Reviewer pairing (adjacent domains)

| Owner | Reviewer |
|---|---|
| P1 | P3 |
| P2 | P5 |
| P3 | P1 |
| P4 | P5 |
| P5 | P2 |

### Reproducer

Rotate among the three people not already Owner or Reviewer on that ticket. Assign the
person with the lightest load that sprint; track the rotation on the board so it doesn't
always fall on the same person.

## Sprint cadence

Weekly sprints, demo + review at the end of each. See `docs/sprint_plan.md` for the full
8-sprint breakdown and per-person workload each week.

## Commit / PR conventions

- Commit messages: `[sprint-N] short description`
- PRs must link the ticket and name Owner / Reviewer / Reproducer
- No direct commits to `main`
