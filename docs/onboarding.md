# Onboarding: Getting Set Up on This Project

Follow these steps in your own terminal to get the repo running locally and make your
first contribution.

## 1. Check Git is installed

```
git --version
```

If this errors, install Git from https://git-scm.com/downloads first.

## 2. Clone the repo

```
cd Desktop
git clone https://github.com/Anjum-Kumawat/dvf-etl-project.git
cd dvf-etl-project
```

You need to be added as a Collaborator on the repo first (ask Anjum), or the repo must
be public, otherwise the clone/push will fail with a permissions error.

## 3. Set your Git identity (first time only)

```
git config --global user.name "Your Name"
git config --global user.email "your-email@example.com"
```

## 4. Read before you start

- `README.md` — architecture and repo layout
- `CONTRIBUTING.md` — roles, and the Owner / Reviewer / Reproducer ticket workflow
- `docs/sprint_plan.md` — full 8-sprint plan and who does what each week

## 5. Pick up a ticket

Go to the repo's **Issues** tab on GitHub. Each ticket uses the template with three
named roles: Primary Owner, Reviewer, Reproducer. Confirm which one you are on a given
ticket before starting work — see `CONTRIBUTING.md` for the reviewer-pairing and
reproducer-rotation rules.

## 6. Create a branch for your ticket

```
git checkout -b feature/<sprint>-<short-description>
```

Example:

```
git checkout -b feature/sprint1-dvf-downloader
```

## 7. Do the work, then commit

```
git add .
git commit -m "[sprint-1] short description of what you did"
```

## 8. Push your branch

```
git push -u origin feature/sprint1-dvf-downloader
```

## 9. Open a Pull Request

GitHub shows a "Compare & pull request" button right after the push — click it, or go to
the repo → **Pull requests** → **New pull request**. Select your branch merging into
`main`. In the PR description, name your Owner / Reviewer / Reproducer.

`main` is protected: it needs at least one approval before it can merge, and you cannot
approve your own PR. Tag your Reviewer so they know to look at it.

## 10. After approval, your Reviewer or you merges the PR

Once merged, your Reproducer re-runs your result from a clean clone (`git clone` +
`docker compose up` + re-run whatever you built) to confirm it actually works before the
ticket is marked Done on the board.

## Keeping your local repo up to date

Before starting new work each time:

```
git checkout main
git pull origin main
git checkout -b feature/<new-ticket>
```

## If something goes wrong

Paste the exact error message into the team chat or to Claude — most Git errors here are
one of: not added as a collaborator yet, wrong remote URL, or forgetting to `pull` the
latest `main` before branching.
