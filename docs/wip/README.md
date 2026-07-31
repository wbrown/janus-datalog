# Work In Progress

Design documents, derivations and campaign records for work that is in flight or recently landed.

## How to tell what is live

**Each document carries its own status, and that status is the authority.** This README deliberately does not list them: a directory index that restates per-document status is a second copy that has to be hand-synced, and it drifts.

Read a document's status header. Where it makes claims about code, verify against the tree; a document is a record of what was decided, not evidence of what is currently true.

## Kinds of document here

- **Designs with open questions** — carry a status line naming what is unratified. `RELATION_ALGEBRA_REUNIFICATION.md`, `CORRECT_BY_CONSTRUCTION_PLANS.md` and `CORRECTNESS_MEASUREMENT.md` each hold their own open decisions; that is where they are answered, not in a cross-cutting list.
- **Derivations** — a completed reasoning record with a fixed subject, kept because the reasoning is worth more than the conclusion alone.
- **Campaign records** — what a migration ruled and what it cost.
- **Investigations** — a question worked through, with or without a resulting change.

## Where things go when they are done

- `docs/archive/completed/` — implemented features and their plans
- `docs/archive/obsolete/` — outdated status files, documents whose premise no longer holds
- `docs/archive/<YYYY-MM>/` — a campaign's artifacts, grouped by the period of the work
- `docs/proposals/` — if it turns out to be a proposal for future work rather than work in flight

Each archive subdirectory carries a README stating what is in it, why it is archived, and where current information lives.
