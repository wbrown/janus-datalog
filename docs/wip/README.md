# Work In Progress

This directory contains documentation for features currently under active development.

## Active Work

### Conditional Aggregate Rewriting
**Status**: 95% complete - integration done, testing blocked by executor limitation

**File**: `CONDITIONAL_AGGREGATE_REWRITING.md` - Complete implementation status, design, and known issues

**Goal**: Automatically rewrite correlated aggregate subqueries into conditional aggregates to eliminate execution overhead (e.g., 588 executions → 1 aggregation pass).

---

## Guidelines

- Move completed work to `docs/archive/completed/`
- Move abandoned work to `docs/archive/obsolete/`
- Keep this directory focused on active development only
