# Executor Package Development Notes

Implementation notes for the query execution engine.

## Files

- **SUBQUERY_API_NOTES.md** - Subquery API design and implementation notes

## Moved/Archived

- **PERFORMANCE_ISSUE.md** - ✅ Moved to `docs/archive/2025-10/` (fixed by October 2025 optimizations)
- **SUBQUERY_POSITIONAL_MAPPING_ISSUE.md** - ⚠️ Moved to `docs/bugs/active/` (active design limitation)

## Context

The executor package (`datalog/executor/`) is responsible for:
- Query execution with relation-based architecture
- Pattern matching and joins
- Expression evaluation
- Subquery execution
- Aggregation operations
