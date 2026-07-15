# Janus-Datalog Examples

15 runnable examples covering the full API. All use the public `db` package.

## Quick Start

```bash
go run -tags example examples/getting_started.go
```

Run from the **repository root**. Examples that load shared data (`examples/data/*.edn`) expect this working directory.

## Learning Path

| Order | File | Concept | Prerequisites |
|-------|------|---------|---------------|
| 1 | `getting_started.go` | Open DB, write facts, query (EDN + qb), QueryInto, getters | None |
| 2 | `query_builder.go` | qb package: patterns, predicates, GetElse, Missing, ordering | 1 |
| 3 | `schema.go` | Schema builder, type validation, uniqueness, cardinality | 1 |
| 4 | `joins_and_patterns.go` | Multi-pattern joins, self-joins, cross-product prevention | 1 |
| 5 | `expressions.go` | Arithmetic, string ops, ground values, time extraction | 2 |
| 6 | `aggregations.go` | count/sum/avg/min/max, grouping, QueryInto with aggregates | 2 |
| 7 | `subqueries.go` | Nested queries, tuple binding, relation binding | 6 |
| 8 | `not_or_clauses.go` | not, not-join, or, or-join, or-default | 2 |
| 9 | `pull_api.go` | PullInto, struct tags, nested refs, convenience getters | 3 |
| 10 | `temporal_queries.go` | AsOf, History, ElementID, time travel | 1 |
| 11 | `multi_source.go` | SliceSource, NewMemorySource, cross-source joins | 2 |
| 12 | `annotations.go` | WithVerbose, custom annotation handler, event tracing | 1 |
| 13 | `storage_internals.go` | Explain, Analyze, plan cache, direct store access | 4 |
| 14 | `export_import.go` | Export/Import EDN, compressed export, round-trip | 1 |
| 15 | `wasm_memory.go` | `OpenMemory`, host-managed Export/Import persistence | 14 |

## Shared Datasets

Query-focused examples load pre-built EDN data from `examples/data/`:

- **`people.edn`** -- 18 people across 4 departments with names, ages, cities, salaries, skills, managers
- **`securities.edn`** -- 5 securities (AAPL, GOOGL, MSFT, AMZN, TSLA) with 10 days of OHLCV price data

To regenerate: `go run examples/generate_data.go`

## Examples by Feature

**Core API**: `getting_started.go`
**Query Builder (qb)**: `query_builder.go`, `joins_and_patterns.go`, `expressions.go`, `aggregations.go`, `subqueries.go`, `not_or_clauses.go`
**Schema & Validation**: `schema.go`
**Typed Results (QueryInto/PullInto)**: `getting_started.go`, `aggregations.go`, `pull_api.go`
**Temporal (AsOf/History)**: `temporal_queries.go`
**Multi-Source Queries**: `multi_source.go`
**Observability**: `annotations.go`
**Advanced/Internals**: `storage_internals.go`
**Backup/Restore**: `export_import.go`
**WASM / memory backend**: `wasm_memory.go`
