# Documentation Index

## Core Documentation

### Getting Started
- **[README.md](README.md)** - Project overview, features, and quick start
- **[DATOMIC_COMPATIBILITY.md](DATOMIC_COMPATIBILITY.md)** - Feature compatibility matrix with Datomic

### Architecture & Implementation
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture overview
- **[RELATIONAL_ALGEBRA_OVERVIEW.md](RELATIONAL_ALGEBRA_OVERVIEW.md)** - Complete guide to the relational algebra system
- **[CLAUDE.md](CLAUDE.md)** - Detailed implementation guide and coding standards
- **[PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md)** - Current performance status and benchmarks

### Development
- **[TODO.md](TODO.md)** - Active development roadmap and priorities

## Reference Documentation

Configuration, semantics, and proofs in `docs/reference/`:

**Storage and CRDT:**
- **[CRDT.md](docs/reference/CRDT.md)** - CRDT storage semantics: LWW, add-wins sets, RGA vectors, EA cache
- **[CRDT_UNIQUE_SEMANTICS.md](docs/reference/CRDT_UNIQUE_SEMANTICS.md)** - Uniqueness resolution under CRDT writes
- **[CRDT_STREAMING_RESOLUTION.md](docs/reference/CRDT_STREAMING_RESOLUTION.md)** - The streaming CRDT iterator design
- **[KEY_ENCODING_AND_CRDT.md](docs/reference/KEY_ENCODING_AND_CRDT.md)** - Key layout, bitwise-NOT Tx, the eight indices, LZ77+FSE compression, Tier-3 blob store
- **[INDEX_SELECTION_PROOF.md](docs/reference/INDEX_SELECTION_PROOF.md)** - Proof of CRDT-correct index selection by binding pattern
- **[OP_POSITION_PROOF.md](docs/reference/OP_POSITION_PROOF.md)** - Why the Op byte goes last (key-decoding correctness)
- **[SCHEMA.md](docs/reference/SCHEMA.md)** - Schema support: types, cardinality, uniqueness, Pull API integration
- **[EXPORT_IMPORT.md](docs/reference/EXPORT_IMPORT.md)** - Database export/import to EDN format for backup and migration
- **[BINARY_EXPORT.md](docs/reference/BINARY_EXPORT.md)** - Compressed seekable JDZL binary dump (`-export-bin` / `-import-bin`)

**Query system:**
- **[PLANNER_OPTIONS.md](docs/reference/PLANNER_OPTIONS.md)** - Complete planner options reference with performance guidance
- **[QUERY_BUILDER.md](docs/reference/QUERY_BUILDER.md)** - Type-safe Go query builder (qb package)
- **[QUERY_INTO.md](docs/reference/QUERY_INTO.md)** - Typed query results into Go structs
- **[REFLECT.md](docs/reference/REFLECT.md)** - Struct ↔ datom reflection API
- **[MULTI_SOURCE.md](docs/reference/MULTI_SOURCE.md)** - Cross-source joins, in-memory sources, `SliceSource[T]`, custom matchers
- **[OR_FALLBACK_SEMANTICS.md](docs/reference/OR_FALLBACK_SEMANTICS.md)** - `or` (union) vs `or-default` (fallback) semantics
- **[TUPLE_GROUND.md](docs/reference/TUPLE_GROUND.md)** - Multi-variable `(ground …)` binding

**Philosophy:**
- **[ARCHITECTURAL_PHILOSOPHY.md](docs/reference/ARCHITECTURAL_PHILOSOPHY.md)** - The "why" behind the architecture choices

## Current Work in Progress

**[docs/wip/](docs/wip/README.md)** - designs with open questions, derivations,
campaign records and investigations. Each document carries its own status header,
and that status is the authority; the directory README is deliberately not a
status list.

### Development Notes

Implementation notes organized by package in `docs/dev-notes/`:
- **executor/** - Query execution engine implementation notes

## Design Proposals

**[docs/proposals/](docs/proposals/)** - designs for work not in flight: storage
and index layouts, distribution and sync, query-language extensions, and API
surfaces. A proposal states what is being considered, not what the engine does.

## Package Documentation

### Core Packages
- `datalog/` - Core types and interfaces
- `datalog/executor/` - Query execution engine
- `datalog/planner/` - Query planning and optimization
- `datalog/storage/` - Persistent storage layer (BadgerDB)
- `datalog/parser/` - EDN and query parsing
- `datalog/query/` - Query types and structures
- `datalog/schema/` - Schema support (types, cardinality, uniqueness)

### Support Packages
- `datalog/codec/` - L85 encoding and value serialization
- `datalog/edn/` - EDN lexer and parser
- `datalog/annotations/` - Query execution annotations

## Examples

See the `examples/` directory for usage examples:
- Basic queries and pattern matching
- Aggregations and grouping
- Time-based queries
- Subqueries and joins
- Financial data analysis

## Archived Documentation

Historical documentation organized by type:

### Major Milestones
- **[docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md](docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md)** - Complete streaming implementation history (Jan-Oct 2025)
- **[docs/archive/2025-10/SUBQUERY_PERFORMANCE_ANALYSIS.md](docs/archive/2025-10/SUBQUERY_PERFORMANCE_ANALYSIS.md)** - Gopher-street performance investigation and resolution
- **[docs/archive/2025-10/GOPHER_STREET_RESPONSE.md](docs/archive/2025-10/GOPHER_STREET_RESPONSE.md)** - Response to gopher-street performance report

### Archive Directories
- **docs/archive/early-design/** - Original design exploration and analysis
- **docs/archive/2025-10/** - October 2025 optimization sprint (major performance work)
- **docs/archive/2025-12/** - December 2025 campaign artifacts
- **docs/archive/2026-07/** - July 2026 campaign artifacts
- **docs/archive/optimization-attempts/** - Historical optimization attempts and profiling
- **docs/archive/completed/** - Completed features and implementations
- **docs/archive/obsolete/** - Documents whose premise no longer holds

Each archive directory contains a README explaining its contents.

## Bug Documentation

Open bugs sit at the top level of **[docs/bugs/](docs/bugs/)**; a fix moves the
document into **[docs/bugs/resolved/](docs/bugs/resolved/)**, so the directory a
document is in states whether it is still live. Cite a bug by its `BUG_NAME`
rather than its path — the path changes when the bug is fixed.

## Future Ideas

See `docs/ideas/` for potential optimizations and features under consideration. The README tracks implementation status.

- **[OPTIMIZATION_OPPORTUNITIES.md](docs/ideas/OPTIMIZATION_OPPORTUNITIES.md)** - Measured optimization roadmap and completed checkpoints
- **[RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md](docs/ideas/RELATION_MATERIALIZATION_AND_SET_INVARIANTS.md)** - Relation set/replayability laws, materialization audit, deduplication rules, and ranked next work