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

Configuration and optimization guides in `docs/reference/`:
- **[PLANNER_OPTIONS.md](docs/reference/PLANNER_OPTIONS.md)** - Complete planner options reference with performance guidance

## Current Work in Progress

See `docs/wip/` for active development work:
- **[PHASE_AS_QUERY_ARCHITECTURE.md](docs/wip/PHASE_AS_QUERY_ARCHITECTURE.md)** - Stage C: AST-oriented planner rewrite (in progress)

### Development Notes

Implementation notes organized by package in `docs/dev-notes/`:
- **executor/** - Query execution engine implementation notes

## Package Documentation

### Core Packages
- `datalog/` - Core types and interfaces
- `datalog/executor/` - Query execution engine
- `datalog/planner/` - Query planning and optimization
- `datalog/storage/` - Persistent storage layer (BadgerDB)
- `datalog/parser/` - EDN and query parsing
- `datalog/query/` - Query types and structures

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
- **docs/archive/optimization-attempts/** - Historical optimization attempts and profiling
- **docs/archive/completed/** - Completed features and implementations

Each archive directory contains a README explaining its contents.

## Bug Documentation

### Resolved Bugs
- **docs/bugs/resolved/** - Documented bug fixes with analysis
  - **[DECORRELATION_BUG_FIX.md](docs/bugs/resolved/DECORRELATION_BUG_FIX.md)** - Pure aggregation decorrelation bug (Oct 2025)
  - See README for complete list

### Active Bugs
- **docs/bugs/active/** - Currently tracked issues

## Future Ideas

See `docs/ideas/` for potential optimizations and features under consideration. The README tracks implementation status.