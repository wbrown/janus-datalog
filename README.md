# Janus Datalog

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/wbrown/janus-datalog)](https://goreportcard.com/report/github.com/wbrown/janus-datalog)

*What if you never wrote a JOIN again, could time-travel through your data, and deployed it all as a single binary?*

```clojure
[:find ?name ?friend-name
 :where [?user :user/name ?name]
        [?user :user/friend ?friend]
        [?friend :user/name ?friend-name]]
```

No JOIN keyword. No ON clauses. Relationships follow naturally through shared variables.

## Why Janus?

Most databases make you choose:

- **Powerful queries** (Datomic, Datalog) → Complex deployment, expensive, JVM required
- **Simple deployment** (SQLite, embedded DBs) → Limited query expressiveness
- **Predictable performance** (Modern optimizers) → Statistics collection, stale plans, "it depends"

**Janus gives you all three:**

- **Datomic-style queries**: Joins, aggregations, subqueries, time-travel, history
- **Single Go binary**: No JVM, no external dependencies, just `go get`
- **No surprises**: Greedy planning without statistics, explicit error handling, predictable performance

Built for real-world financial analysis with sentiment, options, and OHLC data where query failures mean bad decisions.

## Quickstart

**Install:**

```bash
go get github.com/wbrown/janus-datalog
```

**Define your schema attributes as constants:**

```go
import "github.com/wbrown/janus-datalog/datalog/qb"

var (
    UserName = qb.Kw(":user/name")
    UserAge  = qb.Kw(":user/age")
)
```

**Write data:**

```go
db, _ := storage.NewDatabase("my.db")
tx := db.NewTransaction()

alice := datalog.NewIdentity("user:alice")
tx.Add(alice, UserName.Keyword(), "Alice")
tx.Add(alice, UserAge.Keyword(), int64(30))
tx.Commit()
```

**Query it (Go-native):**

```go
e := qb.NewVar("e")
name := qb.NewVar("name")
age := qb.NewVar("age")

q := qb.Query().
    Find(name, age).
    Where(
        qb.Pat(e, UserName, name),
        qb.Pat(e, UserAge, age),
        qb.Gt(age, 21),
    ).
    MustBuild()

results, _ := db.ExecuteQuery(q)
```

**Output:**

```
[["Alice" 30]]
```

That's it. No schema required. No connection pools. No query tuning. Typos caught at compile time.

## Query Builder vs EDN Strings

Janus supports two ways to write queries:

### Query Builder (Recommended)

The `qb` package provides compile-time safety and IDE support:

```go
var (
    PersonName = qb.Kw(":person/name")
    PersonAge  = qb.Kw(":person/age")
)

e := qb.NewVar("e")
name := qb.NewVar("name")
age := qb.NewVar("age")

q := qb.Query().
    Find(name, age).
    Where(
        qb.Pat(e, PersonName, name),
        qb.Pat(e, PersonAge, age),
        qb.Gte(age, 21),
    ).
    MustBuild()

results, _ := db.ExecuteQuery(q)
```

**Why use it:**
- Typos in `":person/naem"` silently return empty results; `PersonName` catches typos at compile time
- IDE autocomplete shows available attributes
- Refactoring is safe with find-replace
- Same `*Var` in multiple patterns = join (no string matching)

### EDN Strings (Fallback)

For REPL exploration, porting Datomic queries, or when you prefer Datalog syntax:

```go
results, _ := db.ExecuteQuery(`
    [:find ?name ?age
     :where [?e :person/name ?name]
            [?e :person/age ?age]
            [(>= ?age 21)]]
`)
```

**When to use:**
- Quick ad-hoc queries during development
- Porting existing Clojure/Datomic code
- Documentation showing Datalog equivalents

Both produce identical results. All database methods accept either form.

See [docs/reference/QUERY_BUILDER.md](docs/reference/QUERY_BUILDER.md) for complete query builder documentation.

## Running Examples

The `examples/` directory contains working demonstrations of Janus features. Examples use build tags to avoid `main()` function conflicts:

```bash
# Build a specific example
go build -tags example examples/aggregation_demo.go

# Run directly
go run -tags example examples/aggregation_demo.go

# List available examples
ls examples/*.go
```

**Note:** You cannot run `go build ./examples` due to multiple main functions. Build examples individually.

Available examples:
- `aggregation_demo.go` - Aggregation functions (sum, avg, min, max, count)
- `subquery_ohlc_demo.go` - Financial OHLC queries with subqueries
- `financial_time_demo.go` - Time-based queries and as-of queries
- `expression_demo.go` - Expression clauses and arithmetic
- `schema_demo.go` - Schema validation, cardinality, and uniqueness
- `pull_demo.go` - Pull API for entity attribute retrieval
- `reflect_demo.go` - Struct reflection API for Go structs ↔ datoms
- And many more in `examples/`

## Tutorial

### Your First Query

The simplest query: find all users.

```go
[:find ?user
 :where [?user :user/name _]]
```

The `_` is a wildcard – it matches anything but doesn't bind a variable. This query says "find entities that have a `:user/name` attribute."

### Joins Happen Automatically

Want to find friends?

```go
[:find ?name ?friend-name
 :where [?user :user/name ?name]
        [?user :user/friend ?friend]
        [?friend :user/name ?friend-name]]
```

Variables that appear in multiple patterns create natural joins. The query planner figures out the optimal order.

### Filters and Comparisons

```go
[:find ?name ?age
 :where [?user :user/name ?name]
        [?user :user/age ?age]
        [(>= ?age 21)]
        [(< ?age 65)]]
```

Predicates apply as soon as their variables are available. You can also chain comparisons:

```go
[(< 21 ?age 65)]  ; Age between 21 and 65
```

### Computed Values

```go
[:find ?name ?total
 :where [?order :order/person ?person]
        [?person :user/name ?name]
        [?order :order/price ?price]
        [?order :order/tax ?tax]
        [(+ ?price ?tax) ?total]]
```

Expression clauses compute new values. The result gets bound to the variable on the right.

### Database Functions

Handle missing attributes gracefully with database functions:

```go
; Return nickname if exists, otherwise "Anonymous"
[:find ?name ?display
 :where [?user :user/name ?name]
        [(get-else $ ?user :user/nickname "Anonymous") ?display]]

; Filter to users without email addresses
[:find ?name
 :where [?user :user/name ?name]
        [(missing? $ ?user :user/email)]]

; Use first available: nickname → fullname → email
[:find ?id ?display
 :where [?user :user/id ?id]
        [(get-some $ ?user :user/nickname :user/fullname :user/email) ?display]]
```

The `$` is the database reference. These functions look up attributes at query time:
- `get-else` – returns attribute value or default
- `missing?` – filters to entities lacking an attribute (or binds boolean)
- `get-some` – returns first existing attribute from a list

### Aggregations

```go
[:find ?dept (avg ?salary) (count ?emp)
 :where [?emp :employee/dept ?dept]
        [?emp :employee/salary ?salary]
 :order-by [[?dept :asc]]]
```

Aggregations group by the non-aggregated variables. The `:order-by` clause sorts the results.

Available aggregations: `sum`, `count`, `avg`, `min`, `max`

### Time Travel

Every fact is timestamped. Query historical state:

```go
[:find ?price
 :in $ ?as-of
 :where [?stock :stock/price ?price]
 :as-of ?as-of]
```

Or extract time components:

```go
[:find ?stock (max ?price)
 :where [?bar :bar/stock ?stock]
        [?bar :bar/time ?t]
        [(year ?t) ?y]
        [(= ?y 2024)]
        [?bar :bar/high ?price]]
```

Time functions: `year`, `month`, `day`, `hour`, `minute`, `second`

### History Queries (Audit Trail)

Track every change with full history mode:

```go
// Create database with history mode enabled
db, _ := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path:        "my.db",
    RetractMode: storage.RetractHistory,
})

// Make changes
tx := db.NewTransaction()
tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
tx.Commit()  // tx 1: assert "Alice"

tx2 := db.NewTransaction()
tx2.Retract(alice, datalog.NewKeyword(":user/name"), "Alice")
tx2.Commit()  // tx 2: retract "Alice"

// Query current state - returns nothing (value was retracted)
db.ExecuteQuery(`[:find ?name :where [_ :user/name ?name]]`)

// Query history - see ALL changes with 5-element pattern [?e ?a ?v ?tx ?op]
db.ExecuteHistoryQuery(`[:find ?name ?tx ?op :where [_ :user/name ?name ?tx ?op]]`)
// Returns: [["Alice" 1 true] ["Alice" 2 false]]
// op=true means assertion, op=false means retraction
```

**Key concepts:**
- `RetractHistory` mode preserves full audit trail (default `RetractDelete` just deletes)
- Current-state queries (`ExecuteQuery`) see only current truth
- History queries (`ExecuteHistoryQuery`) see all assertions AND retractions
- 5-element patterns: `[?e ?a ?v ?tx ?op]` where `?op` is true=assert, false=retract

### Subqueries

When you need scoped aggregations:

```go
[:find ?date ?symbol ?daily-max
 :where [?s :symbol/ticker ?symbol]
        [(q [:find (max ?high)
             :in $ ?sym ?d
             :where [?bar :bar/symbol ?sym]
                    [?bar :bar/date ?d]
                    [?bar :bar/high ?high]]
            ?s ?date) [[?daily-max]]]]
```

The `q` function runs a sub-query with its own `:find` and `:where` clauses. Results bind to the outer query.

### Schema (Optional)

Schema is **completely optional** but provides type safety and cardinality-many support:

```go
import "github.com/wbrown/janus-datalog/datalog/schema"

// Define schema with builder API
s, _ := schema.NewBuilder().
    Attribute(":user/name").Type(schema.TypeString).Add().
    Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
    Attribute(":user/tags").Type(schema.TypeString).Many().Add().
    Build()

// Create database with schema
db, _ := storage.NewDatabaseWithSchema("my.db", s)
```

**What schema gives you:**
- **Type validation** at `Add()` time – catch type errors immediately
- **Uniqueness constraints** at `Commit()` time – enforce data integrity
- **Cardinality-many** – Pull API returns arrays instead of single values

**Performance:** <1% write overhead for type checking, ~6% for uniqueness. Reads unaffected.

Or define schema via EDN file:

```clojure
{:user/name   {:db/valueType :db.type/string}
 :user/email  {:db/valueType :db.type/string
               :db/unique    :db.unique/value}
 :user/tags   {:db/valueType   :db.type/string
               :db/cardinality :db.cardinality/many}}
```

See [docs/reference/SCHEMA.md](docs/reference/SCHEMA.md) for complete documentation.

### Pull API

Retrieve entity attributes declaratively:

```go
// In queries
[:find (pull ?user [:user/name :user/age])
 :where [?user :user/active true]]

// Standalone
result, _ := db.Pull(userId, `[:user/name :user/email {:user/friends [:user/name]}]`)
```

**Features:**
- Nested reference following with cycle detection
- Wildcard `[*]` for all attributes
- Default values: `(default :attr "fallback")`
- **9× faster than equivalent queries**

See [DATOMIC_COMPATIBILITY.md](DATOMIC_COMPATIBILITY.md) for Pull API details.

### Struct Reflection API

For Go developers, janus-datalog provides a reflection-based API that maps structs directly to datoms:

```go
import "github.com/wbrown/janus-datalog/datalog/reflect"

// Define your domain type with struct tags
type Person struct {
    ID      datalog.Identity `datalog:"-,id"`     // Entity identity
    Name    string           `datalog:"name"`     // → :person/name
    Age     int64            `datalog:"age"`      // → :person/age
    Tags    []string         `datalog:"tags"`     // → :person/tags (many)
    Manager *Person          `datalog:"manager"`  // → :person/manager (ref)
}

// Generate schema from struct
schema, _ := reflect.SchemaFromStruct(Person{})
db, _ := storage.NewDatabaseWithSchema("my.db", schema)

// Write structs directly
alice := &Person{Name: "Alice", Age: 30, Tags: []string{"dev", "lead"}}
tx := db.NewTransaction()
aliceID, _ := tx.AddStructAuto(alice)  // ID auto-generated
tx.Commit()
// alice.ID is now populated

// Read into structs
var loaded Person
db.PullInto(aliceID, &loaded)
fmt.Println(loaded.Name)  // "Alice"
fmt.Println(loaded.Tags)  // ["dev", "lead"]
```

**Features:**
- Schema generation from struct definitions
- Automatic namespace derivation: `type PersonInfo` → `:person-info/...`
- Cardinality inference: slices become cardinality-many
- Auto-generated unique IDs with crypto/rand
- Reference handling via nested struct pointers

See [docs/reference/REFLECT.md](docs/reference/REFLECT.md) for complete documentation.

### QueryInto API

Query directly into typed Go structs - no manual tuple iteration:

```go
// Define result struct with datalog tags
type TradeResult struct {
    Symbol string    `datalog:"?symbol"`
    Price  float64   `datalog:"?price"`
    Volume int64     `datalog:"?volume"`
}

// Query into typed slice
var results []TradeResult
err := db.QueryInto(&results, `
    [:find ?symbol ?price ?volume
     :where [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]
            [?t :trade/volume ?volume]]
`)

for _, r := range results {
    fmt.Printf("%s: $%.2f (%d shares)\n", r.Symbol, r.Price, r.Volume)
}
```

**Works with aggregates too:**

```go
type DeptStats struct {
    Dept      string  `datalog:"?dept"`
    AvgSalary float64 `datalog:"(avg ?salary)"`  // Tag matches :find exactly
    HeadCount int64   `datalog:"(count ?emp)"`
}

var stats []DeptStats
db.QueryInto(&stats, `
    [:find ?dept (avg ?salary) (count ?emp)
     :where [?e :employee/dept ?dept]
            [?e :employee/salary ?salary]]
`)
```

**Single result queries:**

```go
var result TradeResult
err := db.QueryOneInto(&result, `[:find ?symbol ?price ?volume :where ...]`)
// Returns ErrNotFound if no results, ErrMultipleResults if >1
```

**Type-safe queries with QueryFor[T]:**

The query builder can derive variables directly from your result struct, eliminating string synchronization between `NewVar()` calls and struct tags:

```go
type PersonResult struct {
    Name string `datalog:"?name"`
    Age  int64  `datalog:"?age"`
}

q := qb.QueryFor[PersonResult]()
f := &q.F
e := qb.NewVar("e")

query := q.Where(
    qb.Pat(e, PersonName, q.Find(&f.Name)),  // &f.Name -> ?name
    qb.Pat(e, PersonAge, q.Find(&f.Age)),    // &f.Age -> ?age
    qb.Gt(q.V(&f.Age), 21),
).MustBuild()

var results []PersonResult
db.QueryInto(&results, query)  // tags guaranteed to match
```

- `q.Find(&f.Field)` - returns `*Var` and adds to Find clause
- `q.V(&f.Field)` - returns `*Var` without adding to Find (for predicates)
- Rename a struct field → compile error forces you to update usages
- Typo in tag → caught when QueryInto tests fail

See [docs/reference/QUERY_INTO.md](docs/reference/QUERY_INTO.md) for complete documentation.

## What Makes Janus Different

### Pure Relational Algebra

Most Datalog engines use semi-naive evaluation or magic sets transformation. Janus uses **pure relational algebra** – just projection (π), selection (σ), and joins (⋈).

This means:
- Simpler implementation (10× less code)
- Easier to understand and debug
- Proven performance characteristics
- No specialized evaluation strategies

Every query compiles to a sequence of relational operations. No magic, just algebra.

See [RELATIONAL_ALGEBRA_OVERVIEW.md](RELATIONAL_ALGEBRA_OVERVIEW.md) for the complete story, or [docs/papers/PAPER_PROPOSAL_2_DATALOG_AS_RELATIONAL_ALGEBRA.md](docs/papers/PAPER_PROPOSAL_2_DATALOG_AS_RELATIONAL_ALGEBRA.md) for the research paper proposal.

### Greedy Planning Without Statistics

Traditional databases: Collect statistics → Build cost model → Explore plan space → Choose "optimal" plan
**Overhead:** Statistics storage (~1% of DB), ANALYZE commands, planning time (5-50ms), staleness issues

**Janus:** Look at query structure → Order by symbol connectivity → Execute

**Why this works:**

Pattern-based queries have **visible selectivity**:

```datalog
[?s :symbol/ticker "NVDA"]     ; Concrete value = obviously selective
[?p :price/symbol ?s]           ; Natural join path via ?s
[?p :price/time ?t]             ; Progressive refinement
```

The query structure tells you the selectivity. No statistics needed.

**Results:**

- Planning time: 15 microseconds (1000× faster than cost-based)
- Zero statistics overhead
- No staleness issues
- Predictable performance

**Trade-off:** This works for pattern-based queries (90%+ of Datalog). If you're writing SQL-style queries with parameterized WHERE clauses, cost-based optimization is still better.

See [docs/papers/STATISTICS_UNNECESSARY_PAPER_OUTLINE.md](docs/papers/STATISTICS_UNNECESSARY_PAPER_OUTLINE.md) for the full characterization.

### Streaming Execution

Janus uses **iterator composition** instead of materializing intermediate results:

```
Pattern → Iterator → Filter → Iterator → Join → Iterator → ...
```

**Benefits:**

- **2.22× faster** on low-selectivity filters (verified)
- **52% memory reduction** (up to 91.5% on large datasets)
- **4.06× speedup** from iterator composition alone
- Handles queries that would OOM other engines
- Lazy evaluation means you only compute what you need

Inspired by Clojure's lazy sequences, but with relational algebra semantics.

See [docs/papers/PAPER_PROPOSAL_3_FUNCTIONAL_STREAMING.md](docs/papers/PAPER_PROPOSAL_3_FUNCTIONAL_STREAMING.md) for the research proposal.

### Explicit Error Handling

Many Datalog engines will happily create Cartesian products that explode your memory. Janus **detects and rejects** them:

```
Query resulted in 3 disjoint relation groups - Cartesian products not supported
```

This happens when you write a query with no join paths between patterns. Instead of silently creating billions of tuples, Janus fails fast with a clear error.

**Design philosophy:** Better to error explicitly than succeed unexpectedly.

## Performance

**Summary:** Production-ready performance for 100K-100M+ datoms. All numbers are **measured** from actual benchmarks.

### Key Results

- **2× faster** on complex queries (clause-based planner + streaming)
- **4.06× faster** iterator composition with 89% memory reduction
- **2.22× faster** streaming execution with 52% memory reduction
- **2.06× faster** parallel subquery execution (8 workers)
- **10-50ms** for simple queries (1-3 patterns, 1M datoms)
- **2-4 seconds** for complex queries (5-10 patterns with subqueries)

### Architectural Wins

| Optimization | Speedup | What It Does |
|--------------|---------|--------------|
| Iterator composition | 4.06× | Lazy evaluation without materialization |
| Streaming execution | 2.22× | Avoid intermediate result materialization |
| Predicate pushdown | 1.58-2.78× | Filter at storage layer (scales with dataset size) |
| Time-range scanning | 4× | Multi-range queries for OHLC data |
| Parallel subqueries | 2.06× | Worker pool for concurrent execution |
| Parallel intern cache | 6.26× | Eliminate lock contention |

### Scale Characteristics

- **100K-1M datoms**: Excellent (sub-second simple queries)
- **1M-10M datoms**: Good (all query types)
- **10M-100M datoms**: Tested and working (scales predictably)
- **500M+ datoms**: Large config tested successfully

### Design Trade-offs

**We chose correctness over raw speed:**

- Explicit Cartesian product detection (errors instead of OOM)
- L85 encoding for human-readable keys (20-25% overhead vs binary)
- Better algorithms over micro-optimizations

**Philosophy:** Make the right thing fast, not the fast thing easy to misuse.

For complete benchmark results, see [PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md).

## Production Validation

Janus isn't a research project – it's built from production experience.

### LookingGlass ScoutPrime (2014-2021)

- **Domain:** Cybersecurity threat intelligence
- **Scale:** Billions of facts
- **Duration:** 7 years in production
- **Architecture:** Distributed Datalog over Elasticsearch
- **Patent:** US10614131B2 (phase-based query planning)

**Lesson learned:** Elasticsearch constraints forced phase decomposition, which turned out to be the right abstraction.

### Financial Analysis Application (2025-present)

- **Domain:** Stock option analysis
- **Stakes:** Real money, real decisions
- **Workload:** Time-series OHLC queries with 4 parallel subqueries
- **Requirements:** Real-time analysis, no query failures

**Use case that drove development:**

Analyzing stock option positions required:
- Historical price queries (time-travel)
- Aggregations across multiple timeframes
- Complex joins (symbols → prices → time ranges)
- Predictable performance (bad queries = bad decisions)

**Why Janus exists:** When real money is on the line, your database can't have surprises.

## Architecture

### Datom: The Fundamental Unit

```go
type Datom struct {
    E  Identity  // Entity (SHA1 hash + L85 encoding)
    A  Keyword   // Attribute (e.g., :user/name)
    V  Value     // Any value (interface{})
    Tx uint64    // Transaction ID / timestamp
}
```

Every fact is a datom. The entire database is just a collection of datoms with multiple indices.

### Five Indices for Fast Queries

| Index | Primary Use Case |
|-------|-----------------|
| **EAVT** | Find all facts about an entity |
| **AEVT** | Find all entities with an attribute |
| **AVET** | Find entities by attribute value |
| **VAET** | Reverse lookup (who references this entity?) |
| **TAEV** | Time-based queries |

The query planner picks the best index based on which values are bound in your pattern.

### Type System: Direct Go Types

**No wrapper types. No value encoding at query time.**

```go
// Values are just interface{} containing:
string              // Text
int64               // Integers
float64             // Decimals
bool                // Booleans
time.Time           // Timestamps
[]byte              // Binary data
Identity            // Entity references
Keyword             // When keywords are values
```

This keeps the query engine simple and fast. Type conversions happen once at storage time.

### Query Execution: Relations All The Way Down

```
Query → Phases → Patterns → Storage Scans → Relations
Relations → Joins → Relations → Filters → Relations
Relations → Aggregations → Relations → Output
```

Every step produces a `Relation` (a set of tuples with named columns). The entire execution is just relational algebra operations composed together.

**Key abstraction:**

```go
type Relation interface {
    Schema() []Symbol                    // Column names
    Iterator(ctx Context) Iterator       // Streaming access
    Project(cols []Symbol) Relation      // π (projection)
    Filter(filter Filter) Relation       // σ (selection)
    Join(other Relation) Relation        // ⋈ (natural join)
    Aggregate(...) Relation              // γ (aggregation)
}
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the complete system architecture.

### L85 Encoding: Human-Readable Storage Keys

Storage keys use **L85 encoding** – a custom Base85 variant that preserves sort order:

```
20 bytes → 25 characters (vs 28 for Base64)
Space efficient + sort preserving + human readable
```

This enables:
- Debug storage without binary tools
- Copy/paste keys between systems
- Range scans work correctly
- URL and JSON safe

**Trade-off:** 20-25% overhead vs raw binary. We chose debuggability.

See implementation in `datalog/codec/l85.go`.

## Research Contributions

Janus has produced several research-worthy contributions. Five paper proposals/outlines are available in [docs/papers/](docs/papers/):

### Papers Ready to Write

1. **[Convergent Evolution](docs/papers/CONVERGENT_EVOLUTION_PAPER_OUTLINE.md)** - How storage constraints led to rediscovering classical query optimization
2. **[Statistics Unnecessary](docs/papers/STATISTICS_UNNECESSARY_PAPER_OUTLINE.md)** - When pattern-based queries don't need cardinality statistics

### Paper Proposals

3. **[Pure Relational Algebra](docs/papers/PAPER_PROPOSAL_2_DATALOG_AS_RELATIONAL_ALGEBRA.md)** - Implementing Datalog with only π, σ, ⋈ (no semi-naive evaluation)
4. **[Functional Streaming](docs/papers/PAPER_PROPOSAL_3_FUNCTIONAL_STREAMING.md)** - Applying lazy evaluation and immutability to query execution

See [docs/papers/README.md](docs/papers/README.md) for complete details.

**Production + research = better systems.** These aren't toy examples – they're lessons learned from processing billions of facts.

## Documentation

### Getting Started

- **[docs/reference/QUERY_BUILDER.md](docs/reference/QUERY_BUILDER.md)** - Go-native query builder (recommended)
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture and design decisions
- **[DATOMIC_COMPATIBILITY.md](DATOMIC_COMPATIBILITY.md)** - Feature comparison (~70% compatibility)
- **[DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md)** - Complete documentation guide

### Deep Dives

- **[RELATIONAL_ALGEBRA_OVERVIEW.md](RELATIONAL_ALGEBRA_OVERVIEW.md)** - How queries become algebra
- **[PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md)** - Measured performance and benchmarks
- **[docs/reference/PLANNER_COMPARISON.md](docs/reference/PLANNER_COMPARISON.md)** - Phase-based vs clause-based planning

### Development

- **[CLAUDE.md](CLAUDE.md)** - Architectural guidance for contributors
- **[TODO.md](TODO.md)** - Roadmap and priorities

## Examples

The `examples/` directory contains progressively complex demonstrations:

```bash
# Start here
go run examples/simple_example.go           # Social network
go run examples/storage_demo.go             # Persistent queries

# Core features
go run examples/expression_demo.go          # Computed values
go run examples/aggregation_demo.go         # Group by and aggregations
go run examples/subquery_proper_demo.go     # Nested queries

# Time-based queries
go run examples/financial_time_demo.go      # Time transactions
go run examples/financial_asof_demo.go      # Historical queries
go run examples/time_functions_demo.go      # year, month, day, etc.
```

Each example is self-contained and demonstrates specific features.

## Running Tests

```bash
# All tests
go test ./...

# Specific package
go test ./datalog/executor -v

# With coverage
go test ./... -cover
```

Test suite includes:
- Unit tests for all core components
- Integration tests for query execution
- Benchmark tests for performance tracking
- Example-based tests for documentation

## Contributing

We welcome contributions! Here's how to get started:

1. **Understand the architecture**: Read [CLAUDE.md](CLAUDE.md) and [ARCHITECTURE.md](ARCHITECTURE.md)
2. **Check the roadmap**: See [TODO.md](TODO.md) for current priorities
3. **Review performance lessons**: Read [PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md)
4. **Run the tests**: `go test ./...` should pass
5. **Follow Go idioms**: Simple functions over complex abstractions

**Areas where we'd love help:**

- Additional aggregation functions (e.g., distinct, median)
- WASM build support
- Query optimization with statistics (for SQL-style workloads)
- Documentation improvements

**Before starting major work**, please open an issue to discuss the approach.

## Datomic Compatibility

Janus implements **~70% of Datomic's feature set** (weighted by typical usage), focusing on the most commonly used features:

**Implemented:**
- Core queries: Patterns, joins, variables
- Predicates: Comparisons, expressions
- Aggregations: sum, count, avg, min, max
- Subqueries: Full q support
- Time queries: as-of, time functions
- History queries: Full audit trail with `ExecuteHistoryQuery`
- Pull API: Nested references, cycle detection, wildcards
- Schema: Type validation, cardinality, uniqueness constraints
- Storage: Persistent BadgerDB backend
- NOT/OR clauses: `(not ...)`, `(not-join ...)`, `(or ...)`, `(or-join ...)` with fallback expressions
- Go-specific: Struct reflection API, QueryInto for typed results

**Not implemented:**
- Rules and recursive queries
- Full-text search
- Distributed transactions

See [DATOMIC_COMPATIBILITY.md](DATOMIC_COMPATIBILITY.md) for the complete compatibility matrix.

**If you're coming from Datomic:** Most simple to moderate queries will work with minimal changes. Complex queries using advanced features may require refactoring.

## Current Status

**Production Ready:**

- Core query engine complete and tested
- Pull API with nested references and cycle detection
- Schema support: type validation, cardinality, uniqueness
- Struct reflection and QueryInto for Go-native ergonomics
- Persistent storage with BadgerDB
- Comprehensive test suite (1.28:1 test-to-code ratio)
- Used in production for financial analysis

**In Progress:**

- Additional aggregation functions (distinct, median)

**Future Work:**

- WASM build for browser deployment
- Statistics-based optimization (for SQL-style queries)
- Distributed query execution

See [TODO.md](TODO.md) for detailed roadmap.

## Design Principles

These principles guided Janus's development:

1. **Correctness First** - Better to fail explicitly than succeed surprisingly
2. **Simple > Complex** - Pure relational algebra over specialized strategies
3. **Predictable > Optimal** - Greedy planning without statistics surprises
4. **Debug-Friendly** - Human-readable keys, explicit errors, clear traces
5. **Production-Ready** - Real workloads, measured performance, comprehensive tests

**Philosophy:** Build systems that you can reason about, debug when they fail, and trust when they work.

## License

Apache 2.0 License - See [LICENSE](LICENSE) file for details.

## Questions?

- **Documentation**: Start with [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md)
- **Issues**: Use GitHub issues for bugs and feature requests
- **Architecture**: See [CLAUDE.md](CLAUDE.md) for design guidance
- **Performance**: Check [PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md) for benchmarks

Built with experience from processing billions of facts across cybersecurity and financial domains. Made for developers who want powerful queries without database surprises.
