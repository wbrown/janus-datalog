# Janus-Datalog Examples

This directory contains 28 runnable examples demonstrating the features and capabilities of the Janus-Datalog engine. All examples are fully functional and execute actual queries.

## Quick Start

**New to Datalog?** Start here:
1. [`simple_example.go`](#simple_examplego) - Your first Datalog query (5 minutes)
2. [`query_execution.go`](#query_executiongo) - Basic pattern matching (10 minutes)
3. [`join_demo.go`](#join_demogo) - Combining data across patterns (10 minutes)

**Want to see real-world usage?** Jump to:
- [`gopher_street_integration.go`](#gopher_street_integrationgo) - Financial market data queries
- [`subquery_ohlc_demo.go`](#subquery_ohlc_demogo) - OHLC (stock price) calculations
- [`iot_example.go`](#iot_examplego) - IoT sensor data analysis

## Running Examples

All examples use the `example` build tag:

```bash
# Run any example
go run -tags example examples/simple_example.go

# Or build first
go build -tags example -o /tmp/demo examples/simple_example.go
/tmp/demo
```

---

## Learning Path

### Level 1: Fundamentals (Start Here)

Master the basics of Datalog queries and the EAVT data model.

#### `simple_example.go`
**What it demonstrates:** Your first Datalog query
**Concepts:** Entity-Attribute-Value-Transaction (EAVT) model, basic pattern matching
**Time:** 5 minutes

Creates simple datoms and queries them. Perfect introduction to how Datalog represents data.

```datalog
[:find ?name
 :where [?person :person/name ?name]]
```

#### `query_execution.go`
**What it demonstrates:** Pattern matching and variable binding
**Concepts:** Variables (`?person`), constants, multiple patterns, joins
**Time:** 10 minutes

Shows how patterns bind variables and combine to filter results.

#### `parser_demo.go`
**What it demonstrates:** EDN query syntax parsing
**Concepts:** Query structure, `:find` clause, `:where` clause, syntax validation
**Time:** 5 minutes

Understand how queries are parsed from Clojure-style EDN syntax into internal structures.

#### `parser_errors_demo.go`
**What it demonstrates:** Query syntax error handling
**Concepts:** Common syntax mistakes, error messages, query validation
**Time:** 5 minutes

Learn to recognize and fix query syntax errors.

---

### Level 2: Storage & Data Modeling

Learn how data is stored and indexed for efficient queries.

#### `storage_example.go`
**What it demonstrates:** Persistent storage with BadgerDB
**Concepts:** Database creation, transactions, multiple indices (EAVT, AEVT, AVET, VAET, TAEV)
**Time:** 15 minutes

Shows how to persist datoms to disk, create transactions, and leverage multiple index types for efficient queries.

#### `long_values_example.go`
**What it demonstrates:** Unbounded value storage
**Concepts:** Fixed-size keys, variable-length values, binary data, type preservation
**Time:** 10 minutes

Demonstrates that while keys are fixed-size (72 bytes), values can be any size: long strings, binary data, images, etc.

---

### Level 3: Query Features

Master the query language features for filtering, transforming, and combining data.

#### `join_demo.go`
**What it demonstrates:** Joining data across multiple patterns
**Concepts:** Implicit joins via shared variables, relational algebra
**Time:** 10 minutes
**Prerequisites:** `simple_example.go`, `query_execution.go`

Learn how patterns with shared variables automatically join data.

#### `binding_demo.go`
**What it demonstrates:** Variable binding and unification
**Concepts:** How variables bind to values, unification across patterns
**Time:** 10 minutes

Deep dive into how the query engine binds variables to concrete values.

#### `expression_demo.go`
**What it demonstrates:** Expression clauses for data transformation
**Concepts:** Arithmetic (`+`, `-`, `*`, `/`), string operations, `ground` values
**Time:** 15 minutes

Use expressions to compute derived values within queries.

```datalog
[:find ?total
 :where [?item :item/price ?price]
        [?item :item/quantity ?qty]
        [(* ?price ?qty) ?total]]
```

#### `comparator_demo.go`
**What it demonstrates:** Comparison operators
**Concepts:** `<`, `>`, `<=`, `>=`, `=`, `!=`, variadic comparisons
**Time:** 10 minutes

Filter data using comparisons, including Clojure-style chained comparisons.

```datalog
[(< 0 ?age 100)]  ; Age between 0 and 100
```

#### `in_clause_demo.go`
**What it demonstrates:** Query input parameters
**Concepts:** `:in` clause, scalar inputs, collection inputs, tuple/relation inputs
**Time:** 15 minutes

Pass parameters into queries for reusability and parameterized queries.

#### `aggregation_demo.go`
**What it demonstrates:** Aggregation functions
**Concepts:** `sum`, `count`, `avg`, `min`, `max`, grouping
**Time:** 15 minutes

Compute aggregate statistics across groups of data.

```datalog
[:find ?dept (avg ?salary)
 :where [?e :person/department ?dept]
        [?e :person/salary ?salary]]
```

---

### Level 4: Advanced Features

Tackle complex queries with subqueries, time-based queries, and multi-row relations.

#### `subquery_simple_demo.go`
**What it demonstrates:** Basic subqueries
**Concepts:** `(q [...])` syntax, tuple binding, passing inputs to subqueries
**Time:** 20 minutes
**Prerequisites:** Aggregation, expression clauses

Execute queries within queries for modular, composable logic.

```datalog
[(q [:find (max ?price)
     :where [?p :product/price ?price]]
    ) [[?max-price]]]
```

#### `subquery_proper_demo.go`
**What it demonstrates:** Advanced subquery patterns
**Concepts:** Multiple subqueries, relation binding, complex data flows
**Time:** 25 minutes
**Prerequisites:** `subquery_simple_demo.go`

Sophisticated subquery usage with multiple subqueries and bindings.

#### `subquery_ohlc_demo.go`
**What it demonstrates:** Real-world financial OHLC calculations
**Concepts:** Open-High-Low-Close price aggregation, time-based filtering
**Time:** 20 minutes
**Prerequisites:** Subqueries, aggregation, time functions

Calculate stock market OHLC prices using subqueries - a complete real-world example.

#### `multi_row_relation_demo.go`
**What it demonstrates:** Multi-row relation binding
**Concepts:** Relations as inputs, filtering with relation sets, batch operations
**Time:** 15 minutes

Pass multiple tuples into patterns for efficient batch filtering.

#### `proof_multi_row_relations.go`
**What it demonstrates:** Relation-based filtering mechanics
**Concepts:** How Relations constrain pattern matches, multi-column binding
**Time:** 10 minutes

Proof-of-concept showing how the Relations API filters pattern matches.

#### `time_functions_demo.go`
**What it demonstrates:** Time extraction functions
**Concepts:** `year`, `month`, `day`, `hour`, `minute`, `second` functions
**Time:** 10 minutes

Extract components from `time.Time` values for temporal analysis.

```datalog
[:find ?date ?count
 :where [?e :event/time ?t]
        [(year ?t) ?year]
        [(month ?t) ?month]
        [(day ?t) ?date]
        [(= ?year 2025)]]
```

---

### Level 5: Performance & Observability

Understand query execution, optimization, and observability.

#### `planner_demo.go`
**What it demonstrates:** Query planning and optimization
**Concepts:** Index selection, phase-based execution, selectivity estimation
**Time:** 20 minutes

See how queries are broken into phases and optimized for execution.

#### `annotations_demo.go`
**What it demonstrates:** Query execution observability
**Concepts:** Annotation system, event handlers, performance monitoring
**Time:** 15 minutes

Monitor query execution with zero-overhead annotation decorators.

#### `annotation_timeout_demo.go`
**What it demonstrates:** Query timeouts and cancellation
**Concepts:** Context timeouts, graceful cancellation, resource cleanup
**Time:** 10 minutes

Prevent runaway queries with timeouts and context cancellation.

#### `crossproduct_debug_demo.go`
**What it demonstrates:** Cartesian product detection
**Concepts:** Disjoint relation groups, query validation, avoiding performance pitfalls
**Time:** 10 minutes

Understand how the engine detects and prevents accidental Cartesian products.

---

### Level 6: Time-Based Queries

Work with temporal data and historical queries.

#### `financial_time_demo.go`
**What it demonstrates:** Time-based transactions
**Concepts:** Transaction IDs as timestamps, temporal ordering
**Time:** 15 minutes

Use timestamps as transaction IDs for time-ordered data.

#### `financial_asof_demo.go`
**What it demonstrates:** As-of queries (point-in-time)
**Concepts:** Historical queries, time travel, temporal consistency
**Time:** 20 minutes
**Prerequisites:** `financial_time_demo.go`

Query data as it existed at a specific point in time.

---

### Level 7: Domain-Specific Examples

Real-world use cases demonstrating complete applications.

#### `company_dataset.go`
**What it demonstrates:** Company/employee domain model
**Concepts:** Entity relationships, multi-attribute entities, organizational hierarchies
**Time:** 15 minutes

Model companies, employees, and departments with relationships.

#### `financial_entities.go`
**What it demonstrates:** Financial data modeling
**Concepts:** Stock entities, price updates, time-series facts
**Time:** 15 minutes

Proper entity modeling for financial market data.

#### `iot_example.go`
**What it demonstrates:** IoT sensor data analysis
**Concepts:** Time-series sensor readings, device entities, measurement queries
**Time:** 15 minutes

Model and query IoT device sensor data.

#### `gopher_street_integration.go`
**What it demonstrates:** Integration with gopher-street market data system
**Concepts:** Real-time position tracking, complex financial queries, multi-symbol analysis
**Time:** 25 minutes
**Prerequisites:** `financial_entities.go`, subqueries, aggregation

Complete integration showing real-world financial market data queries.

---

## Examples by Feature

### Storage & Persistence
- `storage_example.go` - BadgerDB persistent storage
- `long_values_example.go` - Unbounded value sizes

### Query Language
- `simple_example.go` - Basic queries
- `query_execution.go` - Pattern matching
- `join_demo.go` - Joins
- `binding_demo.go` - Variable binding
- `expression_demo.go` - Expressions
- `comparator_demo.go` - Comparisons
- `in_clause_demo.go` - Input parameters

### Aggregation
- `aggregation_demo.go` - Sum, count, avg, min, max

### Subqueries
- `subquery_simple_demo.go` - Basic subqueries
- `subquery_proper_demo.go` - Advanced subqueries
- `subquery_ohlc_demo.go` - OHLC calculations

### Time & History
- `time_functions_demo.go` - Time extraction
- `financial_time_demo.go` - Temporal transactions
- `financial_asof_demo.go` - Point-in-time queries

### Relations
- `multi_row_relation_demo.go` - Multi-row binding
- `proof_multi_row_relations.go` - Relation mechanics

### Performance
- `planner_demo.go` - Query planning
- `annotations_demo.go` - Observability
- `annotation_timeout_demo.go` - Timeouts
- `crossproduct_debug_demo.go` - Cartesian product detection

### Parsing
- `parser_demo.go` - EDN parsing
- `parser_errors_demo.go` - Error handling

### Domain Examples
- `company_dataset.go` - Company/employee data
- `financial_entities.go` - Financial modeling
- `iot_example.go` - IoT sensors
- `gopher_street_integration.go` - Market data integration

---

## Example Quality Standards

All examples in this directory:
- ✅ Build successfully with `go build -tags example`
- ✅ Execute actual queries (no parse-only examples)
- ✅ Include clear comments explaining concepts
- ✅ Show expected output
- ✅ Demonstrate distinct features (no duplication)
- ✅ Are self-contained (no external dependencies except gopher-street integration)

---

## Contributing Examples

When adding new examples:

1. **Use the `example` build tag**:
   ```go
   //go:build example
   // +build example
   ```

2. **Make it executable**: Examples should run queries and show output, not just parse/format

3. **Be focused**: Each example should demonstrate ONE concept clearly

4. **Add comments**: Explain what's being demonstrated and why

5. **Show output**: Use `fmt.Println` to show results

6. **Name clearly**: Use descriptive names (`subquery_demo.go`, not `demo1.go`)

7. **Update this README**: Add your example to the appropriate section

---

## Troubleshooting

**Example won't build:**
- Ensure you're using the `-tags example` flag
- Check you're in the project root directory
- Verify Go 1.21+ is installed

**Need more help?**
- See [CLAUDE.md](../CLAUDE.md) for architecture overview
- See [DATOMIC_COMPATIBILITY.md](../DATOMIC_COMPATIBILITY.md) for Datomic users
- Check [TODO.md](../TODO.md) for known issues

---

## What's NOT in Examples

These features exist but lack examples (contributions welcome):
- ❌ NOT clauses
- ❌ OR clauses
- ❌ Schema management (if implemented)
- ❌ Pull API (if implemented)

See [TODO.md](../TODO.md) for the roadmap.
