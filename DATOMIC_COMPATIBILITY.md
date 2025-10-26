# DATOMIC_COMPATIBILITY.md

This guide is for developers familiar with Datomic who want to understand what janus-datalog supports and what differs from Datomic's feature set.

## Quick Summary

Janus-datalog implements a pragmatic subset of Datomic's Datalog query language with:
- ✅ Core query patterns, expressions, aggregations, and subqueries
- ✅ Time-based queries using transaction IDs
- ✅ BadgerDB persistent storage with EAVT model
- ❌ No Pull API, rules, schema, or transaction functions
- ❌ No NOT/OR clauses or entity API
- ❌ Single-node only (no distributed queries)

## Implemented Datomic Features

### 1. Core Query Language

Basic Datalog queries work as expected:

```clojure
[:find ?name ?age
 :where [?e :person/name ?name]
        [?e :person/age ?age]
        [(> ?age 21)]]
```

**Supported clauses:**
- `:find` - with variables and aggregations
- `:where` - pattern matching and expressions
- `:in` - database and parameter inputs
- `:order-by` - result ordering (parser only, executor pending)

**Pattern matching:**
- `[?e ?a ?v]` - basic triple patterns
- `[?e ?a ?v ?tx]` - with transaction
- `_` - wildcards for ignored positions
- Direct values - `"AAPL"`, `42`, `:status/active`

### 2. Expression Clauses

**Arithmetic operations:**
```clojure
[(+ ?price ?tax) ?total]
[(- ?end ?start) ?duration]
[(* ?quantity ?price) ?amount]
[(/ ?total ?count) ?average]
```

**Comparisons (including variadic):**
```clojure
[(< ?x 100)]
[(> ?y ?z)]
[(= ?a ?b)]
[(!= ?x ?y)]
[(< 0 ?x 100)]  ; chained comparison
```

**String operations:**
```clojure
[(str ?first " " ?last) ?fullname]
```

**Utility functions:**
```clojure
[(ground 42) ?answer]
[(identity ?x) ?y]
```

### 3. Aggregation Functions

All standard aggregations with grouping:

```clojure
[:find ?dept (sum ?salary) (count ?emp) (avg ?salary)
 :where [?emp :employee/dept ?dept]
        [?emp :employee/salary ?salary]]
```

**Supported aggregates:**
- `(sum ?x)`
- `(count ?x)`
- `(avg ?x)`
- `(min ?x)`
- `(max ?x)`

### 4. Time Functions

Extract time components for temporal analysis:

```clojure
[:find ?year ?month ?total
 :where [?sale :sale/date ?date]
        [(year ?date) ?year]
        [(month ?date) ?month]
        [?sale :sale/amount ?amount]
        [(ground 2024) ?target-year]
        [(= ?year ?target-year)]]
```

**Available functions:**
- `(year ?time)`, `(month ?time)`, `(day ?time)`
- `(hour ?time)`, `(minute ?time)`, `(second ?time)`

### 5. Subqueries

Full subquery support for proper aggregation scoping:

```clojure
[:find ?ticker ?date ?ohlc
 :where [?s :symbol/ticker ?ticker]
        [(q [:find (max ?high) (min ?low) (avg ?close)
             :in $ ?sym ?d
             :where [?bar :bar/symbol ?sym]
                    [?bar :bar/date ?d]
                    [?bar :bar/high ?high]
                    [?bar :bar/low ?low]
                    [?bar :bar/close ?close]]
            ?s ?date) [[?ohlc]]]]
```

**Binding types:**
- `[[?var]]` - TupleBinding (single result)
- `[[?var1 ?var2] ...]` - RelationBinding (multiple results)

### 6. Query Inputs

Standard Datomic input patterns:

```clojure
[:find ?name ?age
 :in $ ?min-age [?status ...]
 :where [?e :person/name ?name]
        [?e :person/age ?age]
        [?e :person/status ?status]
        [(>= ?age ?min-age)]]
```

**Supported inputs:**
- `$` - database
- `?var` - scalar
- `[?var ...]` - collection
- `[[?x ?y]]` - tuple
- `[[?x ?y] ...]` - relation

### 7. Time-Based Queries

Query database as of specific times:

```go
// Query as of a timestamp
db.AsOf(timestamp)
```

Every datom includes a transaction ID for temporal queries.

### 8. Storage Model

- **EAVT model** with Entity-Attribute-Value-Transaction tuples
- **Multiple indices**: EAVT, AEVT, AVET, VAET, TAEV
- **BadgerDB backend** for persistence
- **L85 encoding** for sortable, efficient keys

### 9. Type System

**Supported types:**
- Primitives: `string`, `int64`, `float64`, `bool`
- `time.Time` for temporal values
- `[]byte` for binary data
- Entity references via Identity type
- Keywords as first-class values

## Missing Datomic Features

### 1. Pull API ❌

No support for pull patterns:

```clojure
; NOT SUPPORTED:
[:find (pull ?e [*])
 :where [?e :person/name "John"]]

; Use explicit patterns instead:
[:find ?name ?email ?age
 :where [?e :person/name "John"]
        [?e :person/email ?email]
        [?e :person/age ?age]]
```

### 2. Rules ❌

No rule definitions or recursive queries:

```clojure
; NOT SUPPORTED:
[[(ancestor ?a ?d)
  [?a :parent ?d]]
 [(ancestor ?a ?d)
  [?a :parent ?p]
  (ancestor ?p ?d)]]
```

### 3. NOT and OR Clauses ❌

No logical negation or disjunction:

```clojure
; NOT SUPPORTED:
(not [?e :archived true])
(or [?e :status "active"] 
    [?e :status "pending"])
(or-join [?e] ...)
```

### 4. Schema ❌

No schema definitions or constraints:
- No cardinality specifications (one/many)
- No type constraints at schema level
- No uniqueness constraints
- No required attributes
- All attributes effectively `:db.cardinality/one`

### 5. Transaction Features ❌

Limited transaction support:
- **No transaction functions**
- **No retractions** (only assertions)
- **No tempids** for new entities
- **No transaction metadata** beyond timestamp
- **No transaction entities**

### 6. Entity API ❌

No entity navigation:

```clojure
; NOT SUPPORTED:
(:person/name entity)
(:person/friends entity)
(touch entity)
```

### 7. Advanced Query Features ❌

Missing query conveniences:
- `get-else` - no default values
- `missing?` - cannot test attribute absence
- `tuple` - no tuple destructuring in find
- `keys` - no map results
- `with` - no duplicate control

### 8. Advanced Time Features ❌

Limited to as-of queries:
- No `since` queries
- No `history` database
- No tx-range queries
- No full history API

### 9. Database Features ❌

No advanced database operations:
- No database branching/forking
- No speculative transactions
- No database filters
- No log API

### 10. Other Missing Features ❌

- **Nested expressions in predicates**: Cannot do `[(< (- ?t2 ?t1) 300)]`
- **Distinct in aggregations**: No `(count-distinct ?x)`
- **Custom aggregation functions**
- **Query caching or prepared queries**
- **Distributed queries** (single-node only)

## Key Differences from Datomic

### 1. Storage Design
- Uses BadgerDB instead of Datomic's segmented storage
- L85 encoding (custom Base85) for sortable keys
- Fixed 72-byte keys: E(20) + A(32) + Tx(20)

### 2. Transaction Model
- Time-based uint64 transaction IDs
- No entity-based transactions
- Simpler transaction model overall

### 3. Type System
- Direct Go types instead of tagged literals
- No EDN type tags in storage
- Simpler serialization model

### 4. Architecture
- Single-node design
- No peer/client architecture
- No distributed query coordination

### 5. Performance Optimizations
- Dynamic join reordering
- Predicate pushdown
- Streaming iterators
- Relation collapsing algorithm

## Migration Considerations

### From Datomic to Janus-Datalog

**Easy migrations:**
- Basic queries port directly
- Simple aggregations work unchanged
- Time-based queries similar (using AsOf)

**Require refactoring:**
- Pull patterns → explicit patterns
- Rules → inline the logic
- Entity navigation → explicit joins
- Schema validations → application layer

**Not possible:**
- Transaction functions
- History queries beyond as-of
- Distributed queries

### Example Query Conversions

**Datomic Pull API:**
```clojure
[:find (pull ?e [:person/name 
                  :person/email 
                  {:person/friends [:person/name]}])
 :where [?e :person/age ?age]
        [(> ?age 21)]]
```

**Janus-Datalog equivalent:**
```clojure
[:find ?name ?email ?friend-name
 :where [?e :person/age ?age]
        [(> ?age 21)]
        [?e :person/name ?name]
        [?e :person/email ?email]
        [?e :person/friends ?friend]
        [?friend :person/name ?friend-name]]
```

**Datomic with Rules:**
```clojure
[:find ?person ?ancestor
 :in $ %
 :where (ancestor ?person ?ancestor)]
```

**Janus-Datalog equivalent (must inline):**
```clojure
; Must explicitly write out the recursive logic
; or handle in application code
```

## Performance Characteristics

Janus-datalog includes sophisticated optimizations:

1. **Relation Collapsing**: Prevents memory exhaustion on complex queries
2. **Dynamic Join Ordering**: Starts with most selective relations
3. **Early Termination**: Stops on empty intermediate results
4. **Index Selection**: Chooses optimal index based on bound values
5. **Streaming Execution**: Avoids materializing large datasets

These make it suitable for production workloads despite the simpler feature set.

## Recommended Use Cases

**Good fit for:**
- OLAP/analytical queries on moderate datasets
- Time-series data with temporal queries
- Applications needing Datalog's expressiveness
- Single-node deployments

**Consider alternatives for:**
- Applications heavily using Pull API
- Recursive/graph queries requiring rules
- Strong schema enforcement needs
- Distributed/multi-node requirements

## Getting Started

For Datomic users, the transition is straightforward for basic queries:

1. Write patterns instead of pull expressions
2. Use subqueries for complex aggregations
3. Handle schema validation in your application
4. Use explicit patterns for entity navigation

Most Datalog queries will work with minimal changes, making janus-datalog a practical choice for applications that need Datalog's power without Datomic's full complexity.