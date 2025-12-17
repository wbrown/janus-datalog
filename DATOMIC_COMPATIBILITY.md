# DATOMIC_COMPATIBILITY.md

This guide is for developers familiar with Datomic who want to understand what janus-datalog supports and what differs from Datomic's feature set.

## Quick Summary

Janus-datalog implements a pragmatic subset of Datomic's Datalog query language with:
- ✅ Core query patterns, expressions, aggregations, and subqueries
- ✅ Pull API with nested references and cycle detection
- ✅ Time-based queries using transaction IDs
- ✅ Database functions: `get-else`, `missing?`, `get-some`
- ✅ BadgerDB persistent storage with EAVT model
- ❌ No rules, schema, or transaction functions
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

### 7. Database Functions

Functions that access the database for attribute lookups:

**get-else - default values for missing attributes:**
```clojure
[:find ?name ?nickname
 :where [?e :person/name ?name]
        [(get-else $ ?e :person/nickname "No Nickname") ?nickname]]
```

**missing? - test if attribute is absent (as predicate filter):**
```clojure
[:find ?name
 :where [?e :user/name ?name]
        [(missing? $ ?e :user/email)]]  ; Only users without email
```

**missing? - as expression (returns boolean):**
```clojure
[:find ?name ?needs_verification
 :where [?e :user/name ?name]
        [(missing? $ ?e :user/verified) ?needs_verification]]
```

**get-some - first available attribute from fallback list:**
```clojure
[:find ?id ?display-name
 :where [?e :user/id ?id]
        [(get-some $ ?e :user/nickname :user/fullname :user/email) ?display-name]]
```

These functions require the `$` database reference as their first argument.

### 8. Time-Based Queries

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

### 10. Pull API ✅

Declarative entity attribute retrieval with nested reference following:

**In-query pull:**
```clojure
[:find (pull ?e [:person/name :person/age])
 :where [?e :entity/type :type/person]]

; Mixed with regular variables
[:find ?type (pull ?e [:person/name])
 :where [?e :entity/type ?type]]
```

**Standalone API (Go):**
```go
// Single entity
result, err := db.Pull(entityID, `[:user/name :user/age]`)
// result: map[string]interface{}{"user/name": "Alice", "user/age": 30}

// Multiple entities
results, err := db.PullMany(entityIDs, `[:user/name]`)
```

**Supported pull patterns:**
- Simple attributes: `[:attr1 :attr2]`
- Wildcard: `[*]` (all attributes)
- Nested references: `{:user/region [:region/code :region/name]}`
- Default values: `(default :attr "fallback")`
- Limit (parsed, cardinality-many not yet implemented): `(limit :attr 10)`

**Key behaviors:**
- Result keys omit leading colon: `"user/name"` not `":user/name"`
- Missing attributes are omitted from results (not nil)
- Cycle detection prevents infinite loops on circular references
- Unlimited nesting depth

**Performance characteristics:**
- Uses optimized `EntityLookupMatcher` for direct index seeks
- Each attribute lookup is a single AEVT index seek (no query parsing/planning)
- Wildcard `[*]` uses single EAVT prefix scan
- Far more efficient than equivalent application-level N+1 queries

## Missing Datomic Features

### 1. Rules ❌

No rule definitions or recursive queries:

```clojure
; NOT SUPPORTED:
[[(ancestor ?a ?d)
  [?a :parent ?d]]
 [(ancestor ?a ?d)
  [?a :parent ?p]
  (ancestor ?p ?d)]]
```

### 2. NOT and OR Clauses ❌

No logical negation or disjunction:

```clojure
; NOT SUPPORTED:
(not [?e :archived true])
(or [?e :status "active"] 
    [?e :status "pending"])
(or-join [?e] ...)
```

### 3. Schema ❌

No schema definitions or constraints:
- No cardinality specifications (one/many)
- No type constraints at schema level
- No uniqueness constraints
- No required attributes
- All attributes effectively `:db.cardinality/one`

### 4. Transaction Features ❌

Limited transaction support:
- **No transaction functions**
- **No retractions** (only assertions)
- **No tempids** for new entities
- **No transaction metadata** beyond timestamp
- **No transaction entities**

### 5. Entity API ❌

No entity navigation:

```clojure
; NOT SUPPORTED:
(:person/name entity)
(:person/friends entity)
(touch entity)
```

### 6. Advanced Query Features (Partial) ⚠️

**Implemented:**
- `get-else` - default values for missing attributes ✅
- `missing?` - test attribute absence ✅
- `get-some` - first available attribute from fallback list ✅

**Not implemented:**
- `tuple` - no tuple destructuring in find
- `keys` - no map results
- `with` - no duplicate control

### 7. Advanced Time Features ❌

Limited to as-of queries:
- No `since` queries
- No `history` database
- No tx-range queries
- No full history API

### 8. Database Features ❌

No advanced database operations:
- No database branching/forking
- No speculative transactions
- No database filters
- No log API

### 9. Other Missing Features ❌

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
- Rules → inline the logic
- Entity navigation → explicit joins or Pull API
- Schema validations → application layer

**Not possible:**
- Transaction functions
- History queries beyond as-of
- Distributed queries

### Example Query Conversions

**Datomic Pull API (now supported!):**
```clojure
; Works directly in janus-datalog:
[:find (pull ?e [:person/name
                  :person/email
                  {:person/friends [:person/name]}])
 :where [?e :person/age ?age]
        [(> ?age 21)]]
```

Note: Nested references like `{:person/friends [...]}` work when `:person/friends`
stores an entity reference. Cardinality-many attributes are not yet supported,
so only the first friend would be returned.

**Alternative using explicit patterns:**
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
- Entity retrieval using Pull API
- Single-node deployments

**Consider alternatives for:**
- Recursive/graph queries requiring rules
- Strong schema enforcement needs
- Cardinality-many attributes (not yet supported)
- Distributed/multi-node requirements

## Getting Started

For Datomic users, the transition is straightforward:

1. Most queries port directly, including Pull expressions
2. Use subqueries for complex aggregations
3. Handle schema validation in your application
4. Use Pull API or explicit patterns for entity navigation

Most Datalog queries will work with minimal changes, making janus-datalog a practical choice for applications that need Datalog's power without Datomic's full complexity.