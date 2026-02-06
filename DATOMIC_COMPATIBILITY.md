# DATOMIC_COMPATIBILITY.md

This guide is for developers familiar with Datomic who want to understand what janus-datalog supports and what differs from Datomic's feature set.

## Quick Summary

**Estimated feature parity: ~70%** (weighted by typical usage frequency)

Janus-datalog implements a substantial subset of Datomic's Datalog query language:

**Core Features (fully implemented):**
- ✅ Query patterns, expressions, aggregations, subqueries, order-by
- ✅ Pull API with nested references, wildcards, defaults, cycle detection
- ✅ NOT/OR clauses: `(not ...)`, `(not-join ...)`, `(or ...)`, `(or-join ...)`
- ✅ Database functions: `get-else`, `missing?`, `get-some`
- ✅ Schema: type validation, cardinality (one/many/vector), uniqueness
- ✅ CRDT storage: LWW for cardinality-one, add-wins for cardinality-many, RGA for cardinality-vector
- ✅ Time queries: `[(history)]`, `[(as-of ?tx N)]`, `[(tx-between ?tx low high)]`

**Go-Specific Ergonomics:**
- ✅ Struct Reflection API: Go structs ↔ datoms with automatic schema generation
- ✅ QueryInto API: typed query results directly into Go structs (including scalars)
- ✅ Query Builder (qb): compile-time safe query construction with IDE support

**Storage:**
- ✅ BadgerDB persistent storage with CRDT model and 7 indices (EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV)
- ✅ Export/Import to EDN format for backup and migration

**Not Implemented:**
- ❌ Rules (recursive queries)
- ❌ Transaction functions
- ❌ Entity API (navigation)
- ❌ Distributed queries (single-node only)

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
- `:order-by` - result ordering with multi-column and direction support

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

**Comparison binding (bind boolean result to variable):**
```clojure
[(> ?count 0) ?has-items]      ; ?has-items = true/false
[(= ?status "active") ?active] ; ?active = true/false
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

### 8. NOT/OR Clauses

Logical negation and disjunction for complex query patterns:

**NOT - Anti-join (exclude matching tuples):**
```clojure
[:find ?name
 :where [?e :person/name ?name]
        (not [?e :person/archived true])]
```

**NOT-JOIN - Anti-join with explicit join variables:**
```clojure
[:find ?name
 :where [?e :person/name ?name]
        (not-join [?e]
                  [?e :archived true]
                  [?e :deleted true])]
```

**OR - Union of branches:**
```clojure
[:find ?name
 :where [?e :person/name ?name]
        (or [?e :status "active"]
            [?e :status "pending"])]
```

**OR-JOIN - Union with explicit join variables:**
```clojure
[:find ?name
 :where [?e :person/name ?name]
        (or-join [?e]
                 [?e :user/status "active"]
                 [?e :admin/status "enabled"])]
```

**OR with fallback expressions:**

When an OR clause contains expression branches (e.g., `ground`, arithmetic), janus-datalog uses **fallback semantics**: branches are tried in order, returning the first non-empty result. This enables default values:

```clojure
;; Return "Unknown" if no name exists
[:find ?name
 :where (or [?e :person/name ?name]
            [(ground "Unknown") ?name])]

;; Count with zero default
[:find ?count
 :where (or [(q [:find (count ?t) :where [?t :task/done true]] $) [[?count]]]
            [(ground 0) ?count])]
```

This differs from Datomic, where OR always uses union semantics. See [OR_FALLBACK_SEMANTICS.md](docs/reference/OR_FALLBACK_SEMANTICS.md) for details.

**Note:** NOT clauses support data patterns. Predicates and expressions inside NOT are not yet supported.

### 9. Time-Based Queries

Query database as of specific times:

```go
// Query as of a timestamp
db.AsOf(timestamp)
```

Every datom includes a transaction ID for temporal queries.

### 10. Storage Model

- **EAVT model** with Entity-Attribute-Value-Transaction tuples
- **Seven indices**: EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV (EATV/AETV use Tx-descending for CRDT resolution)
- **BadgerDB backend** for persistence
- **L85 encoding** for sortable, efficient keys
- **Export/Import** to EDN format for backup and migration (see [docs/reference/EXPORT_IMPORT.md](docs/reference/EXPORT_IMPORT.md))

### 11. Type System

**Supported types:**
- Primitives: `string`, `int64`, `float64`, `bool`
- `time.Time` for temporal values
- `[]byte` for binary data
- Entity references via Identity type
- Keywords as first-class values

### 12. Pull API ✅

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

### 13. Struct Reflection API ✅

**Go-specific feature** for ergonomic struct ↔ datom conversion:

```go
import "github.com/wbrown/janus-datalog/datalog/reflect"

// Define domain types with struct tags
type Person struct {
    ID      datalog.Identity `datalog:"-,id"`      // Entity identity
    Name    string           `datalog:"name"`      // → :person/name
    Age     int64            `datalog:"age"`       // → :person/age
    Tags    []string         `datalog:"tags"`      // → :person/tags (many)
    Manager *Person          `datalog:"manager"`   // → :person/manager (ref)
}

// Generate schema from struct
schema, _ := reflect.SchemaFromStruct(Person{})
db, _ := storage.NewDatabaseWithSchema(path, schema)

// Write struct as datoms
alice := &Person{Name: "Alice", Age: 30, Tags: []string{"dev"}}
tx := db.NewTransaction()
aliceID, _ := tx.AddStructAuto(alice)  // Auto-generate ID
tx.Commit()

// Read datoms into struct
var loaded Person
db.PullInto(aliceID, &loaded)
// loaded.Name == "Alice", loaded.Tags == ["dev"]
```

**Key features:**
- `SchemaFromStruct()` - Generate schema from Go struct definitions
- `AddStructAuto()` - Write struct with auto-generated unique ID
- `AddStruct()` - Write struct with explicit entity ID
- `PullInto()` - Read entity into struct using Pull API
- `PullIntoMany()` - Read multiple entities into slice

**Tag format:**
| Tag | Meaning |
|-----|---------|
| `datalog:"name"` | Attribute local name (namespace from struct) |
| `datalog:"ns/name"` | Full attribute name |
| `datalog:"-"` | Skip this field |
| `datalog:"-,id"` | Entity identity field |

**Namespace derivation:**
- `Person` → `person`
- `PersonInfo` → `person-info`
- `HTTPServer` → `http-server`

**Cardinality inference:**
- Single values (`string`, `int64`, `*Person`) → cardinality-one
- Slices (`[]string`, `[]*Person`) → cardinality-many

**Note:** This is a Go-specific ergonomic feature not found in Datomic. It builds on the Pull API and Schema support to provide idiomatic Go development.

See [docs/reference/REFLECT.md](docs/reference/REFLECT.md) for complete documentation.

### 14. QueryInto API ✅

**Go-specific feature** for typed query results - eliminates manual tuple iteration:

```go
// Define result struct with datalog tags
type TradeResult struct {
    Symbol string    `datalog:"?symbol"`
    Price  float64   `datalog:"?price"`
    Date   time.Time `datalog:"?date"`
}

// Query directly into typed slice
var results []TradeResult
err := db.QueryInto(&results, `
    [:find ?symbol ?price ?date
     :where [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]
            [?t :trade/date ?date]]
`)

// Use with aggregates - tag matches :find expression exactly
type DeptStats struct {
    Dept      string  `datalog:"?dept"`
    TotalPay  float64 `datalog:"(sum ?salary)"`
    HeadCount int64   `datalog:"(count ?emp)"`
}

var stats []DeptStats
err := db.QueryInto(&stats, `
    [:find ?dept (sum ?salary) (count ?emp)
     :where [?e :employee/dept ?dept]
            [?e :employee/salary ?salary]]
`)

// QueryOneInto for single-result queries
var result TradeResult
found, err := db.QueryOneInto(&result, `
    [:find ?symbol ?price ?date
     :where [?t :trade/id ?id]
            [(= ?id 12345)]
            [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]
            [?t :trade/date ?date]]
`)

// Scalar queries - single column, no struct needed
var names []string
db.QueryInto(&names, `[:find ?name :where [?e :person/name ?name]]`)

var count int64
found, err := db.QueryOneInto(&count, `[:find (count ?e) :where [?e :person/name _]]`)
```

**Scalar types supported:** `string`, `int64`, `float64`, `bool`, `time.Time`, `datalog.Identity`, `datalog.Keyword`, `[]byte`

**Tag mapping:**
- Variables: `datalog:"?symbol"` matches `:find ?symbol`
- Aggregates: `datalog:"(sum ?salary)"` matches `:find (sum ?salary)` exactly
- Positional: Omit tags entirely to use field order = result column order

**Error handling:**
- `ErrNotFound` - QueryOneInto returns no results
- `ErrMultipleResults` - QueryOneInto returns >1 result
- `ErrSymbolNotFound` - Tag references symbol not in query

See [docs/reference/QUERY_INTO.md](docs/reference/QUERY_INTO.md) for complete documentation.

### 15. Query Builder (qb) ✅

**Go-specific feature** for compile-time safe query construction:

```go
import "github.com/wbrown/janus-datalog/datalog/qb"

// Define attributes as constants - typos caught at compile time
var PersonName = qb.Kw(":person/name")
var PersonAge = qb.Kw(":person/age")

// Variables created with NewVar() - same pointer = join
e := qb.NewVar("e")
name := qb.NewVar("name")
age := qb.NewVar("age")

q := qb.Query().
    Find(name, age).
    Where(
        qb.Pat(e, PersonName, name),
        qb.Pat(e, PersonAge, age),  // same e = join!
        qb.Gt(age, 21),
    ).
    MustBuild()

// Use with any query method
results, err := db.ExecuteQuery(q)
```

**Key benefits:**
- Compile-time safety: attribute typos caught immediately
- IDE support: autocomplete for attributes and functions
- Variable identity = join semantics (same `*Var` pointer in multiple patterns)
- Full Datalog support: patterns, predicates, expressions, aggregations, NOT/OR, subqueries

**Available functions:**
- Patterns: `Pat()` (2-5 elements), `Blank()` for wildcards
- Predicates: `Lt`, `Gt`, `Eq`, `Ne`, `Lte`, `Gte`, `Range`
- Expressions: `Add`, `Sub`, `Mul`, `Div`, `Str`, `Ground`, `Identity`
- Time: `Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`
- Aggregates: `Sum`, `Count`, `Avg`, `Min`, `Max`
- Logic: `Not`, `NotJoin`, `Or`, `OrJoin`
- Inputs: `DB`, `Scalar`, `Collection`, `Tuple`, `Relation`

See [docs/reference/QUERY_BUILDER.md](docs/reference/QUERY_BUILDER.md) for complete documentation.

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

### 2. Schema (Partial Support) ⚠️

Schema is supported but with limitations vs Datomic:

**Supported schema features:**
- `:db/valueType` - type constraints (string, long, double, boolean, instant, bytes, ref, keyword)
- `:db/cardinality` - one or many
- `:db/unique` - value uniqueness or identity
- `:db/doc` - documentation strings

**Schema definition via EDN:**
```clojure
{:person/name   {:db/valueType   :db.type/string
                 :db/cardinality :db.cardinality/one}
 :person/friends {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/many}
 :person/email  {:db/valueType   :db.type/string
                 :db/unique      :db.unique/value}}
```

**Schema definition via Go API:**
```go
schema, _ := schema.NewBuilder().
    Attribute(":person/name").Type(schema.TypeString).Add().
    Attribute(":person/friends").Type(schema.TypeRef).Many().Add().
    Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
    Build()

db, _ := storage.NewDatabaseWithSchema(path, schema)
```

**Schema behavior:**
- Type validation enforced on transaction `Add()`
- Uniqueness validation enforced on `Commit()`
- Unknown attributes allowed (additive schema)
- Pull API uses schema for cardinality-many handling
- Schema is optional - existing behavior preserved without schema

**Schema limitations vs Datomic:**
- No `:db/isComponent` - component entity semantics not implemented
- No `:db/index` - all attributes are indexed by default
- No `:db/fulltext` - fulltext search not supported
- No `:db/noHistory` - all datoms are retained
- No upsert semantics - `:db.unique/identity` behaves like `:db.unique/value`
- Schema not stored as datoms - schema is in-memory only

**Performance (writes only):** Type validation adds <1% overhead at `Add()` time; uniqueness checking adds ~6% at `Commit()` time. Reads are unaffected. See `PERFORMANCE_STATUS.md` for benchmarks.

### 3. Transaction Features ⚠️

Partial transaction support:
- ✅ **CRDT semantics** - LWW, add-wins, and RGA based on attribute cardinality
- ✅ **Soft removal** - `Remove(e, a, v)` tombstones for cardinality-many
- ✅ **Hard deletion** - `Retract(e, a, v)` for explicit removal (GDPR, cleanup)
- ❌ **No transaction functions**
- ❌ **No tempids** for new entities
- ❌ **No transaction metadata** beyond ElementID (Lamport + ReplicaID)
- ❌ **No transaction entities**

### 4. Entity API ❌

No entity navigation:

```clojure
; NOT SUPPORTED:
(:person/name entity)
(:person/friends entity)
(touch entity)
```

### 5. Advanced Query Features (Partial) ⚠️

**Implemented:**
- `get-else` - default values for missing attributes ✅
- `missing?` - test attribute absence ✅
- `get-some` - first available attribute from fallback list ✅

**Not implemented:**
- `tuple` - no tuple destructuring in find
- `keys` - no map results
- `with` - no duplicate control

### 6. Time and History Features ✅

**Supported:**
- CRDT storage inherently preserves all writes with ElementIDs (Lamport + ReplicaID)
- `[(history)]` predicate returns all historical versions
- `[(as-of ?tx N)]` returns value as of Lamport time N
- `[(tx-between ?tx low high)]` filters results to a Lamport range

**History Query API:**
```go
// All writes are preserved with CRDT semantics - no special mode needed
db, _ := storage.NewDatabase("/path/to/db")

// Make changes over time
tx := db.NewTransaction()
tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
tx.Commit()  // Lamport 1

tx2 := db.NewTransaction()
tx2.Add(alice, datalog.NewKeyword(":person/name"), "Alicia")  // LWW: replaces "Alice"
tx2.Commit()  // Lamport 2

// Query current state - returns latest value
results, _ := db.ExecuteQuery(`[:find ?name :where [?e :person/name ?name]]`)
// Returns: [["Alicia"]]

// Query ALL historical values using [(history)] predicate
history, _ := db.ExecuteQuery(`[:find ?name ?tx :where [?e :person/name ?name ?tx] [(history)]]`)
// Returns: [["Alice" <ElementID@1>] ["Alicia" <ElementID@2>]]
```

**Not supported:**
- No `since` queries (use `[(tx-between ?tx start ∞)]` instead)

### 7. Database Features ❌

No advanced database operations:
- No database branching/forking
- No speculative transactions
- No database filters
- No log API

### 8. Other Missing Features ❌

- **Nested expressions in predicates**: Cannot do `[(< (- ?t2 ?t1) 300)]`
- **Distinct in aggregations**: No `(count-distinct ?x)`
- **Custom aggregation functions**
- **Query caching or prepared queries**
- **Distributed queries** (single-node only)

## Key Differences from Datomic

### 1. Storage Design
- Uses BadgerDB instead of Datomic's segmented storage
- L85 encoding (custom Base85) for sortable keys
- Fixed 69-byte keys: E(20) + A(32) + Tx(16) + Op(1)
- Seven indices: EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV

### 2. Transaction Model
- ElementID-based transactions (Lamport clock + ReplicaID)
- CRDT semantics: LWW (one), add-wins (many), RGA (vector)
- No entity-based transactions
- Designed for multi-replica support

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

**Easy migrations (port directly):**
- Queries with patterns, expressions, predicates, aggregations
- Pull API expressions (including nested refs, wildcards, defaults)
- NOT/OR clauses
- Subqueries with tuple/relation bindings
- Order-by clauses
- Time-travel queries via `[(history)]` and `[(as-of ?tx N)]`
- Database functions (get-else, missing?, get-some)

**Require refactoring:**
- Rules → inline the logic or handle recursion in application code
- Entity navigation → use Pull API or explicit joins

**Not possible:**
- Transaction functions
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
stores an entity reference. With schema support, cardinality-many attributes return
all values as an array. Use `ResolvePullPattern()` and `PullResolved()` for proper
cardinality handling.

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

These make it suitable for production workloads with substantial Datomic feature coverage.

## Recommended Use Cases

**Good fit for:**
- OLAP/analytical queries on moderate to large datasets
- Time-series data with temporal queries and full audit trails
- Applications needing Datalog's expressiveness with Go's performance
- Entity retrieval using Pull API (9× faster than equivalent queries)
- Applications requiring NOT/OR logic and subqueries
- Single-node deployments with BadgerDB persistence

**Consider alternatives for:**
- Recursive/graph queries requiring rules (no rule support)
- Distributed/multi-node requirements (single-node only)

## Getting Started

For Datomic users, the transition is straightforward:

1. Most queries port directly, including Pull expressions and NOT/OR clauses
2. Use subqueries for complex aggregations with proper scoping
3. Define schema for type validation and cardinality support (one/many/vector)
4. Use Pull API for efficient entity navigation (replaces Entity API)
5. CRDT storage preserves full history automatically - use `[(history)]` for time-travel queries

**For Go developers**, additional ergonomic APIs are available:
- **Struct Reflection API** (`datalog/reflect`): Go structs ↔ datoms with automatic schema generation
- **QueryInto API**: Typed query results directly into Go structs (no manual tuple iteration)
- **PullInto**: Read entities directly into Go structs

Most Datalog queries will work with minimal changes, making janus-datalog a practical choice for applications that need Datalog's power without Datomic's full complexity.