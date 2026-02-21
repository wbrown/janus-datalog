---
name: datalog
description: >
  Query janus-datalog databases for debugging and data exploration.
  Use when the user asks to inspect a .db database, explore datoms,
  check entity attributes, debug query results, or examine CRDT storage state.
argument-hint: <database-path>
allowed-tools: Bash(~/go/bin/datalog *)
---

# Datalog Query

Query janus-datalog databases for debugging and data exploration.

The user will provide a database path and describe what they want to know. Use the `datalog` CLI to run queries against the database.

## Data Model

janus-datalog is a **Datomic-style EAV (Entity-Attribute-Value) store**. All data is stored as datoms: `[entity attribute value tx]`. There are no tables — entities are just collections of attribute-value pairs, connected by shared entity IDs.

Attributes follow a namespaced keyword convention: `:namespace/name` (e.g., `:person/name`, `:order/total`). This means attributes cluster by namespace, which is useful when exploring unknown databases.

**Storage is CRDT-backed** (Conflict-free Replicated Data Types). Every write is preserved with a unique ElementID. Attribute cardinality determines conflict resolution:

| Cardinality | Behavior | Example |
|-------------|----------|---------|
| One | Last-Writer-Wins — only the latest value is returned by default | `:person/name`, `:person/age` |
| Many | Add-Wins Set — all distinct values are returned | `:person/tag`, `:person/role` |
| Vector | RGA — ordered list with deterministic merge | `:product/tags`, `:playlist/songs` |

Because all writes are preserved, you can time-travel: `db.History()` returns all raw datoms (no CRDT resolution), and `db.AsOf(elementID)` queries CRDT-resolved state as of a specific transaction.

## Tool

Binary: `~/go/bin/datalog`

Install if missing:
```bash
go install github.com/wbrown/janus-datalog/cmd/datalog@latest
```

## Invocation

```bash
# Run a query
~/go/bin/datalog -db <path> -query '<EDN query>'

# With input bindings (repeatable, one per :in parameter after $)
~/go/bin/datalog -db <path> -query '<EDN query with :in>' -in '<EDN value>' [-in '<EDN value>' ...]

# With performance annotations
~/go/bin/datalog -db <path> -verbose -query '<EDN query>'

# Export entire database to readable EDN (useful for small DBs)
~/go/bin/datalog -db <path> -export <output.edn>
```

Output is a markdown table. Errors go to stderr.

If the user says `/datalog <path>`, treat `$ARGUMENTS` as the database path. Start by discovering what's in the database, then ask what they want to explore.

## Finding Databases

- Prebuilt benchmark DB: `datalog/storage/testdata/ohlc_benchmark.db`
- Default path when unspecified: `datalog.db` in current directory
- Test databases are created in temp directories by tests (look for `t.TempDir()` calls)
- Ask the user if the path is unclear

## EDN Query Syntax

Queries are EDN vectors. The basic form:

```clojure
[:find <find-spec>
 :in <input-spec>       ;; optional, $ is implicit
 :where <clauses>]
```

### Find Spec

```clojure
;; Return specific variables
[:find ?name ?age :where ...]

;; With aggregations (non-aggregated vars become group-by keys)
[:find ?dept (sum ?salary) (count ?e) :where ...]

;; Available: sum, count, avg, min, max
```

### Where Clauses

**Data patterns** — match entity-attribute-value triples:
```clojure
[?entity :attribute ?value]          ;; basic pattern
[?entity :attribute "literal"]       ;; match literal value
[?entity :attribute _]               ;; wildcard (attribute exists)
[?entity :attribute ?value ?tx]      ;; with transaction ID
```

**Predicates** — filter results:
```clojure
[(> ?age 25)]
[(< ?price 100.0)]
[(!= ?name "Alice")]
[(< 0 ?x 100)]          ;; chained: 0 < ?x < 100
```

**Expressions** — compute values:
```clojure
[(+ ?age 5) ?future-age]
[(- ?price ?discount) ?net]
[(* ?qty ?price) ?total]
[(str ?first " " ?last) ?fullname]
[(ground 42) ?answer]
[(identity ?x) ?y]                       ;; pass-through binding
```

**Time extraction** — extract components from `time.Time` values:
```clojure
[(year ?timestamp) ?y]
[(month ?timestamp) ?m]
[(day ?timestamp) ?d]
[(hour ?timestamp) ?h]
[(minute ?timestamp) ?min]
[(second ?timestamp) ?sec]
```

**NOT clauses** — exclude matches:
```clojure
(not [?p :person/deleted true])
(not-join [?p]
  [?p :person/status "inactive"])
```

**OR clauses** — alternative patterns:
```clojure
(or [?p :person/city "New York"]
    [?p :person/city "Boston"])
(or-join [?p]
  [?p :role/admin true]
  [?p :role/superuser true])
```

**Vector functions** — operate on cardinality-vector attributes:
```clojure
[(nth ?vec 0) ?first-element]           ;; get element by index
[(first ?vec) ?head]                     ;; first element
[(last ?vec) ?tail]                      ;; last element
[(length ?vec) ?len]                     ;; number of elements
[(contains? ?vec "value") ?found]        ;; boolean membership test
[(index-of ?vec "value") ?pos]           ;; index of first match
[(subvec ?vec 1 3) ?slice]               ;; sub-vector [start, end)
```

**`enumerate`** — expands a vector into one row per element with index. This is the primary way to query vector contents:
```clojure
;; Given :product/tags is a vector ["electronics" "sale" "new"]
[:find ?tag ?idx
 :where [?e :product/label "Widget"]
        [?e :product/tags ?vec]
        [(enumerate ?vec) [?idx ?tag]]]
;; Returns: [0 "electronics"], [1 "sale"], [2 "new"]
```

Important: `enumerate` produces **multiple output rows** from a single input row. The binding `[?idx ?tag]` is required — it destructures each element into an index and value. The data pattern that binds `?vec` must appear **before** the enumerate expression in the where clause, and the planner must not reorder it ahead of the pattern that provides the vector.

**Database functions:**
```clojure
[(get-else $ ?e :person/nickname "unknown") ?nick]
[(missing? $ ?e :person/email)]
[(get-some $ ?e :person/nick :person/name :person/email) ?display]
```

**Subqueries** — nested queries that bind results into the outer query:
```clojure
;; Tuple binding — subquery returns one row, binds to variables
[(q [:find (max ?h)
     :in $ ?sym
     :where [?p :price/symbol ?sym]
            [?p :price/high ?h]]
    $ ?s) [[?max-high]]]

;; Relation binding — subquery returns multiple rows
[(q [:find ?p ?h
     :in $ ?sym
     :where [?p :price/symbol ?sym]
            [?p :price/high ?h]]
    $ ?s) [[?price ?high] ...]]
```

The subquery `(q [...] $ ?var1 ?var2)` takes a full query, then lists the inputs to pass (matching the subquery's `:in` clause). The binding after `)` determines how results are captured: `[[?x]]` for a single row, `[[?x ?y] ...]` for multiple rows.

**Tagged literals** — use typed constants directly in patterns and predicates:
```clojure
[#identity "L85hash..." :person/name ?name]   ;; match specific entity
[?e :event/date #inst "2024-06-15T10:30:00Z"] ;; match timestamp
[(> ?d #inst "2024-01-01T00:00:00Z")]          ;; compare against timestamp
```

### Input Parameters

```clojure
;; Scalar input
[:find ?name :in $ ?target-age
 :where [?p :person/age ?target-age]
        [?p :person/name ?name]]

;; Collection input
[:find ?name :in $ [?city ...]
 :where [?p :person/city ?city]
        [?p :person/name ?name]]
```

Pass `:in` values with the `-in` flag (one per binding, order matches `:in` after `$`):

```bash
# Scalar input
~/go/bin/datalog -db <path> \
  -query '[:find ?name :in $ ?age :where [?p :person/age ?age] [?p :person/name ?name]]' \
  -in 30

# String input (quote in EDN)
~/go/bin/datalog -db <path> \
  -query '[:find ?age :in $ ?name :where [?p :person/name ?name] [?p :person/age ?age]]' \
  -in '"Alice"'

# Collection input
~/go/bin/datalog -db <path> \
  -query '[:find ?name :in $ [?city ...] :where [?p :person/city ?city] [?p :person/name ?name]]' \
  -in '["New York" "Boston"]'

# Multiple inputs
~/go/bin/datalog -db <path> \
  -query '[:find ?name :in $ ?min ?max :where [?p :person/age ?a] [(>= ?a ?min)] [(<= ?a ?max)] [?p :person/name ?name]]' \
  -in 20 -in 30
```

Each `-in` value is parsed as EDN, so tagged literals work too: `-in '#inst "2024-01-01T00:00:00Z"'`.

### Order By

```clojure
[:find ?name ?age
 :order-by [?age :desc] [?name :asc]
 :where [?p :person/name ?name]
        [?p :person/age ?age]]
```

### Time-Travel

```go
// Open a database
d, _ := db.Open("path/to/db")

// History — all raw datoms, no CRDT resolution
hist := d.History()
hist.Query(`[:find ?name ?tx :where [?p :person/name ?name ?tx]]`)

// As-of — CRDT-resolved state at a specific transaction
txID, _ := tx.Commit()  // returns datalog.ElementID
asOf := d.AsOf(txID)
asOf.Query(`[:find ?name :where [?p :person/name ?name]]`)
```

## Common Debugging Patterns

### 1. Explore an unknown database

Start by discovering the schema shape:

```bash
# What attributes exist?
~/go/bin/datalog -db <path> -query '[:find ?a :where [_ ?a _]]'

# How many entities per attribute?
~/go/bin/datalog -db <path> -query '[:find ?a (count ?e) :where [?e ?a _]]'
```

Then sample values for attributes that look interesting:

```bash
~/go/bin/datalog -db <path> -query '[:find ?v :where [_ :person/status ?v]]'
```

### 2. Inspect an entity through a known value

Don't query for entity IDs and copy them. Instead, join through a known value to see everything about an entity:

```bash
# All attributes and values for the entity named "Alice"
~/go/bin/datalog -db <path> -query \
  '[:find ?a ?v :where [?e :person/name "Alice"] [?e ?a ?v]]'
```

This works for any attribute — find the entity through something you know, then fan out:

```bash
# Everything about the order with ID "ORD-1234"
~/go/bin/datalog -db <path> -query \
  '[:find ?a ?v :where [?e :order/id "ORD-1234"] [?e ?a ?v]]'
```

### 3. Follow references

Entities reference other entities. Use variable joins to traverse:

```bash
# Who are Alice's friends, and what cities do they live in?
~/go/bin/datalog -db <path> -query \
  '[:find ?friend-name ?city
    :where [?p :person/name "Alice"]
           [?p :person/friend ?f]
           [?f :person/name ?friend-name]
           [?f :person/city ?city]]'
```

Chain as many hops as needed — each shared variable (`?f` above) is a join.

### 4. Find missing or unexpected data

```bash
# People without an email
~/go/bin/datalog -db <path> -query \
  '[:find ?name
    :where [?p :person/name ?name]
           (not [?p :person/email _])]'

# People in neither New York nor Boston
~/go/bin/datalog -db <path> -query \
  '[:find ?name ?city
    :where [?p :person/name ?name]
           [?p :person/city ?city]
           [(!= ?city "New York")]
           [(!= ?city "Boston")]]'
```

### 5. Aggregate to find outliers

```bash
# Which cities have the most people?
~/go/bin/datalog -db <path> -query \
  '[:find ?city (count ?p)
    :where [?p :person/city ?city]]'

# Average age by city
~/go/bin/datalog -db <path> -query \
  '[:find ?city (avg ?age)
    :where [?p :person/city ?city]
           [?p :person/age ?age]]'
```

### 6. For small databases, just export

```bash
~/go/bin/datalog -db <path> -export dump.edn
```

Export format is one datom per line: `[#identity "hash" :attribute value txid]`

## Output Format

Results are printed as markdown tables:

```
| ?name   | ?age |
|---------|------|
| Alice   |   30 |
| Bob     |   25 |
| Charlie |   35 |

_3 rows (1.234ms)_
```

With `-verbose`, stderr gets annotation events showing index selection, join stats, and timing.

## Entity Identity

Entities are identified by an **Identity** — a SHA1 hash of a seed string, displayed as a 25-character L85-encoded string (a sort-order-preserving Base85 encoding).

When you query for an entity variable like `?e`, the result column shows the L85 hash. You don't normally need to use these directly — join through known attribute values instead (see debugging patterns above).

The `#identity "L85hash"` tagged literal exists for cases where you have a hash from logs or export output and need to look it up directly. This is the exception, not the normal workflow.

## Notes

- All attribute names are keywords starting with `:` (e.g., `:person/name`)
- Integer literals are int64, float literals are float64
- String literals use double quotes: `"value"`
- The `_` wildcard matches any value without binding
- Empty results return a table with headers but no rows
- Tagged literals: `#identity "L85..."`, `#inst "RFC3339..."`, `#bytes "L85..."`