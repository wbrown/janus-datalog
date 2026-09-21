# Schema Support

This document describes janus-datalog's schema support for type validation, cardinality, and uniqueness constraints.

## Overview

Schema support is **optional and additive**:
- Without schema: all existing behavior is preserved
- With schema: type validation, cardinality-many, uniqueness constraints, and `:db/neverZeroValue` are enforced
- Unknown attributes are allowed (additive schema model)

## Defining a Schema

### Option 1: EDN File (Datomic-compatible)

```clojure
{:person/name   {:db/valueType   :db.type/string
                 :db/cardinality :db.cardinality/one}
 :person/age    {:db/valueType   :db.type/long}
 :person/email  {:db/valueType   :db.type/string
                 :db/unique      :db.unique/value}
 :person/friends {:db/valueType   :db.type/ref
                  :db/cardinality :db.cardinality/many
                  :db/doc         "References to friend entities"}}
```

Load with:
```go
schema, err := schema.ParseSchemaFile("schema.edn")
```

Every key in an attribute definition must be one of those listed under [Supported Attributes](#supported-attributes), and must be a keyword. An unknown key is a parse error, not a skip, so a misspelled or renamed key cannot yield a definition that silently lacks it.

### Option 2: Go Builder API

```go
import "github.com/wbrown/janus-datalog/datalog/schema"

s, err := schema.NewBuilder().
    Attribute(":person/name").Type(schema.TypeString).Add().
    Attribute(":person/age").Type(schema.TypeLong).Add().
    Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
    Attribute(":person/friends").Type(schema.TypeRef).Many().Doc("References to friends").Add().
    Build()
```

## Using Schema with Database

```go
import "github.com/wbrown/janus-datalog/datalog/db"

// Create database with schema
d, err := db.Open("/path/to/db", db.WithSchema(s))

// Or add schema to existing database
d, _ := db.Open("/path/to/db")
d.SetSchema(s)
```

## Supported Attributes

### `:db/valueType`

| Type | Go Type | Constant |
|------|---------|----------|
| `:db.type/string` | `string` | `schema.TypeString` |
| `:db.type/long` | `int64`, `int`, `int32`, `int16`, `int8` | `schema.TypeLong` |
| `:db.type/double` | `float64`, `float32` | `schema.TypeDouble` |
| `:db.type/boolean` | `bool` | `schema.TypeBoolean` |
| `:db.type/instant` | `time.Time` | `schema.TypeInstant` |
| `:db.type/bytes` | `[]byte` | `schema.TypeBytes` |
| `:db.type/ref` | `datalog.Identity` | `schema.TypeRef` |
| `:db.type/keyword` | `datalog.Keyword` | `schema.TypeKeyword` |
| `:db.type/symbol` | `datalog.Symbol` | `schema.TypeSymbol` |
| `:db.type/tx` | `datalog.ElementID`, `*datalog.ElementID` | `schema.TypeTx` |

### `:db/cardinality`

| Cardinality | Description | Constant |
|-------------|-------------|----------|
| `:db.cardinality/one` | Single value (default) | `schema.CardinalityOne` |
| `:db.cardinality/many` | Multiple values (set) | `schema.CardinalityMany` |
| `:db.cardinality/vector` | Ordered list (RGA) | `schema.CardinalityVector` |

### `:db/uniqueElements`

A janus extension. For vector attributes, enforce unique elements (no duplicates):

| Value | Description |
|-------|-------------|
| `false` | Duplicates allowed (default) |
| `true` | Duplicates rejected at write time |

```clojure
{:character/prefs {:db/valueType      :db.type/string
                   :db/cardinality    :db.cardinality/vector
                   :db/uniqueElements true}}
```

```go
// Vector with unique elements (OrderedSet)
builder.Attribute(":character/prefs").Type(schema.TypeString).OrderedSet().Add()

// Equivalent to:
builder.Attribute(":character/prefs").Type(schema.TypeString).Vector().UniqueElements(true).Add()
```

### `:db/neverZeroValue`

A janus extension. Declares that the zero value of the attribute's value type is not a value of the attribute: an entity carries a value or it carries no datom, and there is no fact "the bio is the empty string".

| Value type | Zero value |
|---|---|
| `:db.type/string` | `""` |
| `:db.type/long` | `0` |
| `:db.type/double` | `0.0` |
| `:db.type/boolean` | `false` |
| `:db.type/instant` | the zero `time.Time` |
| `:db.type/bytes` | a nil or zero-length `[]byte` |
| `:db.type/ref`, `:db.type/keyword`, `:db.type/symbol` | `nil` |
| `:db.type/tx` | the zero `ElementID` |

`:db/valueType` is required alongside it. A definition carrying the flag without a type is rejected by `ParseSchema`, by the builder, and by `Schema.Add`. On a many or vector attribute the property governs each element, never the collection. The store does not normalize: a string of whitespace is a value.

```clojure
{:person/bio {:db/valueType      :db.type/string
              :db/neverZeroValue true}}
```

```go
builder.Attribute(":person/bio").Type(schema.TypeString).NeverZeroValue().Add()
```

Write behavior is under [Zero-Value Rejection](#zero-value-rejection) below.

### `:db/unique`

| Unique | Description | Constant |
|--------|-------------|----------|
| (none) | No uniqueness constraint | none — omit `:db/unique`; the field is nil |
| `:db.unique/value` | Value is canonical for one entity | `schema.UniqueValue` |
| `:db.unique/identity` | `UniqueValue` + eligible for `LookupByUnique` lookup-refs | `schema.UniqueIdentity` |

Uniqueness is enforced **at read time**, not at write time. See
[Uniqueness Semantics](#uniqueness-semantics) below and
[CRDT_UNIQUE_SEMANTICS.md](./CRDT_UNIQUE_SEMANTICS.md) for the full
design discussion.

> **Note on `UniqueIdentity`**: today, `LookupByUnique` accepts either
> `UniqueValue` or `UniqueIdentity` — the Go API does not discriminate.
> The distinction will matter for the future query-language lookup-ref
> syntax (`[:user/email "x@y"]` used as an entity reference in query
> patterns), which will require `UniqueIdentity`. Datomic-style
> write-time upsert / entity merging is explicitly **not** performed
> for either — see [CRDT_UNIQUE_SEMANTICS.md](./CRDT_UNIQUE_SEMANTICS.md)
> D1 for the full rationale (split-entity convergence in
> concurrent-write CRDTs is deferred to a future design round).

### `:db/doc`

Documentation string for the attribute.

## Validation Behavior

### Type Validation

Type validation occurs at `Transaction.Add()` time:

```go
tx := d.NewTransaction()

// OK - string value for string attribute
tx.Add(alice, kw(":person/name"), "Alice")

// ERROR - int value for string attribute
err := tx.Add(alice, kw(":person/name"), 123)
// err: "schema validation failed for :person/name: expected db.type/string (string), got int"
```

### Zero-Value Rejection

For an attribute declared `:db/neverZeroValue`, the zero value lies outside the attribute's domain, and `ValidateDatom` rejects it right after the type check, in the same category as a value of the wrong type:

```go
// ERROR - "" on a :db/neverZeroValue string attribute
err := tx.Add(alice, kw(":person/bio"), "")
// err: "schema validation failed for :person/bio: the :db.type/string zero value is not a value of a NeverZeroValue attribute"

// OK - whitespace is a value
tx.Add(alice, kw(":person/bio"), " ")
```

- `Add` and `Set` return the error at the call and buffer nothing. `AddEntity` and `AddMap` add through `Add` and reject through the same check.
- On a many or vector attribute, a collection containing a zero element is rejected, not filtered.
- `SaveStruct` writes no datom for a field holding the zero value and leaves the stored value unchanged, the rule it already applies to a nil pointer field. A plain `string` or `int64` field tested against its zero is then an exact presence test.
- `Remove` is unconstrained, so a zero-valued datom that reached the store before the declaration can be tombstoned.
- `Import` and the other replay paths apply recorded datoms and bypass the check.
- Reads are unchanged. `PullInto` yields the zero value for an absent attribute, and under the property that is the only thing the zero can mean.

### Uniqueness Semantics

Uniqueness is a **read-time CRDT resolution rule**, not a write-time
validation gate. All writes to a unique attribute succeed; the
canonical owner of each unique value is determined when the database
is read, using walk-based `(A, V)`-LWW resolution.

> Rationale and full design discussion: see
> [CRDT_UNIQUE_SEMANTICS.md](./CRDT_UNIQUE_SEMANTICS.md). This is a
> deliberate departure from Datomic's write-time enforcement, motivated
> by this codebase's CRDT architecture.

#### Writes always succeed

```go
tx1 := d.NewTransaction()
tx1.Set(alice, kw(":person/email"), "alice@example.com")
tx1.Commit() // succeeds

tx2 := d.NewTransaction()
tx2.Set(bob, kw(":person/email"), "alice@example.com")
_, err := tx2.Commit() // succeeds — no uniqueness violation at write time
```

After this sequence, the database contains both assertions. The walk
rule (described below) determines which entity is the canonical owner
and which falls back.

#### Read-time resolution: `(A, V)`-LWW with walk fallback

For a unique attribute, each entity's current value is determined by
walking its `(E, A)` history in descending `Tx` order. For each entry
`(V_i, T_i)`:

1. If it is a `Remove(V_i)`, record `V_i` as retracted at `T_i`.
2. If it is a `Set(V_i)` and `V_i` was retracted at a higher `Tx`, skip.
3. If it is a `Set(V_i)` and **another entity** has asserted `V_i` with
   `Tx > T_i`, skip (superseded).
4. Otherwise, emit `V_i` as the entity's current value.

If no entry passes the checks, the entity has no current value for
this attribute.

**Value-view** (e.g., `LookupByUnique(attr, v)` or a query like
`[?e :user/email "x@y"]`): returns the entity whose walk emits `v`
(always exactly one or none). Symmetric with the entity-view by
construction.

#### Worked example

```go
// Alice first claims email=v1, then email=v2.
tx1 := d.NewTransaction()
tx1.Set(alice, email, "v1@example.com")
tx1.Commit() // Tx = T1

tx2 := d.NewTransaction()
tx2.Set(alice, email, "v2@example.com")
tx2.Commit() // Tx = T2

// Bob then takes v2 with a higher Tx.
tx3 := d.NewTransaction()
tx3.Set(bob, email, "v2@example.com")
tx3.Commit() // Tx = T3 > T2

// Alice's walk: T2:v2 is superseded by bob's T3:v2 → skip.
//               T1:v1 is not superseded by anyone → emit.
//   Alice's current email = "v1@example.com"
//
// Bob's walk:   T3:v2 is not superseded → emit.
//   Bob's current email = "v2@example.com"
//
// LookupByUnique(email, "v1@example.com") → alice
// LookupByUnique(email, "v2@example.com") → bob
```

#### History preservation

`d.History()` returns every raw assertion, including superseded ones —
the walk rule applies only to current-state reads. Time-travel via
`d.AsOf(tx)` applies the walk restricted to `Tx ≤ target`.

#### Idempotent updates

Asserting the same value for the same entity is a no-op under the walk:
the entity's current value is already that V and remains so.

```go
tx1 := d.NewTransaction()
tx1.Set(alice, email, "alice@example.com")
tx1.Commit()

tx2 := d.NewTransaction()
tx2.Set(alice, email, "alice@example.com") // second assertion at higher Tx
tx2.Commit() // succeeds; alice still owns "alice@example.com"
```

#### Application-level upsert with `LookupByUnique`

Upsert-by-natural-key is built on `LookupByUnique`:

```go
// "Find the user with this email, creating one if needed."
e, err := d.LookupByUnique(email, "alice@example.com")
if err != nil {
    return err
}
if e == nil {
    e = datalog.NewIdentity("user:" + uuid.New().String())
}
tx := d.NewTransaction()
tx.Set(e, email, "alice@example.com")
tx.Set(e, name,  "Alice")
_, err = tx.Commit()
```

`LookupByUnique` requires the attribute to be declared unique in the
schema (either `UniqueValue` or `UniqueIdentity`). Applications that
need to detect "did my write win?" after a concurrent commit should
call `LookupByUnique` after the commit and compare the returned
Identity to their own.

#### Migration from pre-redesign behavior

Applications that previously relied on `Commit()` returning a
uniqueness-violation error must now check ownership explicitly via
`LookupByUnique`. The old error string (`"uniqueness violation..."`)
no longer occurs.

## Pull API with Schema

Schema enables proper cardinality-many handling in the Pull API.

### Without Schema (default behavior)

```go
// Returns single value even if multiple exist
result, _ := puller.Pull(alice, pattern)
// result["person/tags"] = "developer"  // Only first value
```

### With Schema

```go
// Resolve pattern with schema
resolved := schema.ResolvePullPattern(pattern, d.Schema())

// Execute with resolved pattern
result, _ := puller.PullResolved(alice, resolved)
// result["person/tags"] = ["developer", "team-lead", "mentor"]  // All values
```

### Complete Example

```go
import "github.com/wbrown/janus-datalog/datalog/db"

// Schema with cardinality-many
s, _ := schema.NewBuilder().
    Attribute(":person/name").Type(schema.TypeString).Add().
    Attribute(":person/friends").Type(schema.TypeRef).Many().Add().
    Build()

d, _ := db.Open(tmpDir, db.WithSchema(s))

// Add data
tx := d.NewTransaction()
tx.Add(alice, kw(":person/name"), "Alice")
tx.Add(alice, kw(":person/friends"), bob)
tx.Add(alice, kw(":person/friends"), carol)
tx.Commit()

// Use the high-level Pull API directly
result, _ := d.Pull(alice, "[:person/name {:person/friends [:person/name]}]")

// Result:
// {
//   "person/name": "Alice",
//   "person/friends": [
//     {"person/name": "Bob"},
//     {"person/name": "Carol"}
//   ]
// }
```

## SchemaProvider Interface

For custom schema implementations:

```go
type SchemaProvider interface {
    GetAttribute(attr datalog.Keyword) *AttributeDefinition
    HasSchema() bool
    IsRef(attr datalog.Keyword) bool
    IsMany(attr datalog.Keyword) bool
}
```

## Performance

Schema validation impacts **writes only**. Reads are completely unaffected.

| Operation | Overhead | When |
|-----------|----------|------|
| Type validation | <0.2% | `Add()` time |
| Schema resolution | 225ns | Once per Pull pattern |

Uniqueness costs nothing at write time — it is a read-time resolution rule, so
it is paid by the read that asks: the AVET walk behind `LookupByUnique` and
behind unique-attribute resolution.

**Recommendation**: Enable schema validation freely. The write overhead is minimal and provides valuable type safety and data integrity.

See `PERFORMANCE_STATUS.md` for detailed benchmarks.

## Limitations

Current limitations compared to Datomic:

1. **No `:db/isComponent`** - component entity semantics not implemented
2. **No `:db/index`** - all attributes are indexed by default
3. **No `:db/fulltext`** - fulltext search not supported
4. **No `:db/noHistory`** - all datoms are retained
5. **No upsert semantics** - `:db.unique/identity` currently behaves like `:db.unique/value`
6. **Schema not stored as datoms** - schema is in-memory only

## Package Reference

```go
import "github.com/wbrown/janus-datalog/datalog/schema"

// Vocabulary. Each is a datalog.Keyword — there is no ValueType, Cardinality
// or Unique type. An AttributeDefinition's three keyword fields are all
// datalog.Keyword, and each draws from one of these closed sets; ParseSchema
// and the builder reject a keyword from the wrong one.
schema.TypeString  schema.TypeLong    schema.TypeDouble  schema.TypeBoolean
schema.TypeInstant schema.TypeBytes   schema.TypeRef     schema.TypeKeyword
schema.TypeSymbol  schema.TypeTx

schema.CardinalityOne  schema.CardinalityMany  schema.CardinalityVector
schema.CardinalityUnknown  // no schema definition; not declarable

schema.UniqueValue  schema.UniqueIdentity
// Absence of a uniqueness constraint is a nil Unique field, written by
// omitting :db/unique — there is no keyword for it and no constant naming it.

// Functions
schema.NewBuilder()                    // Start building a schema
schema.NewSchema()                     // Create empty schema
schema.ParseSchema(input string)       // Parse EDN schema string
schema.ParseSchemaFile(path string)    // Parse EDN schema file
schema.ResolvePullPattern(p, s)        // Resolve pull pattern with schema
schema.ValidateValue(v, t)             // Validate value against type
schema.IsZeroValue(v, t)               // Is v the zero value of value type t
schema.ValidateDatom(s, attr, v)       // Validate datom against schema
```
