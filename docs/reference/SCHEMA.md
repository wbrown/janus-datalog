# Schema Support

This document describes janus-datalog's schema support for type validation, cardinality, and uniqueness constraints.

## Overview

Schema support is **optional and additive**:
- Without schema: all existing behavior is preserved
- With schema: type validation, cardinality-many, and uniqueness constraints are enforced
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

### `:db/cardinality`

| Cardinality | Description | Constant |
|-------------|-------------|----------|
| `:db.cardinality/one` | Single value (default) | `schema.CardinalityOne` |
| `:db.cardinality/many` | Multiple values (set) | `schema.CardinalityMany` |
| `:db.cardinality/vector` | Ordered list (RGA) | `schema.CardinalityVector` |

### `:db/uniqueElements`

For vector attributes, enforce unique elements (no duplicates):

| Value | Description |
|-------|-------------|
| `false` | Duplicates allowed (default) |
| `true` | Duplicates rejected at write time |

```go
// Vector with unique elements (OrderedSet)
builder.Attribute(":character/prefs").Type(schema.TypeString).OrderedSet().Add()

// Equivalent to:
builder.Attribute(":character/prefs").Type(schema.TypeString).Vector().UniqueElements(true).Add()
```

### `:db/unique`

| Unique | Description | Constant |
|--------|-------------|----------|
| (none) | No uniqueness constraint | `""` |
| `:db.unique/value` | Value must be unique across entities | `schema.UniqueValue` |
| `:db.unique/identity` | Value uniqueness (upsert semantics planned) | `schema.UniqueIdentity` |

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

### Uniqueness Validation

Uniqueness validation occurs at `Transaction.Commit()` time:

```go
// First transaction succeeds
tx1 := d.NewTransaction()
tx1.Add(alice, kw(":person/email"), "alice@example.com")
tx1.Commit()

// Second transaction fails - email already exists
tx2 := d.NewTransaction()
tx2.Add(bob, kw(":person/email"), "alice@example.com")
_, err := tx2.Commit()
// err: "uniqueness violation for :person/email: value alice@example.com already exists on entity ..."
```

Uniqueness is also checked within a single transaction:

```go
tx := d.NewTransaction()
tx.Add(alice, kw(":person/email"), "shared@example.com")
tx.Add(bob, kw(":person/email"), "shared@example.com")
_, err := tx.Commit()
// err: "uniqueness violation for :person/email: value shared@example.com already used by entity ..."
```

### Idempotent Updates

Asserting the same value for the same entity is allowed:

```go
tx1 := d.NewTransaction()
tx1.Add(alice, kw(":person/email"), "alice@example.com")
tx1.Commit()

// OK - same entity, same value
tx2 := d.NewTransaction()
tx2.Add(alice, kw(":person/email"), "alice@example.com")
tx2.Commit() // succeeds
```

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
| Uniqueness checking | ~6% | `Commit()` time |
| Schema resolution | 225ns | Once per Pull pattern |

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

// Types
schema.ValueType      // string, long, double, boolean, instant, bytes, ref, keyword
schema.Cardinality    // one, many
schema.Unique         // value, identity

// Functions
schema.NewBuilder()                    // Start building a schema
schema.NewSchema()                     // Create empty schema
schema.ParseSchema(input string)       // Parse EDN schema string
schema.ParseSchemaFile(path string)    // Parse EDN schema file
schema.ResolvePullPattern(p, s)        // Resolve pull pattern with schema
schema.ValidateValue(v, t)             // Validate value against type
schema.ValidateDatom(s, attr, v)       // Validate datom against schema
```
