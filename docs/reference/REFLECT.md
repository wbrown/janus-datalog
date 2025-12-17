# Struct Reflection API

This document describes janus-datalog's struct reflection API for mapping Go structs to/from datoms.

## Overview

The reflection API provides a more ergonomic way to work with the database:

- **`SchemaFromStruct()`** - Generate schema from struct definitions
- **`SaveStruct()`** - Write/update structs with upsert semantics
- **`PullInto()`** / **`PullIntoMany()`** - Read datoms into structs

## Struct Tags

Use the `datalog` tag to control field mapping:

```go
type Person struct {
    ID      datalog.Identity `datalog:"-,id"`      // Entity identity field
    Name    string           `datalog:"name"`      // → :person/name
    Age     int64            `datalog:"age"`       // → :person/age
    Bio     *string          `datalog:"bio"`       // Optional field (pointer)
    Tags    []string         `datalog:"tags"`      // Cardinality-many
    Manager *Person          `datalog:"manager"`   // Ref to another entity
    Friends []*Person        `datalog:"friends"`   // Many refs
    Skip    string           `datalog:"-"`         // Explicit skip
    private string           // Unexported = ignored
}
```

### Tag Format

| Tag | Meaning |
|-----|---------|
| `datalog:"name"` | Attribute local name (namespace from struct) |
| `datalog:"namespace/name"` | Full attribute name |
| `datalog:"-"` | Skip this field |
| `datalog:"-,id"` | This field holds the entity Identity |

### Namespace Derivation

The namespace is derived from the struct name using kebab-case:

| Struct Type | Namespace |
|-------------|-----------|
| `Person` | `person` |
| `PersonInfo` | `person-info` |
| `HTTPServer` | `http-server` |

### Cardinality Inference

Cardinality is determined by schema (authoritative) or Go type (fallback):

| Go Type | Inferred Cardinality |
|---------|---------------------|
| `string`, `int64`, etc. | one |
| `*T` (pointer) | one (optional) |
| `[]T` (slice) | many |
| `[]*T` (slice of pointers) | many |

## Generating Schema

### From Single Struct

```go
import "github.com/wbrown/janus-datalog/datalog/reflect"

schema, err := reflect.SchemaFromStruct(Person{})
```

### From Multiple Structs

```go
schema, err := reflect.SchemaFromStructs(Person{}, Company{}, Order{})
```

### Using Generated Schema

```go
db, err := storage.NewDatabaseWithSchema(path, schema)
```

## Writing Structs

`SaveStruct` provides unified upsert semantics - it creates new entities or updates existing ones.

### With Auto-Generated ID

```go
person := &Person{
    Name:  "Alice",
    Age:   30,
    Tags:  []string{"developer", "team-lead"},
}

tx := db.NewTransaction()
id, err := tx.SaveStruct(person)  // ID auto-generated
tx.Commit()

// person.ID is now set to the generated identity
fmt.Println(person.ID)  // e1234567890-a1b2c3d4...
```

### With Explicit ID

```go
person := &Person{
    ID:   datalog.NewIdentity("alice-unique-id"),
    Name: "Alice",
    Age:  30,
}

tx := db.NewTransaction()
id, err := tx.SaveStruct(person)  // Uses provided ID
tx.Commit()
```

## Updating Structs

`SaveStruct` provides upsert semantics - calling it on an entity that already exists will update it.

### Cardinality-One Fields

For single-value fields, `SaveStruct` automatically:
1. Looks up the existing value
2. If different, retracts the old value
3. Adds the new value

```go
// Original: Alice, age 30
alice.Name = "Alice Smith"
alice.Age = 31

tx := db.NewTransaction()
tx.SaveStruct(&alice)
tx.Commit()
// Now: Alice Smith, age 31 (old values retracted)
```

### Cardinality-Many Fields

For slice fields, `SaveStruct` uses diff-based updates:
- Retracts values in existing but not in new
- Adds values in new but not in existing
- Values present in both are unchanged

```go
// Original tags: ["developer", "golang"]
alice.Tags = []string{"developer", "rust", "architect"}

tx := db.NewTransaction()
tx.SaveStruct(&alice)
tx.Commit()
// Result: ["developer", "rust", "architect"]
// "golang" retracted, "rust" and "architect" added, "developer" unchanged
```

### Nil vs Empty Slice Semantics

For cardinality-many fields, `nil` and empty slices have different meanings:

| Value | Behavior |
|-------|----------|
| `nil` | Skip field (leave existing values unchanged) |
| `[]T{}` (empty) | Clear all existing values |
| `[]T{"a", "b"}` | Replace with diff-based update |

```go
// Load entity - alice has tags ["go", "rust"]
var alice Person
db.PullInto(aliceID, &alice)

// Partial update - only change name, leave tags alone
update := Person{ID: aliceID, Name: "Alice Smith", Tags: nil}
tx := db.NewTransaction()
tx.SaveStruct(&update)
tx.Commit()
// tags still ["go", "rust"]

// Clear all tags
clear := Person{ID: aliceID, Name: "Alice Smith", Tags: []string{}}
tx2 := db.NewTransaction()
tx2.SaveStruct(&clear)
tx2.Commit()
// tags now empty
```

### Complete Update Example

```go
// Load existing entity
var person Person
db.PullInto(personID, &person)

// Modify fields
person.Name = "Alice Smith"
person.Age = 31
person.Tags = []string{"senior-developer", "team-lead"}

// Save with upsert semantics
tx := db.NewTransaction()
_, err := tx.SaveStruct(&person)
if err != nil {
    return err
}
tx.Commit()
```

## Reading Structs

### Single Entity

```go
var person Person
err := db.PullInto(entityID, &person)

fmt.Println(person.Name)  // "Alice"
fmt.Println(person.Tags)  // ["developer", "team-lead"]
```

### Multiple Entities

```go
var people []Person
err := db.PullIntoMany(entityIDs, &people)

for _, p := range people {
    fmt.Println(p.Name)
}
```

### With Pointer Slice

```go
var people []*Person
err := db.PullIntoMany(entityIDs, &people)
```

## Reference Handling

### Writing References

References to other entities use the `datalog.Identity` type:

```go
// Create manager
manager := &Person{Name: "Carol", Age: 35}
tx := db.NewTransaction()
managerID, _ := tx.SaveStruct(manager)

// Create employee with manager reference
employee := &Person{
    Name:    "Alice",
    Manager: &Person{ID: managerID},  // Just need the ID set
}
tx.SaveStruct(employee)
tx.Commit()
```

Or add references separately:

```go
tx.Add(employeeID, datalog.NewKeyword(":person/manager"), managerID)
```

### Reading References

When reading, reference fields contain only the ID (not full nested data) unless nested patterns are generated:

```go
var person Person
db.PullInto(aliceID, &person)

// person.Manager has only the ID field populated
if person.Manager != nil {
    // Load manager's data separately
    var manager Person
    db.PullInto(person.Manager.ID, &manager)
}
```

## Type Mapping

### Go Types to Schema Types

| Go Type | Schema ValueType |
|---------|-----------------|
| `string` | `db.type/string` |
| `int64`, `int`, `int32`, etc. | `db.type/long` |
| `float64`, `float32` | `db.type/double` |
| `bool` | `db.type/boolean` |
| `time.Time` | `db.type/instant` |
| `[]byte` | `db.type/bytes` |
| `datalog.Identity` | `db.type/ref` |
| `datalog.Keyword` | `db.type/keyword` |
| `*OtherStruct` | `db.type/ref` |

## Pattern Generation

The API automatically generates pull patterns from struct definitions:

```go
// Generate pattern for debugging
pattern := reflect.GeneratePullPattern(Person{}, schema)
// Returns: "[:person/name :person/age :person/email :person/tags ...]"

// Simple (flat) pattern without nested refs
simple := reflect.GenerateSimplePullPattern(Person{})
```

## Complete Example

```go
package main

import (
    "fmt"
    "os"

    "github.com/wbrown/janus-datalog/datalog"
    "github.com/wbrown/janus-datalog/datalog/reflect"
    "github.com/wbrown/janus-datalog/datalog/storage"
)

type Person struct {
    ID      datalog.Identity `datalog:"-,id"`
    Name    string           `datalog:"name"`
    Age     int64            `datalog:"age"`
    Tags    []string         `datalog:"tags"`
}

func main() {
    tmpDir, _ := os.MkdirTemp("", "example")
    defer os.RemoveAll(tmpDir)

    // Generate schema from struct
    schema, _ := reflect.SchemaFromStruct(Person{})
    db, _ := storage.NewDatabaseWithSchema(tmpDir, schema)
    defer db.Close()

    // Create
    alice := &Person{
        Name: "Alice",
        Age:  30,
        Tags: []string{"developer", "mentor"},
    }
    tx := db.NewTransaction()
    aliceID, _ := tx.SaveStruct(alice)
    tx.Commit()

    fmt.Printf("Created: %s\n", alice.ID.String()[:20])

    // Read
    var loaded Person
    db.PullInto(aliceID, &loaded)

    fmt.Printf("Name: %s\n", loaded.Name)
    fmt.Printf("Age: %d\n", loaded.Age)
    fmt.Printf("Tags: %v\n", loaded.Tags)

    // Update
    loaded.Age = 31
    loaded.Tags = []string{"developer", "mentor", "architect"}
    tx2 := db.NewTransaction()
    tx2.SaveStruct(&loaded)
    tx2.Commit()

    // Verify update
    var updated Person
    db.PullInto(aliceID, &updated)
    fmt.Printf("Updated Age: %d\n", updated.Age)
    fmt.Printf("Updated Tags: %v\n", updated.Tags)
}
```

## Performance

- **StructInfo caching**: Struct metadata is cached using `sync.Map` for O(1) lookups after first parse
- **Reflection overhead**: ~100ns per field access (Go reflection cost)
- **Schema resolution**: Uses resolved pull patterns for proper cardinality-many handling
- **Bulk operations**: `SaveStruct` calls `Add()` per field (same overhead as manual datom creation)

## Limitations

1. **Self-referential structs**: Pull patterns for self-referential types (e.g., `Friends []*Person`) generate flat patterns to prevent infinite recursion
2. **Circular references on write**: Caller is responsible for ensuring entities exist before referencing them
3. **Unexported fields**: Always ignored (Go reflection limitation)
4. **Interface fields**: Not supported - use concrete types

## Package Reference

```go
import "github.com/wbrown/janus-datalog/datalog/reflect"

// Schema generation
reflect.SchemaFromStruct(v interface{}) (*schema.Schema, error)
reflect.SchemaFromStructs(vs ...interface{}) (*schema.Schema, error)
reflect.MustSchemaFromStruct(v interface{}) *schema.Schema

// Pattern generation
reflect.GeneratePullPattern(v interface{}, s schema.SchemaProvider) string
reflect.GenerateSimplePullPattern(v interface{}) string

// Writing/Updating (usually use Transaction methods instead)
reflect.SaveStruct(tx TransactionUpdater, lookup EntityLookup, v interface{}, s SchemaProvider) (Identity, error)

// Reading (usually use Database methods instead)
reflect.ReadStruct(result map[string]interface{}, v interface{}, s SchemaProvider) error
```

### Transaction Methods

```go
// Save struct with upsert semantics (creates or updates)
// If ID field is empty, generates a new ID; otherwise uses existing ID
tx.SaveStruct(v interface{}) (datalog.Identity, error)
```

### Database Methods

```go
// Read entity into struct
db.PullInto(entityID datalog.Identity, v interface{}) error

// Read multiple entities into slice
db.PullIntoMany(entityIDs []datalog.Identity, v interface{}) error
```
