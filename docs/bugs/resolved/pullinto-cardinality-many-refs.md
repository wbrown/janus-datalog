# PullInto Requires Schema for Cardinality-Many Attributes

**Status: NOT A BUG - Expected Behavior**

## Description

When using `PullInto` with cardinality-many attributes (like `[]datalog.Identity` slices), the database MUST be created with a schema that declares the attribute as `Many()`. Without the schema, PullInto defaults to cardinality-one and only returns the first value.

## Root Cause

`PullInto` calls `schema.ResolvePullPattern()` which looks up each attribute's cardinality:
- If schema is nil or doesn't define the attribute → defaults to cardinality-one
- With cardinality-one, only one value is returned even if multiple exist

## Solution

Always create the database with a schema when using cardinality-many attributes:

```go
// WRONG - no schema, PullInto won't know about cardinality-many
db, err := storage.NewDatabase(path)
// or
db, err := storage.NewDatabaseWithTimeTx(path)

// RIGHT - schema tells PullInto about cardinality
schema, _ := schema.NewBuilder().
    Attribute(":entity/features").Type(schema.TypeRef).Many().Add().
    Build()
db, err := storage.NewDatabaseWithSchema(path, schema)
// or
db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path:      path,
    UseTimeTx: true,
    Schema:    schema,
})
```

## Verification

```go
// Check if schema knows the attribute is many
isMany := db.Schema() != nil && db.Schema().IsMany(datalog.NewKeyword(":entity/features"))
// Must be true for PullInto to return all values
```

## Related

- `ResolvePullPattern` in `datalog/schema/resolve.go` - where cardinality is looked up
- `PullInto` in `datalog/storage/database.go` - uses resolved pattern for fetching
