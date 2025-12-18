# Proposal: Schema-Based Selectivity Hints for Query Planner

**Status**: Not Implemented (documented for future consideration)
**Date**: December 2025
**Related**: Fix for multi-position binding performance (PR #20)

## Problem

The query planner gives patterns identical selectivity scores when it lacks cardinality information. For example, in a query like:

```clojure
[:find ?e ?name
 :where
 [?e :entity/code "POI E"]    ; highly selective (few matches)
 [?e :entity/name ?name]]     ; not selective (all entities have names)
```

Both patterns get similar scores because the planner doesn't know that `:entity/code` values are sparse while `:entity/name` is present on every entity.

## Proposed Solution

Add schema-based selectivity hints to `PlannerOptions`:

```go
type PlannerOptions struct {
    // ... existing fields ...

    // Schema-based selectivity hints
    // Maps attribute names to estimated selectivity (0.0-1.0, lower = more selective)
    // Unique attributes should have ~0.01, non-unique ~0.5-1.0
    AttributeSelectivity map[string]float64
}
```

### Selectivity Values

- `0.01` - Unique attributes (`:db/ident`, `:user/email`)
- `0.1-0.3` - Sparse attributes (`:entity/code`, `:product/sku`)
- `0.5` - Reference attributes (many entities per ref)
- `0.8-1.0` - Common attributes (`:entity/name`, `:created-at`)

### Integration Points

1. **`planner/options.go`**: Add `AttributeSelectivity` field
2. **`planner/planner_patterns.go`**: Use hints in `scorePattern()`
3. **`storage/database.go`**: Extract hints from schema definitions

```go
func (d *Database) SchemaSelectivityHints() map[string]float64 {
    hints := make(map[string]float64)
    if d.schema == nil {
        return hints
    }

    for attr, def := range d.schema.Attributes() {
        if def.Unique {
            hints[attr] = 0.01  // Unique = very selective
        } else if def.Type == schema.TypeRef {
            hints[attr] = 0.5   // Refs typically have many entities per ref
        } else {
            hints[attr] = 0.3   // Default non-unique
        }
    }
    return hints
}
```

## Why Not Implemented

The multi-position binding performance fix (Part 1) solved the immediate 40x regression by improving the storage layer's strategy selection. The matcher now analyzes binding cardinalities at runtime to choose optimal index access patterns.

Schema-based selectivity would improve **pattern ordering** in the planner, but:

1. The runtime cardinality analysis in `chooseBestMultiPositionStrategy` handles the critical case
2. Schema is optional - many databases don't define one
3. Static selectivity hints are approximations; runtime analysis is more accurate
4. Adds complexity for marginal benefit given the storage-layer fix

## When This Would Help

Consider implementing if:

- Queries with 3+ patterns show suboptimal ordering
- Users report slow queries where pattern order matters
- Statistics collection becomes available (then use actual cardinalities)

## Alternative: Runtime Statistics

A more robust solution would be runtime statistics collection:

```go
type AttributeStats struct {
    TotalCount     int64   // Total datoms with this attribute
    DistinctValues int64   // Unique values
    DistinctEntities int64 // Unique entities
}
```

This would provide accurate selectivity without schema dependency, but requires background statistics gathering.
