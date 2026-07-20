# Bug: CollectionInput Not Unpacked Into Multiple Tuples

**Status**: Fixed **Discovered**: 2026-02-05 **Fixed**: 2026-02-05

## Symptoms

Two related issues with collection inputs:

### 1. Two Collection Inputs Don't Cross-Product

Query with two separate collection inputs `[?e ...] [?a ...]` where both are pattern variables returns incomplete results:

```clojure
[:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]
```

With 2 entities and 2 attributes, expected 4 results but only got 2 (only `:person/age` datoms).

### 2. Empty Collection Returns Results

```go
db.ExecuteQueryWithInputs(query, []datalog.Identity{})  // empty collection
```

Expected: 0 results (nothing matches empty set) Actual: Returns all matching data (collection constraint ignored)

## Root Causes

The bug had three contributing factors in different layers:

### 1. Subquery Path: Slice Not Unpacked (executor/subquery.go)

The `CollectionInput` handling in `createInputRelationsFromValuesWithOptions` wrapped the entire input slice in a single tuple:

```go
case query.CollectionInput:
    rel := NewMaterializedRelationWithOptions(
        []query.Symbol{inp.Symbol},
        []Tuple{{orderedValues[valueIndex]}},  // BUG: entire slice = one tuple
        opts,
    )
```

**Fix**: Use reflection to detect and unpack slices into individual tuples.

### 2. Empty Collection Skipped (executor/executor_utils.go)

In `BindQueryInputs`, empty collections were skipped entirely:

```go
case query.CollectionInput:
    if relationIndex < len(inputRelations) {
        rel := inputRelations[relationIndex]
        if rel.Size() > 0 {  // BUG: empty collections skipped
            // ...add to boundRelations
        }
    }
```

This meant empty collection constraints were lost - the join would proceed without the empty relation, returning all data instead of none.

**Fix**: Always add collection relations to `boundRelations`, even when empty. Joining with an empty relation produces 0 results.

### 3. Hash Set Overwriting Tuples (storage/hash_join_matcher.go)

The hash join matcher's `buildHashSet` overwrote entries with the same key:

```go
hashSet[key] = tuple  // BUG: overwrites previous tuples with same key
```

For bindings like:
- (person-1, :person/name)
- (person-1, :person/age)
- (person-2, :person/name)
- (person-2, :person/age)

When building hash set keyed by E (position 0):
- `hashSet["person-1"] = (person-1, :person/age)` ← overwrites :person/name
- `hashSet["person-2"] = (person-2, :person/age)` ← overwrites :person/name

Only `:person/age` tuples survive! The hash join then only matches `:person/age` datoms.

**Fix**: Change hash set to store ALL tuples per key: `map[string][]executor.Tuple`. Update iterator to check against all tuples in the list.

## Test Coverage

All tests now pass:

- `TestCollectionInput_SingleCollection` - single `[?e ...]` works
- `TestCollectionInput_SingleElement` - collection with 1 element works
- `TestCollectionInput_EmptyCollection` - empty collection returns 0 results ✓
- `TestCollectionInput_TwoCollections` - two variable collections produce cross-product ✓
- `TestCollectionInput_ThreeCollections` - works
- `TestCollectionInput_ScalarPlusCollection` - `?a [?e ...]` works
- `TestCollectionInput_CollectionPlusScalar` - `[?e ...] ?a` works
- `TestCollectionInput_KeywordCollection` - keyword values work
- `TestCollectionInput_IntCollection` - int values work
- `TestCollectionInput_StringCollection` - string values work
- `TestBindQueryInputs_TwoCollectionsCrossProduct` - verifies cross-product logic
- `TestBindQueryInputs_EmptyCollectionReturnsEmpty` - verifies empty handling

## Files Changed

1. `executor/subquery.go` - Added reflection to unpack slices in CollectionInput
2. `executor/executor_utils.go` - Always add collection relations to boundRelations
3. `storage/hash_join_matcher.go` - Changed hash set from `map[string]Tuple` to `map[string][]Tuple`
4. `executor/bind_query_inputs_test.go` - New tests for BindQueryInputs
5. `storage/collection_input_test.go` - Comprehensive collection input tests

## Notes

This bug was discovered while implementing AETV index. The investigation revealed it was a pre-existing executor bug exposed by new test coverage. The fix required changes across three files in two packages.
