# Identity Input Parameter Matching Bug

## Status: RESOLVED (False Positive)

## Resolution

**This was NOT a bug in the query engine.** The test incorrectly used Go's struct equality (`!=`) to compare Identity values, which compares ALL fields of the struct. Identity structs loaded from storage have different auxiliary fields (`str`, `l85`, `l85Computed`) than the original Identities, causing struct comparison to fail even when the hash (the actual identity) matches.

**The Fix:** Use `Identity.Equal()` method instead of struct equality `==`/`!=`:
```go
// WRONG - compares all struct fields
if foundID != expectedID { ... }

// CORRECT - compares only the hash
if !foundID.Equal(expectedID) { ... }
```

The query engine correctly filters by Identity values in reference attributes. The `ValuesEqual()` function and all internal comparisons use hash-based comparison.

## Original Summary (Incorrect)

When an `Identity` value is passed as an input parameter to a Datalog query, the constraint `[?e :attr ?inputIdentity]` does NOT correctly filter to entities where `:attr` equals the input Identity. Instead, it returns arbitrary entities that happen to have that attribute, regardless of the attribute's actual value.

## Original Impact Assessment (Incorrect)

**Critical** - This breaks ALL entity queries that filter by a reference attribute:
- `:entity/map` (finding entities belonging to a specific map)
- `:entity/region` (finding entities in a specific region)
- Any reference-type attribute used as a filter

## Original Reproduction

### Minimal Test Case

```go
func TestIdentityQueryMatching(t *testing.T) {
    db, cleanup := createTestDB(t)
    defer cleanup()

    // Define attributes
    refAttr := datalog.NewKeyword(":test/ref")
    codeAttr := datalog.NewKeyword(":test/code")

    // Create two different "parent" entities
    parent1 := datalog.NewIdentity(generateUUID())
    parent2 := datalog.NewIdentity(generateUUID())

    // Create child1 referencing parent1
    child1 := datalog.NewIdentity(generateUUID())
    db.Assert([]datalog.Datom{
        {E: child1, A: refAttr, V: parent1},
        {E: child1, A: codeAttr, V: "child1"},
    })

    // Create child2 referencing parent2
    child2 := datalog.NewIdentity(generateUUID())
    db.Assert([]datalog.Datom{
        {E: child2, A: refAttr, V: parent2},
        {E: child2, A: codeAttr, V: "child2"},
    })

    // Query: find children of parent1
    tuples, _ := db.store.ExecuteQueryWithInputs(
        `[:find ?c ?code :in $ ?parent :where [?c :test/ref ?parent] [?c :test/code ?code]]`,
        parent1,
    )

    // EXPECTED: 1 result - child1 with code "child1"
    // ACTUAL: 1 result - but it's the WRONG entity with a DIFFERENT ref value
}
```

### Observed Behavior

```
parent1: B-v@]gL{QHZOR754fsJj
parent2: e]ffS4,nc`g8G/yfL(j$
child1: 3$b@N<B:f;28=a.8J;0A (refs parent1)
child2: wV!x%p8W=SB<{({La}N: (refs parent2)

Query for parent1's children returned 1 results:
  Result 0: ID=b:Ic<JFVFBc[;0jc6IKa_/C9, Code=child1

BUG: Wrong child returned:
  expected: 3$b@N<B:f;28=a.8J;0A
  got:      b:Ic<JFVFBc[;0jc6IKa_/C9,

Found entity's ref: 9s4PW66]2[:i&&3&OdO%e(j$T
Our parent1: B-v@]gL{QHZOR754fsJj
Refs equal: false
```

Key observations:
1. Query correctly returns 1 result (not 2)
2. But the returned entity has a **completely different** `:test/ref` value
3. The entity ID returned doesn't match either child we created
4. The ref attribute of the found entity doesn't match our input `parent1`

## Root Cause Investigation

The bug appears to be in how `Identity` values are matched when passed as query input parameters. Possible areas to investigate:

1. **Value encoding/decoding**: Identity values may not be encoded/decoded consistently when used as inputs vs. when stored
2. **Input binding**: The input parameter binding may not correctly propagate Identity values to the pattern matcher
3. **Comparison**: Identity comparison in the matcher may have a bug

## Workaround

None known. The fundamental operation of "find entities where ref = X" is broken for Identity values.

## Test Location

- Bug reproduction test: `tests/identity_input_matching_bug_test.go`
- Original discovery: `narrative-generators/pkg/scenario/identity_bug_test.go`

## Related Issues

This may be related to other input parameter handling issues:
- `BUG_STRING_PREDICATES_CANT_USE_PARAMETERS.md` (resolved)
- `BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT.md` (resolved)

## Date Discovered

2024-12-17

