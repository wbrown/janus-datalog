# Proof: OR-join does not require multiple iterations of the same sequence

## Question

Does the OR-join evaluation algorithm inherently require iterating the same
data sequence multiple times? If not, the `StreamingRelation.Iterator() called
multiple times` panic is a bug in the implementation, not a fundamental
limitation requiring LazySeq wrapping or materialization.

## OR-join has two semantic modes

### Mode 1: Union semantics (no expressions in branches)

Each branch runs independently, once. Results are merged.

```
(or-join [?x ?y]
  [?x :attr1 ?y]     ;; branch 1: one scan
  [?x :attr2 ?y])    ;; branch 2: one scan
```

- Branch 1 executes → produces Relation R1
- Branch 2 executes → produces Relation R2
- Result = R1 ∪ R2
- Each branch iterator is consumed exactly once
- The union result is iterated exactly once by the consumer

**No re-iteration required.**

### Mode 2: Fallback semantics (branches contain expressions)

Per outer tuple, try branches in order until one matches.

```
(or-join [?x ?count]
  [(q [...] $) [[?x ?count] ...]]    ;; branch 1: subquery
  [(ground 0) ?count])                ;; branch 2: default
```

For each outer tuple:
1. Execute branch 1 with this tuple's context → Relation or nil
2. If branch 1 matched, use it. Otherwise try branch 2.
3. Short-circuit: stop on first match.

Each branch execution is independent — fresh evaluation per outer tuple.
No branch result is shared across outer tuples. The branch result is
consumed once (to check for matches and extract tuples), then discarded.

**No re-iteration required.**

## Nested OR-join

Consider the failing query pattern:

```
(or-join [?related ?self ?stype]            ;; OUTER: fallback (has ground)
  (and [(ground :type/parent) ?stype]
       (or-join [?related ?self]            ;; INNER: union (pure DataPatterns)
         [?related :child/parent ?self]
         [?self :entity/friend ?related]))
  (and [(ground :type/child) ?stype]
       [?self :entity/friend ?related]))
```

The **outer** or-join uses fallback semantics (branches contain `ground`).
The **inner** or-join uses union semantics (branches are pure DataPatterns).

For each outer tuple (e.g., Alice with ?stype=:type/parent):
1. Evaluate outer branch 1: `(and [(ground :type/parent) ?stype] (or-join ...))`
2. The `ground` expression binds ?stype — checked against outer tuple
3. The inner or-join executes:
   - Branch A: scan `[?related :child/parent Alice]` → one iterator, consumed once
   - Branch B: scan `[Alice :entity/friend ?related]` → one iterator, consumed once
   - Union: merge A and B → consumed once by the and-branch collapse
4. The and-branch result is consumed once by the outer fallback to check for matches

At no point is any iterator consumed more than once.

## Why the panic occurs

The `ProductIterator` panic means the executor has **disjoint relation groups**
after clause execution — groups that don't share symbols and can't be joined.
`ProductIterator` computes their cross product, which requires re-iterating
the shorter relation for each tuple of the longer one.

But this query should NOT have disjoint groups. The symbols flow:
- Clause 1: `[?self :entity/name ?self-name]` → provides `{?self, ?self-name}`
- Clause 2: `[?self :entity/type ?stype]` → provides `{?self, ?stype}`, joins on `?self`
- Clause 3: `(or-join [?related ?self ?stype] ...)` → provides `{?related, ?self, ?stype}`, joins on `?self, ?stype`
- Clause 4: `[?related :entity/name ?related-name]` → provides `{?related, ?related-name}`, joins on `?related`

Every clause shares at least one symbol with the accumulated context. No
disjoint groups should exist. The `ProductIterator` should never be created.

## Conclusion

1. **OR-join does not require multiple iterations.** Both union and fallback
   modes consume each branch iterator exactly once.

2. **The ProductIterator panic is a symptom of a symbol-tracking bug.** The
   executor is failing to recognize that the or-join result shares symbols
   with other groups, creating a false disjoint condition.

3. **LazySeq wrapping is treating the symptom, not the cause.** The real fix
   is to ensure the or-join result has correct symbols so the collapse joins
   it with the other groups instead of creating a cross product.

## Investigation needed

Check what symbols `executeOrJoinClauseFallback` reports on its result.
The `OrFallbackRelation.Symbols()` must include the join variables
(`?related`, `?self`, `?stype`). If the symbols are wrong or empty,
the collapse will treat it as disjoint and create a `ProductIterator`.
