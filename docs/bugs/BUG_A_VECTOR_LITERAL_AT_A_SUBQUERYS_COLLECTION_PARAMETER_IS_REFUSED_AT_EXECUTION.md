# A vector literal at a subquery's collection parameter is refused at execution

**Status**: Open. A subquery call that passes a vector literal at a collection parameter parses, and `SubqueryPattern.Validate` and the typed-position walk accept it. `Query` and `Analyze` then refuse it with `unsupported subquery input element query.VectorConstant`, and `Explain` and `ExplainAlgebra` plan it. This holds at 22d903d, 5097666 and cd029af.

```clojure
[:find ?e :where [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ["x" "y"]) [[?e]]]]
```

The call is to answer the tuples the inner query answers for the vector's elements. Over `:item/one "x"` at `first` and `:item/one "y"` at `second`, that is `first` and `second`. At 22d903d, the refusal of a non-vector constant at a collection parameter names this form as its repair.

## The route

1. The parser reads `["x" "y"]` at a subquery argument as a `query.VectorConstant` (`datalog/query/types.go`), whose `Values` holds the elements.
2. `SubqueryPattern.Validate` (`datalog/query/clause_scope.go`) refuses an argument at a collection parameter only when the argument is a `query.Constant` whose value `VectorElements` does not read as a vector. A `VectorConstant` is not a `Constant`, so the check passes it.
3. `positionSymbolsFromSubquery` (`datalog/query/entity_position_symbols.go`) reads a `VectorConstant` at a collection parameter as its elements, one each, against the parameter's positions. It accepts `"x"` and `"y"` at a value position.
4. The query is planned. `Explain` and `ExplainAlgebra` return the plan.
5. `executeSubquery` (`datalog/executor/query_executor.go:1240`) builds the frames' environment relation from the call's arguments. Its switch has an arm for `query.Constant` and an arm for `query.Variable`. A `VectorConstant` reaches the default arm, which refuses it at `query_executor.go:1255`:

   ```go
   default:
       return nil, fmt.Errorf("unsupported subquery input element %T", arg)
   ```

6. So `Query` and `Analyze` fail at the subquery's clause:

   ```
   query execution failed: phase 1 failed: clause 0 (subquery) failed: unsupported subquery input element query.VectorConstant
   ```

   In a rule body invoked free, `(r ?e)`, the failure is the rule's:

   ```
   query execution failed: phase 1 failed: clause 0 (rule invocation) failed: rule r: phase 1 failed: clause 0 (subquery) failed: unsupported subquery input element query.VectorConstant
   ```

   In a rule body invoked bound, `:in $ % ?e`, it is the adorned rule's:

   ```
   query execution failed: phase 1 failed: clause 0 (rule invocation) failed: rule adorned.b/r: phase 1 failed: clause 1 (subquery) failed: unsupported subquery input element query.VectorConstant
   ```

## The entry points

| entry | 22d903d | 5097666 | cd029af |
|---|---|---|---|
| `ParseQuery`, `ParseRules` | accepted | accepted | accepted |
| `db.Query`, `storage.Database.Query` | refused: `unsupported subquery input element query.VectorConstant` | same | same |
| `Analyze` | refused: `unsupported subquery input element query.VectorConstant` | same | same |
| `Explain`, `ExplainAlgebra` | return a plan | same | same |

The table holds for the call alone, for the call after an outer `[?e :item/one]`, and for the call in a rule body passed as `%` and invoked free or bound. It holds in latest, `History` and `AsOf`, with the cache on and off. `qb.Subquery` takes `*Var` arguments alone (`datalog/qb/subquery.go`), so the builder cannot carry a vector literal.

## What 22d903d changed

At 22d903d, `SubqueryPattern.Validate` refuses a non-vector constant at a collection parameter, at every text boundary, with this message (`clause_scope.go:243`):

```
subquery argument N is <c>, and the collection parameter [?v ...] takes a vector: pass a vector literal, or a variable bound to one
```

At 5097666 no such message exists. A string at a collection parameter is refused at execution with `collection binding [?v ...] requires a vector result, got string`, a message that names no repair.

So at 22d903d a caller who passes a string at a collection parameter is told to pass a vector literal. Following that, `Query` and `Analyze` refuse the call with `unsupported subquery input element query.VectorConstant`, and `Explain` plans the same text.

## Contracts it contradicts

- CLAUDE.md, "Extend, Don't Avoid": "When a component can't handle a valid input, **extend the component**. Never: Return an error to refuse the work ("not supported")". The parser, `SubqueryPattern.Validate`, the typed-position walk and the planner each accept the call as a valid input. `executeSubquery` refuses it as unsupported.
- Four texts at 22d903d state that the form works:
  - `SubqueryPattern.Validate`'s message: "pass a vector literal, or a variable bound to one".
  - `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §7, the bullet 22d903d adds: the refusal names "the argument counted from one, the parameter and the repair".
  - The godoc of `positionSymbolsFromSubquery`: "A collection parameter binds a vector literal's elements one each, so each is read against the parameter's positions".
  - 22d903d's commit body: "the walk reads a vector literal's elements there and nothing else".

## What answers

These calls answer at 22d903d at the same collection parameter:

- A hand-built `query.Constant{Value: []interface{}{"x", "y"}}`, or a `query.Constant` holding `[]string{"x", "y"}`. The `Constant` arm binds the slice, and the collection binding reads its elements through `VectorElements`.
- A variable bound to a vector at a value-position collection parameter, from `:in` or from a `ground`:
  - `[:find ?e :in $ ?w :where [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ?w) [[?e]]]]` with `?w` given `[]interface{}{"x", "y"}`.
  - `[:find ?e :where [(ground ["x" "y"]) ?w] [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ?w) [[?e]]]]`.

Only the vector literal, the argument the parser builds for `["x" "y"]`, is refused.

## Reach

- Every caller of `db.Query`, `storage.Database.Query` or `Analyze` whose subquery call passes a vector literal at a collection parameter, directly or in a rule body passed as `%`, in every view.
- A caller who passes a non-vector constant at a collection parameter, receives 22d903d's refusal, and writes the first repair it names.
- A caller who writes a vector literal argument directly, at every tree.

## The fix, if taken

`executeSubquery`'s argument switch (`query_executor.go:1240`) takes a `query.VectorConstant` as it takes a `query.Constant`, binding the parameter's symbol to the vector's elements:

```go
case query.VectorConstant:
    environmentSyms = append(environmentSyms, sym)
    environmentValues = append(environmentValues, arg.Values)
```

The collection binding reads `arg.Values`, a `[]interface{}`, as it reads a `Constant`'s slice. With this arm, both reproducers below pass at 22d903d: the call answers `first` and `second`, and `first` alone under the bound invocation. No new structure is needed.

## Reproducers

Both tests are in package `db_test`, in `datalog/db/review_subquery_repro_test.go` and `datalog/db/review_refute_test.go`. Each fails at 22d903d, 5097666 and cd029af, and passes with the arm above.

- `TestReviewAVectorLiteralAtACollectionParameterAnswers` runs the call alone and after an outer `[?e :item/one]`, on `Query`, `Explain`, `ExplainAlgebra` and `Analyze`, in latest, `History` and `AsOf`, with the cache on and off. It asserts no error at each entry, and `first` and `second` from `Query`.
- `TestReviewRefuteAVectorLiteralAtACollectionParameterInARuleBodyAnswers` runs the call in the body of `(r ?e)`, invoked free and bound to `first`, over the same entries, views and cache settings. It asserts no error at each entry, and from `Query` both items free and `first` alone bound.

At 22d903d, run with `GOFLAGS=-trimpath go test ./datalog/db -run '^Name$' -count=1 -v`, each test fails with 24 refusals, every one `unsupported subquery input element query.VectorConstant`. They are `Query` and `Analyze`, for each of two texts or invocations, in three views, with the cache on and off. `Explain` and `ExplainAlgebra` return a plan in each of those cases. The first test's first refusal:

```
review_subquery_repro_test.go:110: cache=true latest Query [:find ?e :where [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ["x" "y"]) [[?e]]]]: refused: query execution failed: phase 1 failed: clause 0 (subquery) failed: unsupported subquery input element query.VectorConstant
```

The second test's first refusals, free and bound:

```
review_refute_test.go:37: cache=true latest free Query: refused: query execution failed: phase 1 failed: clause 0 (rule invocation) failed: rule r: phase 1 failed: clause 0 (subquery) failed: unsupported subquery input element query.VectorConstant
review_refute_test.go:37: cache=true latest bound Query: refused: query execution failed: phase 1 failed: clause 0 (rule invocation) failed: rule adorned.b/r: phase 1 failed: clause 1 (subquery) failed: unsupported subquery input element query.VectorConstant
```

The tests and the definitions they share, from `datalog/db/review_subquery_repro_test.go`:

```go
package db_test

import (
    "fmt"
    "testing"

    "github.com/wbrown/janus-datalog/datalog"
    "github.com/wbrown/janus-datalog/datalog/db"
    "github.com/wbrown/janus-datalog/datalog/executor"
)

// subqueryReproDB opens a memory database holding first's :item/one "x" and
// second's :item/one "y".
func subqueryReproDB(t *testing.T, opts ...db.Option) (*db.DB, datalog.ElementID) {
    d, err := db.OpenMemory(opts...)
    if err != nil {
        t.Fatal(err)
    }
    tx := d.NewTransaction()
    for _, f := range []struct {
        e string
        v string
    }{{"item:first", "x"}, {"item:second", "y"}} {
        if err := tx.Add(datalog.NewIdentity(f.e), datalog.NewKeyword(":item/one"), f.v); err != nil {
            t.Fatal(err)
        }
    }
    at, err := tx.Commit()
    if err != nil {
        t.Fatal(err)
    }
    return d, at
}

// runAtEntry runs a query through one entry point, a panic read as an error.
func runAtEntry(d *db.DB, kind string, q interface{}, inputs ...interface{}) (tuples [][]interface{}, err error) {
    defer func() {
        if p := recover(); p != nil {
            err = fmt.Errorf("panic: %v", p)
        }
    }()
    switch kind {
    case "Explain":
        _, err = d.Explain(q, inputs...)
        return nil, err
    case "ExplainAlgebra":
        _, err = d.ExplainAlgebra(q, inputs...)
        return nil, err
    case "Analyze":
        _, err = d.Analyze(q, inputs...)
        return nil, err
    }
    return executor.CollectTuples(d.Query(q, inputs...))
}

var entryKinds = []string{"Query", "Explain", "ExplainAlgebra", "Analyze"}

// The refusal names a vector literal as the repair: a subquery call passing
// a vector literal at a collection parameter answers the tuples the inner
// query answers for the vector's elements, on every entry point.
func TestReviewAVectorLiteralAtACollectionParameterAnswers(t *testing.T) {
    for _, opts := range [][]db.Option{nil, {db.WithoutCache()}} {
        d, at := subqueryReproDB(t, opts...)
        for _, view := range []struct {
            name string
            d    *db.DB
        }{{"latest", d}, {"History", d.History()}, {"AsOf", d.AsOf(at)}} {
            for _, text := range []string{
                `[:find ?e :where [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ["x" "y"]) [[?e]]]]`,
                `[:find ?e :where [?e :item/one] [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ["x" "y"]) [[?e]]]]`,
            } {
                for _, kind := range entryKinds {
                    tuples, err := runAtEntry(view.d, kind, text)
                    t.Logf("cache=%v %s %s %s: %v %v", opts == nil, view.name, kind, text, tuples, err)
                    if err != nil {
                        t.Errorf("cache=%v %s %s %s: refused: %v", opts == nil, view.name, kind, text, err)
                    } else if kind == "Query" && len(tuples) != 2 {
                        t.Errorf("cache=%v %s %s %s: answers %v, want first and second", opts == nil, view.name, kind, text, tuples)
                    }
                }
            }
        }
        if err := d.Close(); err != nil {
            t.Fatal(err)
        }
    }
}
```

From `datalog/db/review_refute_test.go`:

```go
package db_test

import (
    "testing"

    "github.com/wbrown/janus-datalog/datalog"
    "github.com/wbrown/janus-datalog/datalog/db"
)

// The collection-parameter refusal names a vector literal as its repair. A
// vector literal at a subquery's collection parameter, written in a rule's
// body and invoked free and bound, answers the tuples the inner query
// answers for the vector's elements.
func TestReviewRefuteAVectorLiteralAtACollectionParameterInARuleBodyAnswers(t *testing.T) {
    rules := `[[(r ?e) [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ["x" "y"]) [[?e]]]]]`
    first := datalog.NewIdentity("item:first")
    for _, opts := range [][]db.Option{nil, {db.WithoutCache()}} {
        d, at := subqueryReproDB(t, opts...)
        for _, view := range []struct {
            name string
            d    *db.DB
        }{{"latest", d}, {"History", d.History()}, {"AsOf", d.AsOf(at)}} {
            for _, c := range []struct {
                name, text string
                inputs     []interface{}
                want       int
            }{
                {"free", `[:find ?e :in $ % :where (r ?e)]`, []interface{}{rules}, 2},
                {"bound", `[:find ?e :in $ % ?e :where (r ?e)]`, []interface{}{rules, first}, 1},
            } {
                for _, kind := range entryKinds {
                    tuples, err := runAtEntry(view.d, kind, c.text, c.inputs...)
                    t.Logf("cache=%v %s %s %s: %v %v", opts == nil, view.name, c.name, kind, tuples, err)
                    if err != nil {
                        t.Errorf("cache=%v %s %s %s: refused: %v", opts == nil, view.name, c.name, kind, err)
                    } else if kind == "Query" && len(tuples) != c.want {
                        t.Errorf("cache=%v %s %s %s: answers %v, want %d tuples", opts == nil, view.name, c.name, kind, tuples, c.want)
                    }
                }
            }
        }
        if err := d.Close(); err != nil {
            t.Fatal(err)
        }
    }
}
```

## Related

- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §6, which states that `SubqueryPattern.Validate` refuses "a constant of another shape than a vector at a collection input's position", and §7, which states the refusal's contract.
