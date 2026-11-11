package db_test

import (
    "errors"
    "fmt"
    "testing"

    "github.com/wbrown/janus-datalog/datalog"
    "github.com/wbrown/janus-datalog/datalog/db"
    "github.com/wbrown/janus-datalog/datalog/executor"
    "github.com/wbrown/janus-datalog/datalog/parser"
    "github.com/wbrown/janus-datalog/datalog/query"
)

// walkReproDB opens a memory database holding first's :item/one "x" and
// :item/n 5, and second's :item/one "y".
func walkReproDB(t *testing.T, opts ...db.Option) (*db.DB, datalog.ElementID, datalog.Identity) {
    first := datalog.NewIdentity("item:first")
    second := datalog.NewIdentity("item:second")
    d, err := db.OpenMemory(opts...)
    if err != nil {
        t.Fatal(err)
    }
    tx := d.NewTransaction()
    for _, f := range []struct {
        e datalog.Identity
        a string
        v interface{}
    }{{first, ":item/one", "x"}, {first, ":item/n", int64(5)}, {second, ":item/one", "y"}} {
        if err := tx.Add(f.e, datalog.NewKeyword(f.a), f.v); err != nil {
            t.Fatal(err)
        }
    }
    at, err := tx.Commit()
    if err != nil {
        t.Fatal(err)
    }
    return d, at, first
}

func walkReproQuery(d *db.DB, kind, text string, inputs ...interface{}) (tuples [][]interface{}, err error) {
    defer func() {
        if p := recover(); p != nil {
            err = fmt.Errorf("panic: %v", p)
        }
    }()
    switch kind {
    case "Explain":
        _, err = d.Explain(text, inputs...)
        return nil, err
    case "ExplainAlgebra":
        _, err = d.ExplainAlgebra(text, inputs...)
        return nil, err
    case "Analyze":
        _, err = d.Analyze(text, inputs...)
        return nil, err
    }
    return executor.CollectTuples(d.Query(text, inputs...))
}

// A rule set whose predicate wr writes a string at the transaction position
// p's body occupies is accepted by NewRuleSet. A query that invokes only g,
// or no predicate at all, never evaluates wr: its runtime reaches no typed
// position with the string, and it answers first.
func TestReviewAConstantInAPredicateTheQueryNeverEvaluatesDoesNotRefuseIt(t *testing.T) {
    text := `[[(g ?o) [?o :item/n 5]] [(wr ?o) (p "x" ?o)] [(p ?c ?o) [?o :item/one ?w ?c]]]`
    parsed, err := parser.ParseRules(text)
    if err != nil {
        t.Fatalf("the rule set parses and NewRuleSet accepts it: %v", err)
    }
    for _, opts := range [][]db.Option{nil, {db.WithoutCache()}} {
        d, at, first := walkReproDB(t, opts...)
        for _, rules := range []interface{}{text, parsed} {
            for _, q := range []string{
                `[:find ?o :in $ % :where (g ?o)]`,
                `[:find ?o :in $ % :where [?o :item/n 5]]`,
            } {
                for _, view := range []struct {
                    name string
                    d    *db.DB
                }{{"latest", d}, {"History", d.History()}, {"AsOf", d.AsOf(at)}} {
                    tuples, err := walkReproQuery(view.d, "Query", q, rules)
                    t.Logf("cache=%v %T %s %s: %v %v", opts == nil, rules, view.name, q, tuples, err)
                    if err != nil {
                        t.Errorf("cache=%v %T %s %s: refused (%v); the query never evaluates wr", opts == nil, rules, view.name, q, err)
                    } else if len(tuples) != 1 || tuples[0][0] != first {
                        t.Errorf("cache=%v %T %s %s: answers %v, want [[first]]", opts == nil, rules, view.name, q, tuples)
                    }
                }
                for _, kind := range []string{"Explain", "ExplainAlgebra", "Analyze"} {
                    if _, err := walkReproQuery(d, kind, q, rules); err != nil {
                        t.Errorf("cache=%v %T %s %s: refused (%v)", opts == nil, rules, kind, q, err)
                    }
                }
            }
        }
        if err := d.Close(); err != nil {
            t.Fatal(err)
        }
    }
}

// A query naming a variable in the unify namespace is refused as
// EngineNamespaceError naming it, whatever the inputs; ?unify/foo.1 is the
// user's own variable, not the input ?foo, which the text places at the value
// position alone.
func TestReviewAUserVariableInTheUnifyNamespaceIsRefusedAsEngineNamespaceError(t *testing.T) {
    d, _, _ := walkReproDB(t)
    for _, c := range []struct {
        query string
        input interface{}
    }{
        {`[:find ?v :in $ ?foo :where [?unify/foo.1 :item/one ?v] [?e :item/one ?foo]]`, "x"},
        {`[:find ?v :in $ ?mine/foo :where [?e :item/one ?v ?unify.mine/foo.1] [?e :item/one ?mine/foo]]`, "x"},
    } {
        for _, kind := range []string{"Query", "Explain", "ExplainAlgebra", "Analyze"} {
            _, err := walkReproQuery(d, kind, c.query, c.input)
            var engine *query.EngineNamespaceError
            t.Logf("%s %s: %v", kind, c.query, err)
            if !errors.As(err, &engine) {
                t.Errorf("%s %s: refused as %v, not as EngineNamespaceError", kind, c.query, err)
            }
        }
    }
    if err := d.Close(); err != nil {
        t.Fatal(err)
    }
}

// A constant of another type a subquery's argument binds into an inner
// typed position is refused at the query's entry, as one bound through a
// scalar parameter is, when the parameter is a tuple's or a relation's
// symbol, or a scalar declared after a tuple parameter, directly and in a
// rule's body.
func TestReviewAConstantThroughASubqueryTupleOrRelationParameterIsRefused(t *testing.T) {
    for _, opts := range [][]db.Option{nil, {db.WithoutCache()}} {
        d, at, _ := walkReproDB(t, opts...)
        for _, c := range []struct {
            name, query string
            inputs      []interface{}
        }{
            {"control: scalar parameter", `[:find ?o :where [(q [:find ?o :in $ ?c :where [?o :item/one ?w ?c]] $ "x") [[?o]]]]`, nil},
            {"tuple parameter's symbol", `[:find ?o :where [(q [:find ?o :in $ [?c ?d] :where [?o :item/one ?w ?c]] $ "x" 1) [[?o]]]]`, nil},
            {"relation parameter's symbol", `[:find ?o :where [(q [:find ?o :in $ [[?c ?d]] :where [?o :item/one ?w ?c]] $ "x" 1) [[?o]]]]`, nil},
            {"scalar parameter after a tuple parameter", `[:find ?o :where [(q [:find ?o :in $ [?c ?d] ?t :where [?o :item/one ?w ?t]] $ 1 2 "x") [[?o]]]]`, nil},
            {"tuple parameter's symbol in a rule body", `[:find ?o :in $ % :where (r ?o)]`, []interface{}{`[[(r ?o) [(q [:find ?o :in $ [?c ?d] :where [?o :item/one ?w ?c]] $ "x" 1) [[?o]]]]]`}},
        } {
            for _, view := range []struct {
                name string
                d    *db.DB
            }{{"latest", d}, {"History", d.History()}, {"AsOf", d.AsOf(at)}} {
                tuples, err := walkReproQuery(view.d, "Query", c.query, c.inputs...)
                t.Logf("cache=%v %s %s: %v %v", opts == nil, view.name, c.name, tuples, err)
                if err == nil {
                    t.Errorf("cache=%v %s %s: a string at the transaction position answers %v and no error", opts == nil, view.name, c.name, tuples)
                }
            }
        }
        if err := d.Close(); err != nil {
            t.Fatal(err)
        }
    }
}
