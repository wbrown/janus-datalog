package db_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// reviewAnswer runs one query and renders its tuples, its error or its panic.
func reviewAnswer(d *db.DB, text string, inputs ...interface{}) (answer string) {
	defer func() {
		if p := recover(); p != nil {
			answer = fmt.Sprintf("panic: %v", p)
		}
	}()
	tuples, err := executor.CollectTuples(d.Query(text, inputs...))
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("tuples: %v", tuples)
}

func reviewTypingDB(t *testing.T, opts ...db.Option) (*db.DB, datalog.ElementID, datalog.Identity) {
	first := datalog.NewIdentity("item:first")
	second := datalog.NewIdentity("item:second")
	one := datalog.NewKeyword(":item/one")
	next := datalog.NewKeyword(":item/next")
	d, err := db.OpenMemory(opts...)
	if err != nil {
		t.Fatal(err)
	}
	tx := d.NewTransaction()
	for _, f := range []struct {
		e datalog.Identity
		a datalog.Keyword
		v interface{}
	}{{first, one, "x"}, {first, next, second}, {second, one, "y"}} {
		if err := tx.Add(f.e, f.a, f.v); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	held, err := executor.CollectTuples(d.Query(`[:find ?t :where [?e :item/one "x" ?t]]`))
	if err != nil || len(held) != 1 {
		t.Fatalf("the assertion's transaction: %v %v", held, err)
	}
	at, ok := datalog.DerefElementID(held[0][0])
	if !ok {
		t.Fatalf("the transaction position binds %T", held[0][0])
	}
	return d, at, first
}

// An input whose rule argument reaches a body's value position, and a
// not-join's or an or-join's position only through a variable outside the
// join list that shares the head variable's name, reaches no typed
// position: it answers what the ground-bound argument answers.
func TestReviewARuleArgumentReachingAJoinListLocalAloneIsNotRefused(t *testing.T) {
	for _, opts := range [][]db.Option{nil, {db.WithoutCache()}} {
		d, _, _ := reviewTypingDB(t, opts...)
		for _, c := range []struct {
			name, rules string
		}{
			{"not-join", `[[(r ?a ?b) [?b :item/one ?a] (not-join [?b] [?a :item/next ?b])]]`},
			{"or-join", `[[(r ?a ?b) [?b :item/one ?a] (or-join [?b] [?a :item/next ?b] [?b :item/one _])]]`},
		} {
			for _, view := range []struct {
				name string
				d    *db.DB
			}{{"latest", d}, {"history", d.History()}} {
				input := reviewAnswer(view.d, `[:find ?b :in $ % ?a :where (r ?a ?b)]`, c.rules, "x")
				ground := reviewAnswer(view.d, `[:find ?b :in $ % :where [(ground "x") ?a] (r ?a ?b)]`, c.rules)
				t.Logf("cache=%v %s %s: input %s / ground %s", opts == nil, view.name, c.name, input, ground)
				if input != ground {
					t.Errorf("cache=%v %s %s: the rule argument input answers %s, the ground-bound argument %s", opts == nil, view.name, c.name, input, ground)
				}
			}
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// An input reaching a typed position through a rule argument is refused as
// the same input reaching the same pattern directly is, a pattern naming the
// argument at two positions included, and a constant of another type written
// at the invocation's typed position is refused as the same constant written
// in the pattern is.
func TestReviewARuleArgumentIsRefusedAsTheDirectInputIs(t *testing.T) {
	for _, opts := range [][]db.Option{nil, {db.WithoutCache()}} {
		d, at, first := reviewTypingDB(t, opts...)
		for _, c := range []struct {
			name, direct, rule, rules string
			inputs                    []interface{}
		}{
			{"value and transaction", `[:find ?e :in $ ?t :where [?e :item/one ?t ?t]]`, `[:find ?e :in $ % ?t :where (r ?e ?t)]`, `[[(r ?e ?t) [?e :item/one ?t ?t]]]`, []interface{}{"x"}},
			{"entity and attribute", `[:find ?v :in $ ?x :where [?x ?x ?v]]`, `[:find ?v :in $ % ?x :where (r ?x ?v)]`, `[[(r ?x ?v) [?x ?x ?v]]]`, []interface{}{first}},
			{"entity and transaction", `[:find ?v :in $ ?x :where [?x :item/one ?v ?x]]`, `[:find ?v :in $ % ?x :where (r ?x ?v)]`, `[[(r ?x ?v) [?x :item/one ?v ?x]]]`, []interface{}{at}},
			{"constant at the transaction", `[:find ?e :where [?e :item/one _ "x"]]`, `[:find ?e :in $ % :where (r ?e "x")]`, `[[(r ?e ?t) [?e :item/one _ ?t]]]`, nil},
		} {
			for _, view := range []struct {
				name string
				d    *db.DB
			}{{"latest", d}, {"history", d.History()}} {
				direct := reviewAnswer(view.d, c.direct, c.inputs...)
				rule := reviewAnswer(view.d, c.rule, append([]interface{}{c.rules}, c.inputs...)...)
				t.Logf("cache=%v %s %s: direct %s / rule %s", opts == nil, view.name, c.name, direct, rule)
				if strings.HasPrefix(direct, "error:") && !strings.HasPrefix(rule, "error:") {
					t.Errorf("cache=%v %s %s: the direct form is refused (%s), the rule form answers %s", opts == nil, view.name, c.name, direct, rule)
				}
			}
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Explain and Analyze read a rule argument's input as Query does.
func TestReviewExplainAndAnalyzeReadARuleArgumentAsQueryDoes(t *testing.T) {
	d, _, _ := reviewTypingDB(t)
	rules := `[[(r ?e ?t) [?e :item/one _ ?t]]]`
	q := `[:find ?e :in $ % ?t :where (r ?e ?t)]`
	_, explainErr := d.Explain(q, rules, "x")
	_, analyzeErr := d.Analyze(q, rules, "x")
	t.Logf("Explain: %v / Analyze: %v / Query: %s", explainErr, analyzeErr, reviewAnswer(d, q, rules, "x"))
	if explainErr == nil || analyzeErr == nil {
		t.Errorf("Explain %v, Analyze %v: a string reaching the transaction position through a rule argument", explainErr, analyzeErr)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
