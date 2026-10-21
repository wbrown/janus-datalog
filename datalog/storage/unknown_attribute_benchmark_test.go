package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkUnknownAttributeCheck measures what the refusal costs a query on
// every backend: the check alone, through the matcher, and the whole query
// path. Each runs on a database declaring the fixture schema: opened without
// the option, where the gate is off; opened with it, for a query naming a
// declared attribute, which runs; opened with it, for a query whose subquery
// names a declared attribute and runs once per outer tuple; opened with it,
// for a query under a rule set whose or-join branch passes, at a position the
// rule set requires, a variable the enclosing scope binds to a declared
// attribute, so the roster resolves under the rule set; and opened with it,
// for a query naming an attribute the store carries but the schema does not
// declare, which is refused.
func BenchmarkUnknownAttributeCheck(b *testing.B) {
	const declared = `[:find ?v :where [?e :t/string ?v]]`
	const declaredInSubquery = `[:find ?e ?n :where [?e :t/string ?v] [(q [:find (count ?x) :in $ ?e :where [?e :t/string ?x]] $ ?e) [[?n]]]]`
	const declaredThroughRules = `[:find ?e :in $ % :where [(ground :t/string) ?a] (or-join [[?a] ?e] (held ?a ?e) [?e :t/string _])]`
	const requiringRules = `[[(held [?a] ?e) [?e ?a _]]]`
	const undeclared = `[:find ?v :where [?e :t/loose ?v]]`
	cases := []struct {
		name    string
		refuse  bool
		text    string
		rules   string
		refused bool
	}{
		{"gate-off", false, declared, "", false},
		{"declared", true, declared, "", false},
		{"declared-in-subquery", true, declaredInSubquery, "", false},
		{"declared-through-rules", true, declaredThroughRules, requiringRules, false},
		{"undeclared-written", true, undeclared, "", true},
	}
	for _, backend := range AvailableBackends() {
		for _, c := range cases {
			opts := DatabaseOptions{Schema: schemaFactsFixture(b), RefuseUnknownAttributes: c.refuse}
			db := openBackendDB(b, backend, opts)
			entity := datalog.NewIdentity("holder")
			tx := db.NewTransaction()
			if err := tx.Add(entity, kw(":t/string"), "held"); err != nil {
				b.Fatal(err)
			}
			if err := tx.Add(entity, kw(":t/loose"), "loose"); err != nil {
				b.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			q, err := parser.ParseQuery(c.text)
			if err != nil {
				b.Fatal(err)
			}
			var rules *query.RuleSet
			var inputs []interface{}
			if c.rules != "" {
				rules, err = parser.ParseRules(c.rules)
				if err != nil {
					b.Fatal(err)
				}
				inputs = []interface{}{rules}
			}

			b.Run("check/"+backend.Name+"/"+c.name, func(b *testing.B) {
				benchmarkCheck(b, db, q, rules, inputs, c.refused)
			})
			b.Run("query/"+backend.Name+"/"+c.name, func(b *testing.B) {
				benchmarkQuery(b, db, q, inputs, c.refused)
			})
		}
	}
}

// benchmarkCheck times the matcher's check alone on the roster q's plan
// carries under rules, made once ahead of the loop, which refuses q when
// refused is set and accepts it otherwise. A database that refuses nothing
// makes a plan carrying no roster, and its check reads none.
func benchmarkCheck(b *testing.B, db *Database, q *query.Query, rules *query.RuleSet, inputs []interface{}, refused bool) {
	matcher := db.Matcher().(*PatternMatcher)
	plan, err := db.Explain(q, inputs...)
	if err != nil {
		b.Fatal(err)
	}
	var attrs []datalog.Keyword
	if plan.Rosters != nil {
		attrs = plan.Rosters.DefaultSourceAttributes(rules)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := matcher.UnknownAttributes(datalog.SymDollar, attrs)
		if refused && err == nil {
			b.Fatal("the check accepted a query it refuses")
		}
		if !refused && err != nil {
			b.Fatal(err)
		}
	}
}

// benchmarkQuery times the whole query path, the check included, with inputs
// bound to the query's :in. An accepted query returns the holder's one value;
// a refused one returns the refusal.
func benchmarkQuery(b *testing.B, db *Database, q *query.Query, inputs []interface{}, refused bool) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tuples, err := executor.CollectTuples(db.Query(q, inputs...))
		if refused {
			if err == nil {
				b.Fatal("the query ran where it is refused")
			}
			continue
		}
		if err != nil {
			b.Fatal(err)
		}
		if len(tuples) != 1 {
			b.Fatalf("got %d tuples, want 1", len(tuples))
		}
	}
}
