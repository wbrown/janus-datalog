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
// declared attribute, which runs; and opened with it, for a query naming an
// attribute the store carries but the schema does not declare, which is
// refused.
func BenchmarkUnknownAttributeCheck(b *testing.B) {
	const declared = `[:find ?v :where [?e :t/string ?v]]`
	const undeclared = `[:find ?v :where [?e :t/loose ?v]]`
	cases := []struct {
		name    string
		refuse  bool
		text    string
		refused bool
	}{
		{"gate-off", false, declared, false},
		{"declared", true, declared, false},
		{"undeclared-written", true, undeclared, true},
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

			b.Run("check/"+backend.Name+"/"+c.name, func(b *testing.B) {
				benchmarkCheck(b, db, q, c.refused)
			})
			b.Run("query/"+backend.Name+"/"+c.name, func(b *testing.B) {
				benchmarkQuery(b, db, q, c.refused)
			})
		}
	}
}

// benchmarkCheck times the matcher's check alone, which refuses q when refused
// is set and accepts it otherwise.
func benchmarkCheck(b *testing.B, db *Database, q *query.Query, refused bool) {
	matcher := db.Matcher().(*PatternMatcher)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := matcher.UnknownAttributes(q)
		if refused && err == nil {
			b.Fatal("the check accepted a query it refuses")
		}
		if !refused && err != nil {
			b.Fatal(err)
		}
	}
}

// benchmarkQuery times the whole query path, the check included. An accepted
// query returns the holder's one value; a refused one returns the refusal.
func benchmarkQuery(b *testing.B, db *Database, q *query.Query, refused bool) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tuples, err := executor.CollectTuples(db.Query(q))
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
