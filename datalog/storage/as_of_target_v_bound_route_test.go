package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An as-of handle answers, on the V-bound route, the cardinality-many members
// written at or before its target and none written after, with the attribute
// open and with it constant.
func TestAnAsOfHandleOnTheVBoundRouteAnswersTheMembersAtOrBeforeItsTarget(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":z/name").Type(schema.TypeString).Add().
		Attribute(":z/tags").Type(schema.TypeLong).Many().Add().
		Build()
	if err != nil {
		t.Fatal(err)
	}
	tags := datalog.NewKeyword(":z/tags")
	before := datalog.NewIdentity("as-of-target-before")
	after := datalog.NewIdentity("as-of-target-after")
	queries := []struct {
		name string
		text string
	}{
		{"open attribute", `[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`},
		{"constant attribute", `[:find ?e :in $ ?v :where [?e :z/tags ?v]]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			tx := db.NewTransaction()
			if err := tx.Add(before, datalog.NewKeyword(":z/name"), "before"); err != nil {
				t.Fatal(err)
			}
			if err := tx.Add(before, tags, int64(3)); err != nil {
				t.Fatal(err)
			}
			target, err := tx.Commit()
			if err != nil {
				t.Fatal(err)
			}
			tx = db.NewTransaction()
			if err := tx.Add(after, tags, int64(3)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			h := db.AsOf(target)
			for _, q := range queries {
				t.Run(q.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(h.Query(q.text, int64(3)))
					if err != nil {
						t.Fatal(err)
					}
					if len(tuples) != 1 || tuples[0][0] != before {
						t.Fatalf("%s answered %v, want the member written at or before the target alone", q.text, tuples)
					}
				})
			}
		})
	}
}
