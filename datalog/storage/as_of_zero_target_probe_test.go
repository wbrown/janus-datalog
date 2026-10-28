package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An as-of handle at the zero ElementID reads nothing on any route
// (AsOfSnapshot's doc): every datom lies after the target.
func TestAsOfZeroTargetProbe(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":z/name").Type(schema.TypeString).Add().
		Attribute(":z/tags").Type(schema.TypeLong).Many().Add().
		Attribute(":z/vec").Type(schema.TypeLong).Vector().Add().
		Build()
	if err != nil {
		t.Fatal(err)
	}
	x := datalog.NewIdentity("zero-x")
	queries := []struct {
		name  string
		text  string
		input interface{}
	}{
		{"unbound", `[:find ?e ?a ?v :where [?e ?a ?v]]`, nil},
		{"constant attribute", `[:find ?e ?v :where [?e :z/name ?v]]`, nil},
		{"join, cache route", `[:find ?v ?t :where [?e :z/name ?v] [?e :z/tags ?t]]`, nil},
		{"V-bound card one, constant attribute", `[:find ?e :in $ ?v :where [?e :z/name ?v]]`, "x"},
		{"V-bound card one, open attribute", `[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`, "x"},
		{"V-bound card many, constant attribute", `[:find ?e :in $ ?v :where [?e :z/tags ?v]]`, int64(3)},
		{"V-bound card many, open attribute", `[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`, int64(3)},
		{"V-bound vector, open attribute", `[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`, []interface{}{int64(1), int64(2)}},
		{"vector, constant attribute", `[:find ?e ?v :where [?e :z/vec ?v]]`, nil},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			info, err := db.Snapshot("empty")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("empty-database snapshot At = %v (zero: %v)", info.At, info.At == datalog.ElementID{})
			tx := db.NewTransaction()
			tx.Add(x, datalog.NewKeyword(":z/name"), "x")
			tx.Add(x, datalog.NewKeyword(":z/tags"), int64(3))
			if err := tx.Set(x, datalog.NewKeyword(":z/vec"), []int64{1, 2}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			named, err := db.AsOfSnapshot("empty")
			if err != nil {
				t.Fatal(err)
			}
			handles := []struct {
				name string
				h    *Database
			}{{"AsOf(zero)", db.AsOf(datalog.ElementID{})}}
			_ = named
			for _, hh := range handles {
				h := hh.h
				for _, q := range queries {
					t.Run(hh.name+"/"+q.name, func(t *testing.T) {
						var rel executor.Relation
						var err error
						if q.input != nil {
							rel, err = h.Query(q.text, q.input)
						} else {
							rel, err = h.Query(q.text)
						}
						tuples, err := executor.CollectTuples(rel, err)
						if err != nil {
							t.Fatal(err)
						}
						if len(tuples) != 0 {
							t.Errorf("%s: %s answered %v", hh.name, q.text, tuples)
						}
					})
				}
			}
		})
	}
}
