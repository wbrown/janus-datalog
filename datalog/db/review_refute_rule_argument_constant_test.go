package db_test

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Review instrument (PR 126 round 4, import-records refuter). The proof's
// sentence (BOUND_TRANSACTION_POSITION_PROOF.md:14, ANTIPATTERN_AUDIT A4's
// 2026-11 Status) says a constant of another type at the transaction position
// is refused at the user boundary. Written as a rule invocation's argument
// that the rule's body puts at T, it is asserted here to be refused as the
// same constant in a pattern is, in the latest, History and as-of views, with
// the cache on and off.
func TestReviewRefuteARuleArgumentConstantOfAnotherTypeAtTIsRefused(t *testing.T) {
	entity := datalog.NewIdentity("review-tx:one")
	name := datalog.NewKeyword(":t/name")
	rules := `[[(named ?e ?t) [?e :t/name _ ?t]]]`
	for _, opts := range [][]db.Option{nil, {db.WithoutCache()}} {
		d, err := db.OpenMemory(opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer func(d *db.DB) {
			controlTuples, controlErr := executor.CollectTuples(d.Query(`[:find ?e :where [(q [:find ?x :in $ ?n :where [?x :t/name ?n]] $ "a") [[?e]]]]`))
			t.Logf("cache=%v control, subquery constant at V: tuples=%v err=%v", opts == nil, controlTuples, controlErr)
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}(d)
		if err != nil {
			t.Fatal(err)
		}
		tx := d.NewTransaction()
		if err := tx.Add(entity, name, "a"); err != nil {
			t.Fatal(err)
		}
		committed, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		views := []struct {
			name string
			d    *db.DB
		}{{"latest", d}, {"history", d.History()}, {"as-of", d.AsOf(committed)}}
		for _, v := range views {
			for _, c := range []struct {
				label string
				query string
				in    []interface{}
			}{
				{"pattern constant (control)", `[:find ?e :where [?e :t/name _ "x"]]`, nil},
				{"rule argument string", `[:find ?e :in $ % :where (named ?e "x")]`, []interface{}{rules}},
				{"rule argument int64", `[:find ?e :in $ % :where (named ?e 5)]`, []interface{}{rules}},
				{"subquery argument string", `[:find ?e :where [(q [:find ?x :in $ ?tx :where [?x :t/name _ ?tx]] $ "x") [[?e]]]]`, nil},
			} {
				label := fmt.Sprintf("cache=%v/%s/%s", opts == nil, v.name, c.label)
				var tuples [][]interface{}
				var qerr error
				func() {
					defer func() {
						if r := recover(); r != nil {
							qerr = fmt.Errorf("panic: %v", r)
						}
					}()
					tuples, qerr = executor.CollectTuples(v.d.Query(c.query, c.in...))
				}()
				t.Logf("%s: tuples=%v err=%v", label, tuples, qerr)
				if qerr == nil {
					t.Errorf("%s: a constant of another type at T answers %v and no error; the proof says it is refused at the user boundary", label, tuples)
				}
			}
		}
	}
}
