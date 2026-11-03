package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A query whose expression fails at an assignment where every other clause
// holds answers the failure through every read entry of the database: the
// result's iterator, QueryInto, QueryOneInto, and a rule body evaluated for
// an invocation (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1;
// TestAJoinWithAnEmptySideAnswersEmptyThoughTheOtherSideFails).
func TestRefuteEval_AFailureWhereEveryOtherClauseHoldsFailsEveryReadEntry(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	v := datalog.NewKeyword(":item/v")
	type fact struct {
		e datalog.Identity
		a datalog.Keyword
		v interface{}
	}
	alice := datalog.NewIdentity("refute-eval:alice")
	facts := []fact{{alice, name, "Alice"}}
	for i := 0; i < 16; i++ {
		numeric := datalog.NewIdentity("refute-eval:n" + string(rune('a'+i)))
		facts = append(facts, fact{numeric, v, int64(i)})
	}
	stringy := datalog.NewIdentity("refute-eval:stringy")
	facts = append(facts, fact{stringy, v, "x"})

	const guarded = `[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]`
	type entity struct {
		P datalog.Identity `datalog:"?p"`
	}
	cases := []struct {
		name string
		run  func(d *Database) error
	}{
		{"the result's iterator", func(d *Database) error {
			_, err := executor.CollectTuples(d.Query(guarded))
			return err
		}},
		{"QueryInto", func(d *Database) error {
			var into []entity
			return d.QueryInto(&into, guarded)
		}},
		{"QueryOneInto", func(d *Database) error {
			var one entity
			found, err := d.QueryOneInto(&one, guarded)
			if err == nil && !found {
				t.Errorf("QueryOneInto found no tuple")
			}
			return err
		}},
		{"a rule body", func(d *Database) error {
			_, err := executor.CollectTuples(d.Query(`[:find ?p :in $ % :where (guard ?p)]`,
				`[[(guard ?p) [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]]`))
			return err
		}},
		{"a rule body invoked with its argument bound", func(d *Database) error {
			_, err := executor.CollectTuples(d.Query(`[:find ?p :in $ % :where [?p :item/name "Alice"] (guard ?p)]`,
				`[[(guard ?p) [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b]]]`))
			return err
		}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for _, f := range facts {
				require.NoError(t, tx.Add(f.e, f.a, f.v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					require.ErrorContains(t, c.run(d), "is not a number")
				})
			}
		})
	}
}

// A failing expression's group beside a group with no tuple fails no query:
// no assignment holds every other clause (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md
// §1, the failure contract).
func TestRefuteEval_AFailingGroupBesideAnEmptyGroupFailsNoQuery(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	v := datalog.NewKeyword(":item/v")
	alice := datalog.NewIdentity("refute-eval-empty:alice")
	stringy := datalog.NewIdentity("refute-eval-empty:stringy")
	queries := []string{
		`[:find ?p :where [?p :item/name _] [?q :item/v ?a] [(+ ?a 1) ?b] [?r :item/missing _]]`,
		`[:find ?p :where [?p :item/name _] [?r :item/missing _] [?q :item/v ?a] [(+ ?a 1) ?b]]`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			require.NoError(t, tx.Add(stringy, v, "x"))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, q := range queries {
				t.Run(q, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(q))
					require.NoError(t, err)
					require.Empty(t, got)
				})
			}
		})
	}
}
