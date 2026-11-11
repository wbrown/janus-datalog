package db_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// A fallback branch of one data pattern reading its fourth or fifth position
// answers on every leg what the reference leg answers, in the latest view and
// an as-of view, through query text and the query builder: the fifth
// position false, a variable bound false, a free variable the query finds,
// and the fourth position bound to a Lamport no datom has.
func TestRefuteFallbackBranchOfFiveElementsAnswersWithPrefetchAsTheReference(t *testing.T) {
	e, v := qb.NewVar("e"), qb.NewVar("v")
	built := qb.Query().Find(e, v).Where(
		qb.Pat(e, qb.Kw(":t/n"), qb.Blank()),
		qb.OrDefaultJoin(v).Required(e).
			Branch(qb.Pat(e, qb.Kw(":t/a"), v, qb.Blank(), qb.V(false))).
			Branch(qb.Ground(int64(0)).As(v)),
	).MustBuild()
	forms := []struct {
		name  string
		query interface{}
	}{
		{"text, constant false", `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v _ false] [(ground 0) ?v])]`},
		{"text, free variable found", `[:find ?e ?v ?added :where [?e :t/n _] (or-default-join [[?e] ?v ?added] [?e :t/a ?v _ ?added] (and [(ground 0) ?v] [(ground false) ?added]))]`},
		{"text, or-default", `[:find ?e ?v :where [?e :t/n _] (or-default [?e :t/a ?v _ false] [(ground 0) ?v])]`},
		{"text, fourth position bound to a value no transaction is", `[:find ?e ?v :where [(ground 5) ?tx] [?e :t/n _] (or-default-join [[?e ?tx] ?v] [?e :t/a ?v ?tx] [(ground 0) ?v])]`},
		{"builder, constant false", built},
	}
	answers := func(t *testing.T, mode optimizerMode, asOf bool, q interface{}) []string {
		d, err := db.OpenMemory(db.WithPlannerOptions(mode.plannerOptions()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		names := map[datalog.Identity]string{}
		tx := d.NewTransaction()
		for i := 1; i <= 120; i++ {
			e := datalog.NewIdentity(fmt.Sprintf("e%d", i))
			names[e] = fmt.Sprintf("e%d", i)
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":t/n"), int64(i)))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":t/a"), int64(i)))
		}
		point, err := tx.Commit()
		require.NoError(t, err)
		view := d
		if asOf {
			view = d.AsOf(point)
		}
		rel, err := view.Query(q)
		require.NoError(t, err)
		tuples, err := executor.CollectTuples(rel, nil)
		require.NoError(t, err)
		var out []string
		for _, tuple := range tuples {
			s := names[tuple[0].(datalog.Identity)]
			for _, v := range tuple[1:] {
				s += fmt.Sprintf(" %v", v)
			}
			out = append(out, s)
		}
		sort.Strings(out)
		return out
	}
	for _, form := range forms {
		for _, asOf := range []bool{false, true} {
			want := answers(t, referenceMode, asOf, form.query)
			for _, mode := range legsFor(t, optimizerModes, prefetchOption) {
				t.Run(fmt.Sprintf("%s/asOf=%v/%s", form.name, asOf, mode.name), func(t *testing.T) {
					assert.Equal(t, want, answers(t, mode, asOf, form.query))
				})
			}
		}
	}
}
