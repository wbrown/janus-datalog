package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A get-else whose binding variable the clauses before it already bind is
// a unification, as any expression onto a bound variable is: the tuple
// survives when the value read, or the default, equals the bound value, and
// is dropped otherwise. That holds when the binding is the get-else's own
// entity variable and when another pattern binds it, under both planner
// modes. A pattern that names one variable at two positions unifies them
// the same way.
func TestAGetElseOntoABoundVariableUnifies(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			self := datalog.NewIdentity("u:self")
			other := datalog.NewIdentity("u:other")
			lone := datalog.NewIdentity("u:lone")
			link := datalog.NewKeyword(":u/link")
			n := datalog.NewKeyword(":u/n")
			m := datalog.NewKeyword(":u/m")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(self, link, self))
			require.NoError(t, tx.Add(other, link, self))
			require.NoError(t, tx.Add(self, n, int64(1)))
			require.NoError(t, tx.Add(self, m, int64(1)))
			require.NoError(t, tx.Add(other, n, int64(2)))
			require.NoError(t, tx.Add(other, m, int64(3)))
			require.NoError(t, tx.Add(lone, m, int64(5)))
			_, err := tx.Commit()
			require.NoError(t, err)

			cases := []struct {
				name  string
				query string
			}{
				{"the binding is the entity variable", `[:find ?f :where [?f :u/link ?w] [(get-else $ ?f :u/link :none) ?f]]`},
				{"another pattern binds the binding", `[:find ?f :where [?f :u/m ?x] [(get-else $ ?f :u/n :none) ?x]]`},
				{"a pattern names one variable twice", `[:find ?f :where [?f :u/link ?f]]`},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(db.Query(c.query))
					require.NoError(t, err)
					require.Len(t, tuples, 1, "only the entity whose value equals the bound one survives: %v", tuples)
					require.Equal(t, self, tuples[0][0])
				})
			}
		})
	}
}
