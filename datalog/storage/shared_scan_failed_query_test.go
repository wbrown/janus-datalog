package storage

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A query that fails after a shared scan's site matched, before any reader
// read the scan to its end, closes the scan at the failure, through Query and
// through the database's executor alike, on every leg: on a counting store
// opened equals closed, and the failure is the query's error. The or-join's
// subject and its branch read one fingerprint, so the scan is shared; the
// subject's first window pulls one tuple of it, and the branch's free rule
// invocation exceeds MaxDerivedTuples before the branch reads the scan.
func TestAQueryFailingWithASharedScanOpenClosesTheScan(t *testing.T) {
	text := `[:find ?e ?x :in $ % :where [?e :item/score ?v] (or-join [[?v] ?x] (and [?f :item/score ?w] [(> ?w ?v)] (reach ?g ?x) [(= ?x ?f)]))]`
	rules := `[[(reach ?a ?b) [?a :item/next ?b]] [(reach ?a ?c) [?a :item/next ?b] (reach ?b ?c)]]`
	entries := []struct {
		name string
		run  func(d *Database) error
	}{
		{"Query", func(d *Database) error {
			_, err := executor.CollectTuples(d.Query(text, rules))
			return err
		}},
		{"NewExecutor().ExecuteWithRelations under a caller's context", func(d *Database) error {
			q, err := parser.ParseQuery(text)
			if err != nil {
				return err
			}
			ruleSet, err := parser.ParseRules(rules)
			if err != nil {
				return err
			}
			_, err = executor.CollectTuples(d.NewExecutor().ExecuteWithRelations(executor.NewContext().WithRules(ruleSet), q, nil))
			return err
		}},
	}
	score := datalog.NewKeyword(":item/score")
	next := datalog.NewKeyword(":item/next")
	fill := func(t *testing.T, d *Database) {
		tx := d.NewTransaction()
		const chain = 4
		for i := 0; i < chain; i++ {
			item := datalog.NewIdentity("failing-query:" + strconv.Itoa(i))
			require.NoError(t, tx.Add(item, score, int64(i)))
			if i+1 < chain {
				require.NoError(t, tx.Add(item, next, datalog.NewIdentity("failing-query:"+strconv.Itoa(i+1))))
			}
		}
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	for _, mode := range legsFor(optimizerModes, allOptions...) {
		for _, entry := range entries {
			t.Run(mode.name+"/"+entry.name, func(t *testing.T) {
				var opened, closed atomic.Int32
				opts := mode.plannerOptions()
				opts.MaxDerivedTuples = 1
				d, err := NewDatabaseWithOptions(DatabaseOptions{
					Store:          scanTallyStore{Store: NewBadgerEmulatorStore(nil), opened: &opened, closed: &closed},
					PlannerOptions: &opts,
				})
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				fill(t, d)
				opened.Store(0)
				closed.Store(0)
				err = entry.run(d)
				t.Logf("scans opened %d, closed %d, error: %v", opened.Load(), closed.Load(), err)
				var limit *query.DerivedTupleLimitError
				require.ErrorAs(t, err, &limit, "the derived-tuple bound's failure is the query's error")
				require.NotZero(t, opened.Load(), "the query opened its scans before it failed")
				require.Equal(t, opened.Load(), closed.Load(), "every scan the query opened is closed at its failure")
			})
		}
	}
}
