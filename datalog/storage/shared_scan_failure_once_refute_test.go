package storage

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// A query answers each failure of each scan it opened once, through every
// public reader, on every leg, as TestAScanSharedRelationAnswersEachFailureOfItsScanOnce
// states for a scan read at one site. A shared scan read to its end answers
// its close failure to the reader reaching the end, and the query's end
// answers nothing for it; a shared scan whose readers stop early, under a
// limit, answers it at the query's end alone, on the Query route and through
// the database's executor alike.
func TestRefuteASharedScanAnswersEachFailureOnce(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	rank := datalog.NewKeyword(":item/rank")
	rules := `[[(scored ?e ?v) [?e :item/score ?v]]]`
	cases := []struct {
		name     string
		text     string
		rules    bool
		failRead bool
	}{
		{"not-join over the outer fingerprint, close fails", `[:find ?e :where [?e :item/score ?v] (not-join [?v] [?f :item/score ?w] [(> ?w ?v)])]`, false, false},
		{"not-join over the outer fingerprint, read fails", `[:find ?e :where [?e :item/score ?v] (not-join [?v] [?f :item/score ?w] [(> ?w ?v)])]`, false, true},
		{"aggregating subquery, close fails", `[:find ?e :where [?e :item/score ?v] [(q [:find (max ?w) :in $ :where [?f :item/score ?w]] $) [[?m]]] [(<= ?v ?m)]]`, false, false},
		{"not over a rule invocation, close fails", `[:find ?e :in $ % :where [?e :item/score ?v] (not-join [?v] (scored ?f ?w) [(> ?w ?v)])]`, true, false},
		{"correlated count subquery, close fails", `[:find ?e ?n :where [?e :item/score ?v] [(q [:find (count ?f) :in $ ?v :where [?f :item/score ?w] [(>= ?w ?v)]] $ ?v) [[?n]]]]`, false, false},
		{"two sites joined on the value, limited, close fails", `[:find ?e :where [?e :item/score ?v] [?f :item/score ?v] :limit 1]`, false, false},
		{"two fingerprints at two sites each, limited, close fails", `[:find ?e :where [?e :item/score ?v] [?f :item/score ?v] [?e :item/rank ?r] [?g :item/rank ?r] :limit 1]`, false, false},
	}
	readers := []struct {
		name   string
		answer func(db *Database, committed datalog.ElementID, text string, rules interface{}) error
	}{
		{"Query collected", func(db *Database, committed datalog.ElementID, text string, rules interface{}) error {
			if rules != nil {
				_, err := executor.CollectTuples(db.Query(text, rules))
				return err
			}
			_, err := executor.CollectTuples(db.Query(text))
			return err
		}},
		{"Query WithContext collected", func(db *Database, committed datalog.ElementID, text string, rules interface{}) error {
			if rules != nil {
				_, err := executor.CollectTuples(db.Query(text, rules, WithContext(context.Background())))
				return err
			}
			_, err := executor.CollectTuples(db.Query(text, WithContext(context.Background())))
			return err
		}},
		{"QueryOneInto", func(db *Database, committed datalog.ElementID, text string, rules interface{}) error {
			var answer struct {
				E datalog.Identity `datalog:"?e"`
			}
			if rules != nil {
				_, err := db.QueryOneInto(&answer, text, rules)
				return err
			}
			_, err := db.QueryOneInto(&answer, text)
			return err
		}},
		{"AsOf collected", func(db *Database, committed datalog.ElementID, text string, rules interface{}) error {
			tx := committed
			if rules != nil {
				_, err := executor.CollectTuples(db.AsOf(tx).Query(text, rules))
				return err
			}
			_, err := executor.CollectTuples(db.AsOf(tx).Query(text))
			return err
		}},
		{"History collected", func(db *Database, committed datalog.ElementID, text string, rules interface{}) error {
			if rules != nil {
				_, err := executor.CollectTuples(db.History().Query(text, rules))
				return err
			}
			_, err := executor.CollectTuples(db.History().Query(text))
			return err
		}},
		{"NewExecutor Execute collected", func(db *Database, committed datalog.ElementID, text string, rules interface{}) error {
			if rules != nil {
				return nil
			}
			q, err := parser.ParseQuery(text)
			if err != nil {
				return err
			}
			_, err = executor.CollectTuples(db.NewExecutor().Execute(q))
			return err
		}},
	}
	for _, mode := range legsFor(optimizerModes, allOptions...) {
		for _, c := range cases {
			for _, reader := range readers {
				t.Run(mode.name+"/"+c.name+"/"+reader.name, func(t *testing.T) {
					var failing, failRead atomic.Bool
					var scans atomic.Int32
					hits := 0
					options := mode.plannerOptions()
					options.Handler = func(e annotations.Event) {
						if e.Name == annotations.ScanSharingCacheHit {
							hits++
						}
					}
					db, err := NewDatabaseWithOptions(DatabaseOptions{
						Store:          scanCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failRead: &failRead, scans: &scans},
						PlannerOptions: &options,
					})
					require.NoError(t, err)
					defer func() { require.NoError(t, db.Close()) }()
					tx := db.NewTransaction()
					require.NoError(t, tx.Add(datalog.NewIdentity("refute-failure:first"), score, int64(3)))
					require.NoError(t, tx.Add(datalog.NewIdentity("refute-failure:first"), rank, int64(1)))
					require.NoError(t, tx.Add(datalog.NewIdentity("refute-failure:second"), score, int64(5)))
					require.NoError(t, tx.Add(datalog.NewIdentity("refute-failure:second"), rank, int64(2)))
					committed, err := tx.Commit()
					require.NoError(t, err)

					var ruleInput interface{}
					if c.rules {
						ruleInput = rules
					}
					failRead.Store(c.failRead)
					failing.Store(true)
					err = reader.answer(db, committed, c.text, ruleInput)
					failing.Store(false)
					t.Logf("scans opened %d, shared-scan hits %d, close failures answered %d, error: %v", scans.Load(), hits, occurrences(err, errStoreClose), err)
					if scans.Load() == 0 {
						return
					}
					require.ErrorIs(t, err, errStoreClose, "the scan's close failure reaches the caller")
					require.Equal(t, int(scans.Load()), occurrences(err, errStoreClose), "each failed close is answered once: %v", err)
					if c.failRead {
						require.Equal(t, int(scans.Load()), occurrences(err, errStoreScan), "each failed read is answered once: %v", err)
					}
				})
			}
		}
	}
}
