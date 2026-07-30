package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestVValidationReportsWhatItsScansCost pins the completion event for the
// candidate-then-validate arm, across both branches it emits from.
//
// The arm declared four reporting fields and wrote one, and its Close closed
// two iterators and returned. So a query resolved by this strategy showed no
// scan and no cost in a trace — and it is the deepest read the engine makes:
// one candidate scan per binding tuple, plus a validation lookup per candidate,
// or a claimant walk when the attribute is unique. A path that reports nothing
// is indistinguishable from one that scanned nothing.
//
// Both branches are covered because they count in different places. The
// candidate loop increments as it validates; the unique short-circuit returns a
// tuple without going near that loop, so an increment only there would report
// every unique-attribute binding as matching nothing.
func TestVValidationReportsWhatItsScansCost(t *testing.T) {
	name := datalog.NewKeyword(":person/name")
	email := datalog.NewKeyword(":person/email")

	alice := datalog.NewIdentity("vvalidation:alice")
	carol := datalog.NewIdentity("vvalidation:carol")

	for _, tc := range []struct {
		branch string
		attr   datalog.Keyword
		boundV any
		// rejectsCandidate: the V-primary scan surfaced a datom whose (E, A)
		// winner carries a different value, so resolution produced more than
		// the pattern matched. That gap is why the strategy exists, and an arm
		// reporting only its matches would price the rejected scan at nothing.
		rejectsCandidate bool
		// wantScansOpened is one candidate scan per binding tuple on the
		// validate branch, and none on the unique one — that branch resolves a
		// claimant through a walk instead. Zero is the honest report there, and
		// a reader seeing it against positive intake knows which branch ran.
		wantScansOpened int
	}{
		{branch: "candidate then validate", attr: name, boundV: "Alice",
			rejectsCandidate: true, wantScansOpened: 1},
		{branch: "unique short-circuit", attr: email, boundV: "alice@example.com",
			wantScansOpened: 0},
	} {
		t.Run(tc.branch, func(t *testing.T) {
			// DisableCache, and not incidentally: a binding-driven pattern is
			// answered by matchWithBindingsFromCache before analyzeReuseStrategy
			// is consulted, so with the cache on this arm is not reached at all.
			// That arm reports nothing either, so reaching the
			// code under test means going around it.
			var events []annotations.Event
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:              t.TempDir(),
				DisableCache:      true,
				AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
			})
			require.NoError(t, err)
			defer db.Close()

			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
				Build()
			require.NoError(t, err)
			db.SetSchema(s)

			commit := func(fn func(tx *Transaction) error) {
				t.Helper()
				tx := db.NewTransaction()
				require.NoError(t, fn(tx))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			// alice holds both values. carol held the name and was renamed
			// away, so she is the candidate the V-primary scan finds and
			// validation must reject.
			commit(func(tx *Transaction) error { return tx.Set(alice, name, "Alice") })
			commit(func(tx *Transaction) error { return tx.Set(alice, email, "alice@example.com") })
			commit(func(tx *Transaction) error { return tx.Set(carol, name, "Alice") })
			commit(func(tx *Transaction) error { return tx.Set(carol, name, "Caroline") })

			// The binding relation supplies V, which is what selects this arm:
			// analyzeReuseStrategy reaches its candidate-then-validate branch at
			// `case 2: // V is bound` (matcher_strategy.go), so a binding over
			// the entity takes a join strategy and never comes near this code.
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: tc.attr},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			}
			bindings := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?v")},
				[]executor.Tuple{{tc.boundV}},
			)

			m := db.Matcher().(*PatternMatcher)
			rel, err := m.MatchWithConstraints(
				query.PatternQuery(pattern), executor.Relations{bindings}, nil)
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 1, "alice holds the value; carol was renamed away")

			complete := lastScanComplete(events, annotations.ScanVValidation)
			if complete == nil {
				// Naming what did report turns a wrong assumption about which
				// strategy the router picks into a readable failure rather than
				// a nil dereference.
				var saw []annotations.ScanStrategy
				for _, e := range events {
					if e.Name == annotations.StorageScanComplete {
						s, _ := e.Data[annotations.KeyStrategy].(annotations.ScanStrategy)
						saw = append(saw, s)
					}
				}
				t.Fatalf("this arm reported no completion; strategies that did: %v", saw)
			}

			require.Equal(t, tc.wantScansOpened, complete.Data[annotations.KeyScansOpened],
				"the arm owes the count of candidate scans it opened; it addresses "+
					"no single run, so this stands in a bound's place")
			require.NotContains(t, complete.Data, annotations.KeyIndex,
				"one candidate scan per binding: naming one index would name "+
					"whichever came last")

			scanned, ok := complete.Data[annotations.KeyDatomsScanned].(int)
			require.True(t, ok, "the completion must carry intake")
			require.Positive(t, scanned,
				"the scans read the index; zero intake would mean they did not, "+
					"which is what this arm reported before it reported at all")

			resolved, _ := complete.Data[annotations.KeyDatomsResolved].(int)
			matched, _ := complete.Data[annotations.KeyDatomsMatched].(int)
			require.GreaterOrEqual(t, scanned, resolved,
				"intake bounds what resolution can produce")
			require.Equal(t, 1, matched,
				"one tuple was emitted, so one match must be reported")

			if tc.rejectsCandidate {
				require.Greater(t, resolved, matched,
					"carol's superseded datom was surfaced as a candidate and "+
						"rejected; reporting only matches would price that scan at nothing")
			} else {
				require.Equal(t, matched, resolved,
					"the claimant walk resolved exactly the entity it emitted")
			}
		})
	}
}
