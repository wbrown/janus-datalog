package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Reproductions for docs/bugs/BUG_VBOUND_BYTES_VALIDATION_PANIC.md
//
// V-bound queries on a cardinality-one attribute route through
// validatingVBoundIterator.validateCandidate, which compares the EATV winner
// to the bound value with raw interface ==:
//
//	matches := winner.V == it.currentBoundV
//
// For []byte both sides are dynamic type []uint8, which is not comparable, so
// the comparison panics with "comparing uncomparable type []uint8". The fix is
// to use datalog.ValuesEqual / m.valuesEqual.

func newBytesOneDB(t *testing.T, mode optimizerMode) (*Database, datalog.Identity, datalog.Keyword) {
	t.Helper()
	// Cardinality-one, NOT unique: forces the candidate+validate path
	// (validateCandidate), not the unique (A,V)-LWW short-circuit.
	s, err := schema.NewBuilder().
		Attribute(":doc/hash").Type(schema.TypeBytes).One().Add().
		Build()
	require.NoError(t, err)
	opts := mode.plannerOptions()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		Schema:         s,
		PlannerOptions: &opts,
	})
	require.NoError(t, err)
	return db, datalog.NewIdentity("doc-1"), datalog.NewKeyword(":doc/hash")
}

func queryByHash(t *testing.T, db *Database, v []byte) []datalog.Identity {
	t.Helper()
	tuples, err := executor.CollectTuples(db.Query(`[:find ?e :in $ ?v :where [?e :doc/hash ?v]]`, v))
	require.NoError(t, err)
	out := make([]datalog.Identity, 0, len(tuples))
	for _, tuple := range tuples {
		id, ok := tuple[0].(datalog.Identity)
		require.Truef(t, ok, "expected Identity, got %T", tuple[0])
		out = append(out, id)
	}
	return out
}

// TestVBoundCardinalityOneBytes_NoPanic: a V-bound query on a byte attribute
// must not panic and must return the matching entity.
func TestVBoundCardinalityOneBytes_NoPanic(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, e, a := newBytesOneDB(t, mode)
			defer db.Close()

			v := []byte{0xde, 0xad, 0xbe, 0xef}
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, v))
			_, err := tx.Commit()
			require.NoError(t, err)

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("V-bound []byte query panicked (bug reproduced): %v", r)
				}
			}()

			got := queryByHash(t, db, v)
			require.Len(t, got, 1)
			require.True(t, got[0].Equal(e))
		})
	}
}

// TestVBoundCardinalityOneBytes_MatchesByContent: a different slice instance
// with identical content must still match (byte-content equality).
func TestVBoundCardinalityOneBytes_MatchesByContent(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, e, a := newBytesOneDB(t, mode)
			defer db.Close()

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, []byte{0xca, 0xfe, 0xba, 0xbe}))
			_, err := tx.Commit()
			require.NoError(t, err)

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("V-bound []byte content match panicked (bug reproduced): %v", r)
				}
			}()

			// Distinct slice, same content as stored.
			got := queryByHash(t, db, []byte{0xca, 0xfe, 0xba, 0xbe})
			require.Len(t, got, 1)
			require.True(t, got[0].Equal(e))
		})
	}
}

// TestVBoundCardinalityOneBytes_RejectsStaleCandidate: after overwrite, the old
// value still has an AVET candidate entry, but the EATV winner differs. The
// candidate must be rejected (and rejection must compare by content, not panic).
func TestVBoundCardinalityOneBytes_RejectsStaleCandidate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, e, a := newBytesOneDB(t, mode)
			defer db.Close()

			v1 := []byte{0x01, 0x01}
			v2 := []byte{0x02, 0x02}

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, v1))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(e, a, v2)) // v1's AVET entry remains; LWW supersedes it
			_, err = tx.Commit()
			require.NoError(t, err)

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("V-bound stale-candidate validation panicked (bug reproduced): %v", r)
				}
			}()

			require.Empty(t, queryByHash(t, db, v1), "stale value must not match the current winner")
		})
	}
}

// TestVBoundCardinalityOneBytes_AfterOverwrite: querying the current value after
// an overwrite returns the entity.
func TestVBoundCardinalityOneBytes_AfterOverwrite(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, e, a := newBytesOneDB(t, mode)
			defer db.Close()

			v1 := []byte{0x0a}
			v2 := []byte{0x0b}

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, v1))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(e, a, v2))
			_, err = tx.Commit()
			require.NoError(t, err)

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("V-bound post-overwrite query panicked (bug reproduced): %v", r)
				}
			}()

			got := queryByHash(t, db, v2)
			require.Len(t, got, 1)
			require.True(t, got[0].Equal(e))
		})
	}
}

// TestVBoundCardinalityOneBytes_AfterRemove: after a tombstone the value must
// not match. (The Op==Remove check precedes the == comparison, so this is a
// contract test that complements the panic reproductions above.)
func TestVBoundCardinalityOneBytes_AfterRemove(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, e, a := newBytesOneDB(t, mode)
			defer db.Close()

			v := []byte{0xfe, 0xed}

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, v))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Remove(e, a, v))
			_, err = tx.Commit()
			require.NoError(t, err)

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("V-bound post-remove query panicked (bug reproduced): %v", r)
				}
			}()

			require.Empty(t, queryByHash(t, db, v), "removed value must not match")
		})
	}
}

// TestVBoundCardinalityOneBytes_ValidationTrail captures the v-validation
// annotation trail so the failure under -race can be diagnosed: it shows whether
// the candidate scan found a candidate and what the EATV winner comparison saw.
func TestVBoundCardinalityOneBytes_ValidationTrail(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, e, a := newBytesOneDB(t, mode)
			defer db.Close()

			var mu sync.Mutex
			var trail []string
			db.AnnotationHandler = func(ev annotations.Event) {
				if strings.HasPrefix(ev.Name, "v-validation/") ||
					strings.Contains(ev.Name, "join") ||
					strings.Contains(ev.Name, "collapse") ||
					strings.Contains(ev.Name, "phase") ||
					strings.Contains(ev.Name, "pattern") {
					mu.Lock()
					trail = append(trail, fmt.Sprintf("%s %v", ev.Name, ev.Data))
					mu.Unlock()
				}
			}

			v := []byte{0xde, 0xad, 0xbe, 0xef}
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, v))
			_, err := tx.Commit()
			require.NoError(t, err)

			tuples, err := executor.CollectTuples(db.Query(`[:find ?e :in $ ?v :where [?e :doc/hash ?v]]`, v))
			require.NoError(t, err)

			mu.Lock()
			for _, s := range trail {
				t.Log(s)
			}
			mu.Unlock()

			require.Len(t, tuples, 1)
		})
	}
}

// TestVBoundCardinalityOneBytes_DirectMatcher drives the matcher directly with
// ?v bound, bypassing the executor's join of the pattern result with the :in
// input. This isolates matcher behaviour from the join: if this returns the
// entity under -race but the full query does not, the join is the culprit.
// It drives the matcher rather than the executor, so the algebra optimizer is
// not on its path: it pins one mode explicitly instead of looping the axis.
func TestVBoundCardinalityOneBytes_DirectMatcher(t *testing.T) {
	db, e, a := newBytesOneDB(t, optimizerMode{name: "algebra_off", algebra: false})
	defer db.Close()

	v := []byte{0xde, 0xad, 0xbe, 0xef}
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v))
	_, err := tx.Commit()
	require.NoError(t, err)

	q, err := parser.ParseQuery(`[:find ?e :in $ ?v :where [?e :doc/hash ?v]]`)
	require.NoError(t, err)
	pattern := q.Where[0].(*query.DataPattern)

	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?v")},
		[]executor.Tuple{{v}},
	)
	result, err := db.Matcher().Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
	require.NoError(t, err)

	it := result.Iterator()
	count := 0
	for it.Next() {
		count++
	}
	require.NoError(t, it.Error())
	it.Close()
	require.Equal(t, 1, count, "matcher must return the matching entity (independent of the executor join)")
}
