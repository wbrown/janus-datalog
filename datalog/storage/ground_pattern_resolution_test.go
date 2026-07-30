package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A V-bound pattern asks whether an attribute's *resolved* value is the one it
// names. The tests here assert the answer, where cache_constant_pattern_parity_test.go
// asserts only that the two cache modes give the same one.
//
// Both shapes are needed and neither substitutes for the other. Parity cannot be
// weakened by an expectation authored against whichever mode the author ran, but
// it is satisfied by two arms that are wrong together — which is the state a
// resolution defect present on both paths leaves behind. An absolute expectation
// catches that and cannot catch a divergence the author did not think to probe.
//
// Every case runs under both cache modes, because the arm reached is decided by
// configuration as well as by query shape.

// commitGroundPattern applies one transaction and requires it to land.
func commitGroundPattern(t *testing.T, db *Database, fn func(tx *Transaction) error) {
	t.Helper()
	tx := db.NewTransaction()
	require.NoError(t, fn(tx))
	_, err := tx.Commit()
	require.NoError(t, err)
}

// TestGroundVectorPatternDistinguishesClearedFromNeverSet pins the vector half
// of the absence semantics: a vector that was set and then emptied holds the
// value `[]`, and a vector that was never set holds nothing.
//
// The two states are distinguishable in storage — the cleared one has datoms,
// the never-set one has none — so the read paths are required to distinguish
// them. The parity table covers the same two states and passes, which
// establishes that both arms answer alike and nothing about whether the answer
// they share is the one the semantics require.
func TestGroundVectorPatternDistinguishesClearedFromNeverSet(t *testing.T) {
	skill := datalog.NewKeyword(":person/skill")
	e := datalog.NewIdentity(constantPatternSeed)

	// The marker keeps the entity present under some attribute, so a miss on
	// :person/skill is the attribute's absence and not the entity's.
	neverSet := func(t *testing.T, db *Database) {
		commitGroundPattern(t, db, func(tx *Transaction) error {
			return tx.Add(e, datalog.NewKeyword(":person/marker"), "present")
		})
	}
	clearedToEmpty := func(t *testing.T, db *Database) {
		neverSet(t, db)
		commitGroundPattern(t, db, func(tx *Transaction) error {
			return tx.Set(e, skill, []interface{}{"go", "sql"})
		})
		commitGroundPattern(t, db, func(tx *Transaction) error {
			return tx.Set(e, skill, []interface{}{})
		})
	}

	const (
		boundEmpty = `[:find ?tx :where [#id "cache-parity:subject" :person/skill [] ?tx]]`
		unbound    = `[:find ?v :where [#id "cache-parity:subject" :person/skill ?v]]`
	)

	for _, tc := range []struct {
		name  string
		write func(t *testing.T, db *Database)
		query string
		want  int
		// emptyVectorBinding requires the single tuple's value to be the
		// empty vector, which is what distinguishes "bound []" from "bound
		// something else" on the V-unbound query.
		emptyVectorBinding bool
	}{
		{"never set does not match an empty-vector literal", neverSet, boundEmpty, 0, false},
		{"cleared to empty matches an empty-vector literal", clearedToEmpty, boundEmpty, 1, false},
		{"never set binds nothing", neverSet, unbound, 0, false},
		{"cleared to empty binds the empty vector", clearedToEmpty, unbound, 1, true},
	} {
		for _, mode := range cacheTestModes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				db, cleanup := createCacheTestDB(t, mode.disableCache, nil)
				defer cleanup()
				db.SetSchema(constantPatternSchema(t))
				tc.write(t, db)

				rel, err := db.Query(tc.query)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(rel, nil)
				require.NoError(t, err)
				require.Len(t, tuples, tc.want)

				if tc.emptyVectorBinding {
					require.True(t, datalog.ValuesEqual(tuples[0][0], []interface{}{}),
						"a cleared vector resolves to the empty vector, got %#v", tuples[0][0])
				}
			})
		}
	}
}

// TestClearedVectorBindsEmptyVectorNotNil pins that a cleared vector binds the
// empty vector for every element type, not only the ones typedVector converts.
//
// nil is not a datalog value. It has no case in ValuesEqual or hashValue, both
// of which panic on anything outside the closed domain, so a nil reaching a
// tuple's value position is not a wrong answer here — it is a crash at whatever
// join or dedup touches that tuple next, arbitrarily far away.
//
// typedVector builds a typed empty slice for string, long, double and boolean
// and returns its input unchanged for everything else. A cleared vector's input
// is a nil slice, so ref, instant, keyword, symbol and bytes vectors all reached
// the tuple carrying nil once the empty-vector early return was removed.
func TestClearedVectorBindsEmptyVectorNotNil(t *testing.T) {
	refs := datalog.NewKeyword(":doc/refs")
	e := datalog.NewIdentity(constantPatternSeed)

	refSchema := func(t *testing.T) schema.SchemaProvider {
		t.Helper()
		s, err := schema.NewBuilder().
			Attribute(":doc/refs").Type(schema.TypeRef).Vector().Add().
			Build()
		require.NoError(t, err)
		return s
	}

	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache, nil)
			defer cleanup()
			db.SetSchema(refSchema(t))

			commitGroundPattern(t, db, func(tx *Transaction) error {
				return tx.Set(e, refs, []interface{}{
					datalog.NewIdentity("doc:one"),
					datalog.NewIdentity("doc:two"),
				})
			})
			commitGroundPattern(t, db, func(tx *Transaction) error {
				return tx.Set(e, refs, []interface{}{})
			})

			rel, err := db.Query(`[:find ?v :where [#id "cache-parity:subject" :doc/refs ?v]]`)
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 1, "a cleared vector is a value and binds")

			require.NotNil(t, tuples[0][0],
				"nil is not a datalog value and must never occupy a tuple position")
			require.True(t, datalog.ValuesEqual(tuples[0][0], []interface{}{}),
				"a cleared vector resolves to the empty vector, got %#v", tuples[0][0])
		})
	}
}

// TestVBoundScanWithUnboundEntityRejectsSupersededValue covers the V-bound arm
// that binds A and V while leaving E free.
//
// It is the same defect shape as BUG_VBOUND_PREFIX_SCAN_MATCHES_SUPERSEDED_VALUE
// one arm over: chooseIndex answers AVET with the prefix [A, V], which narrows
// the scan to the datoms carrying the queried value, and for a superseded value
// those are exactly the losers. The E-bound arm was fixed by dropping V from the
// prefix; this arm cannot take that fix, because without V the scan is the whole
// attribute. It needs the candidate-then-validate shape instead.
//
// Stated as a question a caller would ask: "which entities are currently named
// Alice?" must not answer with an entity that was renamed away from Alice.
func TestVBoundScanWithUnboundEntityRejectsSupersededValue(t *testing.T) {
	name := datalog.NewKeyword(":person/name")
	e := datalog.NewIdentity(constantPatternSeed)

	renamed := func(t *testing.T, db *Database) {
		commitGroundPattern(t, db, func(tx *Transaction) error { return tx.Set(e, name, "Alice") })
		commitGroundPattern(t, db, func(tx *Transaction) error { return tx.Set(e, name, "Alicia") })
	}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{"the superseded value names no entity",
			`[:find ?e :where [?e :person/name "Alice"]]`, 0},
		{"the current value names its entity",
			`[:find ?e :where [?e :person/name "Alicia"]]`, 1},
	} {
		for _, mode := range cacheTestModes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				db, cleanup := createCacheTestDB(t, mode.disableCache, nil)
				defer cleanup()
				db.SetSchema(constantPatternSchema(t))
				renamed(t, db)

				rel, err := db.Query(tc.query)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(rel, nil)
				require.NoError(t, err)
				require.Len(t, tuples, tc.want)
			})
		}
	}
}
