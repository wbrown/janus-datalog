package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// This file covers the one dispatch arm the cache-parity suite never reached:
// matchFromCache, taken when a pattern binds E **and** A as constants.
//
// Nothing else reaches it. The guard is `m.cache != nil && !isHistoryMode() &&
// e != nil && a != nil`, and e/a come from extractValue, which yields non-nil
// only for a Constant. So:
//
//   - `[?e :attr ?v]`, `[?e :attr "lit"]` — E is a variable; the cardinality
//     dispatch and the scan-all-entities arms handle these.
//   - `[:in $ ?e ... [?e :attr ?v]]` — E arrives through a binding relation,
//     which routes to matchWithBindingsFromCache, a different implementation
//     that declines bound-V vectors outright.
//   - history mode — bypassed by the guard.
//
// crdt_cache_matrix_test.go loops cacheTestModes twenty-eight times and reaches
// this arm zero times: every one of its queries binds E through `:in`, and its
// single #identity case (TestCacheMatrix_EConstantAUnbound) leaves A unbound,
// which fails `a != nil` by construction. ea_cache_bypass_test.go is `:in $ ?e`
// in all twelve. So the most common shape in the engine — its own comment calls
// matchFromCache "the default for every E-and-A-bound pattern" — had no parity
// test anywhere in the repository.
//
// Two bugs have now been found in it, five months apart, both of the same kind:
// an absence the streaming arm reads and CacheEntry cannot carry.
// BUG_CACHE_CARDINALIY_ONE_TOMBSTONE was a Remove tombstone read as a present
// value; BUG_CACHE_EMPTY_VECTOR_NEVER_SET is a never-set vector matching an
// empty-vector literal. The cases below are the arm's full branch set rather
// than those two, because enumerating from the code is what would have caught
// either before it shipped.

// constantPatternCase is one (fixture, query) pair aimed at matchFromCache.
//
// Every query binds E with an #id literal over the fixture's seed and A with a
// keyword, which is the only shape that enters the arm.
type constantPatternCase struct {
	name string
	// write puts the entity into the CRDT state under test.
	write func(t *testing.T, db *Database, e datalog.Identity)
	query string
}

// constantPatternSchema declares one attribute per cardinality so a case can
// aim at any of the arm's three branches by choosing an attribute.
func constantPatternSchema(t *testing.T) schema.SchemaProvider {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tag").Type(schema.TypeString).Many().Add().
		Attribute(":person/skill").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	return s
}

const constantPatternSeed = "cache-parity:subject"

// TestConstantPatternCacheParity requires the cache-resolved arm and the
// storage arms to answer identically, over every branch matchFromCache takes
// crossed with the CRDT states that reach it.
//
// The states matter as much as the branches: an entry holds a resolved value,
// and what distinguishes "never written" from "written and retracted" lives in
// the datoms, not in the resolution. Those are the pairs the cache cannot tell
// apart unless it is told to carry the difference, so each cardinality supplies
// both members of its pair — never-set against tombstoned, never-set against
// cleared-to-empty.
func TestConstantPatternCacheParity(t *testing.T) {
	name := datalog.NewKeyword(":person/name")
	tag := datalog.NewKeyword(":person/tag")
	skill := datalog.NewKeyword(":person/skill")

	// Fixture writers, named for the CRDT state they leave behind.
	neverSet := func(t *testing.T, db *Database, e datalog.Identity) {
		// The entity exists under a different attribute, so the attribute
		// under test is genuinely absent rather than the entity being unknown.
		tx := db.NewTransaction()
		require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/marker"), "present"))
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	commit := func(t *testing.T, db *Database, fn func(tx *Transaction) error) {
		t.Helper()
		tx := db.NewTransaction()
		require.NoError(t, fn(tx))
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	setName := func(t *testing.T, db *Database, e datalog.Identity) {
		neverSet(t, db, e)
		commit(t, db, func(tx *Transaction) error { return tx.Set(e, name, "Alice") })
	}
	supersededName := func(t *testing.T, db *Database, e datalog.Identity) {
		setName(t, db, e)
		commit(t, db, func(tx *Transaction) error { return tx.Set(e, name, "Alicia") })
	}
	tombstonedName := func(t *testing.T, db *Database, e datalog.Identity) {
		setName(t, db, e)
		commit(t, db, func(tx *Transaction) error { return tx.Remove(e, name, "Alice") })
	}
	twoTags := func(t *testing.T, db *Database, e datalog.Identity) {
		neverSet(t, db, e)
		commit(t, db, func(tx *Transaction) error {
			if err := tx.Add(e, tag, "dev"); err != nil {
				return err
			}
			return tx.Add(e, tag, "ops")
		})
	}
	allTagsRemoved := func(t *testing.T, db *Database, e datalog.Identity) {
		twoTags(t, db, e)
		commit(t, db, func(tx *Transaction) error {
			if err := tx.Remove(e, tag, "dev"); err != nil {
				return err
			}
			return tx.Remove(e, tag, "ops")
		})
	}
	twoSkills := func(t *testing.T, db *Database, e datalog.Identity) {
		neverSet(t, db, e)
		commit(t, db, func(tx *Transaction) error {
			return tx.Set(e, skill, []interface{}{"go", "sql"})
		})
	}
	clearedSkills := func(t *testing.T, db *Database, e datalog.Identity) {
		twoSkills(t, db, e)
		commit(t, db, func(tx *Transaction) error {
			return tx.Set(e, skill, []interface{}{})
		})
	}

	// A V-bound case binds E, A and V all as constants, so the pattern produces
	// no variable to find. The fourth position is what the repo already uses for
	// this — `[#id … :person/tag "dev" ?tx]` — and it keeps the pattern ground
	// in E, A and V, which is what routes it into matchFromCache. Whether the
	// tuple appears at all is the whole semantic content of a ground pattern; the
	// Tx it carries is normalised away by renderTuple below, because two
	// databases mint different ElementIDs for the same logical write.
	const (
		nameUnbound  = `[:find ?v :where [#id "cache-parity:subject" :person/name ?v]]`
		nameMatch    = `[:find ?tx :where [#id "cache-parity:subject" :person/name "Alice" ?tx]]`
		nameMismatch = `[:find ?tx :where [#id "cache-parity:subject" :person/name "Bob" ?tx]]`

		tagUnbound = `[:find ?v :where [#id "cache-parity:subject" :person/tag ?v]]`
		tagPresent = `[:find ?tx :where [#id "cache-parity:subject" :person/tag "dev" ?tx]]`
		tagAbsent  = `[:find ?tx :where [#id "cache-parity:subject" :person/tag "nope" ?tx]]`

		skillUnbound  = `[:find ?v :where [#id "cache-parity:subject" :person/skill ?v]]`
		skillEmpty    = `[:find ?tx :where [#id "cache-parity:subject" :person/skill [] ?tx]]`
		skillEqual    = `[:find ?tx :where [#id "cache-parity:subject" :person/skill ["go" "sql"] ?tx]]`
		skillNotEqual = `[:find ?tx :where [#id "cache-parity:subject" :person/skill ["go"] ?tx]]`
	)

	for _, tc := range []constantPatternCase{
		// CardinalityOne. entry.OneValue() is nil for both never-set and
		// tombstoned, and the tombstone pair is what BUG_CACHE_CARDINALIY_ONE_TOMBSTONE
		// was: ResolveLWW returned the highest-Tx datom's V without reading its Op.
		{"one/never set, V unbound", neverSet, nameUnbound},
		{"one/never set, V bound", neverSet, nameMatch},
		{"one/present, V unbound", setName, nameUnbound},
		{"one/present, V bound equal", setName, nameMatch},
		{"one/present, V bound unequal", setName, nameMismatch},
		{"one/tombstoned, V unbound", tombstonedName, nameUnbound},
		{"one/tombstoned, V bound to the removed value", tombstonedName, nameMatch},
		// Supersession is a different shape from a tombstone: the losing
		// datom is still an Add, so a V-bound prefix scan finds it and the
		// group it resolves contains only that datom.
		{"one/superseded, V unbound", supersededName, nameUnbound},
		{"one/superseded, V bound to the superseded value", supersededName, nameMatch},

		// CardinalityMany. An emptied set and a never-written one both leave
		// len(manySet) == 0, the same collapse as the vector pair below.
		{"many/never set, V unbound", neverSet, tagUnbound},
		{"many/never set, V bound", neverSet, tagPresent},
		{"many/members, V unbound", twoTags, tagUnbound},
		{"many/members, V bound present", twoTags, tagPresent},
		{"many/members, V bound absent", twoTags, tagAbsent},
		{"many/all removed, V unbound", allTagsRemoved, tagUnbound},
		{"many/all removed, V bound to a removed member", allTagsRemoved, tagPresent},

		// CardinalityVector. The never-set/cleared pair is
		// BUG_CACHE_EMPTY_VECTOR_NEVER_SET: the streaming arm reads
		// Stats.TotalElements and returns before comparing V, and CacheEntry
		// keeps no such fact.
		{"vector/never set, V unbound", neverSet, skillUnbound},
		{"vector/never set, V bound empty", neverSet, skillEmpty},
		{"vector/never set, V bound non-empty", neverSet, skillEqual},
		{"vector/populated, V unbound", twoSkills, skillUnbound},
		{"vector/populated, V bound equal", twoSkills, skillEqual},
		{"vector/populated, V bound unequal", twoSkills, skillNotEqual},
		{"vector/cleared to empty, V unbound", clearedSkills, skillUnbound},
		{"vector/cleared to empty, V bound empty", clearedSkills, skillEmpty},
		{"vector/cleared to empty, V bound non-empty", clearedSkills, skillEqual},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					assertCacheModesAgree(t, omode,
						func(t *testing.T, db *Database) {
							db.SetSchema(constantPatternSchema(t))
							tc.write(t, db, datalog.NewIdentity(constantPatternSeed))
						},
						func(t *testing.T, db *Database) interface{} {
							return sortedQueryTuples(t, db, tc.query)
						})
				})
			}
		})
	}
}

// TestConstantPatternCacheParitySchemaless is the same arm with no schema, where
// card falls back to CardinalityOne for every attribute.
//
// Separate from the table above because the fallback is a different route into
// the same branch — matchUnboundScan defaults `card` before consulting the
// schema at all — so a schema-driven table exercising CardinalityOne does not
// establish that the schemaless default reaches it the same way.
func TestConstantPatternCacheParitySchemaless(t *testing.T) {
	attr := datalog.NewKeyword(":loose/value")
	e := datalog.NewIdentity(constantPatternSeed)

	for _, tc := range []struct {
		name  string
		write func(t *testing.T, db *Database)
		query string
	}{
		{"never set", func(t *testing.T, db *Database) {},
			`[:find ?v :where [#id "cache-parity:subject" :loose/value ?v]]`},
		{"present, V unbound", func(t *testing.T, db *Database) {
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, attr, int64(7)))
			_, err := tx.Commit()
			require.NoError(t, err)
		}, `[:find ?v :where [#id "cache-parity:subject" :loose/value ?v]]`},
		{"present, V bound", func(t *testing.T, db *Database) {
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, attr, int64(7)))
			_, err := tx.Commit()
			require.NoError(t, err)
		}, `[:find ?tx :where [#id "cache-parity:subject" :loose/value 7 ?tx]]`},
		{"overwritten, V bound to the superseded value", func(t *testing.T, db *Database) {
			for _, v := range []int64{7, 9} {
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(e, attr, v))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
		}, `[:find ?tx :where [#id "cache-parity:subject" :loose/value 7 ?tx]]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, omode := range optimizerModes {
				t.Run(omode.name, func(t *testing.T) {
					assertCacheModesAgree(t, omode,
						func(t *testing.T, db *Database) { tc.write(t, db) },
						func(t *testing.T, db *Database) interface{} {
							return sortedQueryTuples(t, db, tc.query)
						})
				})
			}
		})
	}
}

// sortedQueryTuples runs a query and returns the relation's tuples, rendered
// and sorted.
//
// Sorted because a cardinality-many set resolves out of a Go map, so tuple
// order is not stable between two runs of the same query, let alone between two
// databases. Rendered because the comparison is over what the query answered.
func sortedQueryTuples(t *testing.T, db *Database, q string) []string {
	t.Helper()
	rel, err := db.Query(q)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)

	rendered := make([]string, 0, len(tuples))
	for _, tuple := range tuples {
		rendered = append(rendered, renderTuple(tuple))
	}
	sort.Strings(rendered)
	return rendered
}

// renderTuple renders one result tuple for cross-database comparison, standing
// an ElementID in for its own text.
//
// An ElementID is a Lamport counter paired with the replica that minted it, so
// two databases carrying the same logical writes still disagree on it, and a
// comparison that included it would fail on every Tx-binding query regardless
// of what the cache did. What such a query asserts is whether its ground
// pattern matched at all — whether the tuple is there — and that survives the
// substitution. Every value at every other tuple position is compared as it
// renders, so a wrong value or a wrong number of tuples still reds.
// DerefElementID rather than a type assertion on ElementID: a Tx binding
// arrives as either the value or a *ElementID depending on the path that built
// the tuple, so asserting only the value form normalises one path and prints
// the other's raw ID — which reads as a cache divergence when it is only two
// spellings of the same datum.
func renderTuple(tuple []interface{}) string {
	parts := make([]string, len(tuple))
	for i, v := range tuple {
		if _, isTx := datalog.DerefElementID(v); isTx {
			parts[i] = "<tx>"
			continue
		}
		parts[i] = fmt.Sprintf("%v", v)
	}
	return fmt.Sprintf("%v", parts)
}
