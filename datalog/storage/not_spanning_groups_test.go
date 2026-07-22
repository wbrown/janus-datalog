package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// Reproducers for BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP: a not
// clause whose variables span disjoint relation groups must not anti-join
// each group separately — that wipes the group carrying only the correlate
// with a partially-bound body, and turns the entity-side anti-join
// existential ("has any value") instead of correlated. The correct execution
// joins the spanning groups (the clause's correlation is the connector,
// exactly as a bridging predicate's theta-join), anti-joins once on the full
// key set, and replaces the spanning groups with the filtered join.

// notSpanningFixture writes three "thing" entities: one flagged with the
// probe's value ("hot"), one flagged differently ("cold"), one unflagged.
// The probe entity carries the value the NOT must correlate against.
func notSpanningFixture(t *testing.T, d *Database) (flagged, unflagged, otherFlag datalog.Identity) {
	t.Helper()
	probe := datalog.NewIdentity("probe:1")
	flagged = datalog.NewIdentity("entity:flagged")
	unflagged = datalog.NewIdentity("entity:unflagged")
	otherFlag = datalog.NewIdentity("entity:other-flag")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(probe, datalog.NewKeyword(":probe/flag"), "hot"))
	for _, e := range []datalog.Identity{flagged, unflagged, otherFlag} {
		require.NoError(t, tx.Add(e, datalog.NewKeyword(":entity/kind"), "thing"))
	}
	require.NoError(t, tx.Add(flagged, datalog.NewKeyword(":entity/flag"), "hot"))
	require.NoError(t, tx.Add(otherFlag, datalog.NewKeyword(":entity/flag"), "cold"))
	_, err := tx.Commit()
	require.NoError(t, err)
	return flagged, unflagged, otherFlag
}

// TestNotSpanningDisjointGroups pins the bare-not shape at top level: ?outer
// is WHERE-bound in its own group, and the NOT must exclude exactly the
// entities flagged with the outer value — not every entity with any flag.
func TestNotSpanningDisjointGroups(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :where
			                      [_ :probe/flag ?outer]
			                      [?e :entity/kind "thing"]
			                      (not [?e :entity/flag ?outer])]`)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"the NOT must correlate ?outer across groups: only the hot-flagged entity is excluded")
		})
	}
}

// TestNotJoinSpanningDisjointGroups pins the explicit-header form of the same
// shape: not-join declares both variables, whose bindings live in two
// disjoint groups — the declared header selects and bridges them (the
// original defect errored per-group instead).
func TestNotJoinSpanningDisjointGroups(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :where
			                      [_ :probe/flag ?outer]
			                      [?e :entity/kind "thing"]
			                      (not-join [?e ?outer]
			                        [?e :entity/flag ?outer])]`)
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"a not-join whose declared header spans disjoint groups must bridge them")
		})
	}
}

// TestNotBodyConsumesInBoundParameter pins the environment leg of the class:
// the correlate reaches the query through :in. Pattern-consumed, it rides
// the bound-input relation as a column in its own group; the NOT's bridging
// must pull that group in exactly as for a WHERE-bound correlate.
func TestNotBodyConsumesInBoundParameter(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode)
			_, unflagged, otherFlag := notSpanningFixture(t, d)

			rel, err := d.Query(`[:find ?e
			                      :in $ ?flag
			                      :where
			                      [?e :entity/kind "thing"]
			                      (not [?e :entity/flag ?flag])]`,
				"hot")
			require.NoError(t, err)
			got := collectEntityResults(t, rel)
			require.Equal(t, map[string]bool{
				unflagged.String(): true,
				otherFlag.String(): true,
			}, got,
				"an :in-bound correlate consumed by a NOT body must correlate, not turn existential")
		})
	}
}
