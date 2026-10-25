package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The property contracts of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md
// §4.7 that a query through the database pins.

// A subject's key survives a fallback branch of one pattern on an attribute
// of cardinality one keyed by the subject's entity on a current handle, where
// the pattern answers at most one tuple per entity, and not under History,
// where the pattern answers every version. The subject is a rule invocation,
// whose relation is keyed by its symbols under either handle, so the History
// leg has a key whose loss it observes.
func TestSubjectKeySurvivesAFallbackPatternBranchOfCardinalityOne(t *testing.T) {
	declared := schema.NewBuilder().
		Attribute(":t/n").Type(schema.TypeLong).Add().
		Attribute(":t/a").Type(schema.TypeLong).Add().
		MustBuild()
	e1, e2 := named("e1"), named("e2")
	facts := []fact{
		{e1, ":t/n", int64(1)}, {e1, ":t/a", int64(5)},
		{e2, ":t/n", int64(2)},
	}
	rules := mustRuleSet(t, `[[(node ?e ?n) [?e :t/n ?n]]]`)
	subjectQuery := `[:find ?e ?n :in $ % :where (node ?e ?n)]`
	formQuery := `[:find ?e ?n ?v :in $ % :where (node ?e ?n) (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared})
			names := commitFacts(t, d, facts)
			handles := []struct {
				name     string
				db       *Database
				survives bool
			}{
				{"current", d, true},
				{"history", d.History(), false},
			}
			for _, handle := range handles {
				subject, err := handle.db.Query(subjectQuery, rules)
				require.NoError(t, err, handle.name)
				subjectKeys := subject.Properties().Keys
				require.NotEmpty(t, subjectKeys, "%s: the subject carries a key for the contract to be testable", handle.name)

				rel, err := handle.db.Query(formQuery, rules)
				require.NoError(t, err, handle.name)
				require.ElementsMatch(t, []string{"e1 1 5", "e2 2 0"}, renderedTuples(t, rel, names), handle.name)
				for _, key := range subjectKeys {
					require.Equal(t, handle.survives, hasKey(rel.Properties().Keys, key),
						"%s: the subject's key %v among the result's keys %v", handle.name, key, rel.Properties().Keys)
				}
			}
		})
	}
}
