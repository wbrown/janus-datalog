package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// fallbackBranchFixture is the schema and facts the fallback-branch tests
// read: 120 entities each holding :t/n and :t/a, so one window of a fallback
// over them holds more than prefetchEntityThreshold entities and, on the
// prefetch legs, is prefetched.
func fallbackBranchFixture(t *testing.T) (*schema.Schema, []fact) {
	t.Helper()
	const subjectSize = 120
	s, err := schema.NewBuilder().
		Attribute(":t/n").Type(schema.TypeLong).One().Add().
		Attribute(":t/a").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	var facts []fact
	for i := 1; i <= subjectSize; i++ {
		e := named(fmt.Sprintf("e%d", i))
		facts = append(facts, fact{e, ":t/n", int64(i)}, fact{e, ":t/a", int64(i)})
	}
	return s, facts
}

// fallbackBranchCases are fallbacks whose first branch is one data pattern
// reading its fourth or fifth position, each with the default 0. In the
// latest view and an as-of view every tuple is an assertion: a constant false
// at the fifth position answers nothing and each subject tuple takes the
// default, a constant true, a blank or a variable the form provides answers
// the value, the variable bound true. A blank fourth position answers the
// value, a variable the form provides binds the datom's Tx, a variable bound
// to a Lamport no datom has takes the default, and one bound to the datom's
// own Tx answers the value.
var fallbackBranchCases = []struct {
	name string
	text string
}{
	{"fifth position a constant false", `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v _ false] [(ground 0) ?v])]`},
	{"fifth position a constant true", `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v _ true] [(ground 0) ?v])]`},
	{"fifth position a blank", `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v _ _] [(ground 0) ?v])]`},
	{"fifth position a variable bound by an earlier clause to false", `[:find ?e ?v :where [(ground false) ?added] [?e :t/n _] (or-default-join [[?e ?added] ?v] [?e :t/a ?v _ ?added] [(ground 0) ?v])]`},
	{"fifth position a variable the form provides", `[:find ?e ?v ?added :where [?e :t/n _] (or-default-join [[?e] ?v ?added] [?e :t/a ?v _ ?added] (and [(ground 0) ?v] [(ground false) ?added]))]`},
	{"fourth position a blank", `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v _] [(ground 0) ?v])]`},
	{"fourth position a variable the form provides", `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v ?tx] [?e :t/a ?v ?tx] (and [(ground 0) ?v] [(ground 0) ?tx]))]`},
	{"fourth position bound by an earlier clause to a Lamport no datom has", `[:find ?e ?v :where [(ground 5) ?tx] [?e :t/n _] (or-default-join [[?e ?tx] ?v] [?e :t/a ?v ?tx] [(ground 0) ?v])]`},
	{"fourth position bound by an earlier clause to the datom's own Tx", `[:find ?e ?v :where [?e :t/n _] [?e :t/a _ ?tx] (or-default-join [[?e ?tx] ?v] [?e :t/a ?v ?tx] [(ground 0) ?v])]`},
	{"or-default, fifth position a constant false", `[:find ?e ?v :where [?e :t/n _] (or-default [?e :t/a ?v _ false] [(ground 0) ?v])]`},
}

// A fallback branch of one data pattern reading its fourth or fifth position
// answers on every leg what the reference leg answers, with the EA cache on
// and off, in the latest view and an as-of view. The arm that builds a
// fallback branch from the EA cache on the prefetch legs serves the pattern a
// lookup answers, [?e attr ?v], and these branches take the planned build,
// where the matcher decides every position.
func TestReviewFallbackBranchOfFiveElementsAnswersAlikeOnEveryLeg(t *testing.T) {
	s, facts := fallbackBranchFixture(t)
	answer := func(t *testing.T, mode optimizerMode, disableCache bool, asOf bool, text string) []string {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: disableCache})
		names := commitFacts(t, d, facts)
		view := d
		if asOf {
			point, err := d.store.MaxElementID()
			require.NoError(t, err)
			view = d.AsOf(point)
		}
		rel, err := view.Query(text)
		require.NoError(t, err, text)
		return renderedTuples(t, rel, names)
	}
	for _, c := range fallbackBranchCases {
		for _, asOf := range []bool{false, true} {
			viewName := "latest"
			if asOf {
				viewName = "as-of"
			}
			reference := answer(t, referenceMode, true, asOf, c.text)
			for _, mode := range legsFor(optimizerModes, prefetchOption) {
				for _, disable := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/disableCache=%v", c.name, viewName, mode.name, disable), func(t *testing.T) {
						assert.Equal(t, reference, answer(t, mode, disable, asOf, c.text), c.text)
					})
				}
			}
		}
	}
}
