package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// magicSetsEvents counts the rules/magic-sets events a database's handler
// receives, each with its adornment.
type magicSetsEvents struct {
	mu    sync.Mutex
	bound [][]bool
}

func (m *magicSetsEvents) handle(e annotations.Event) {
	if e.Name != annotations.RulesMagicSets {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bound = append(m.bound, append([]bool(nil), e.Data["adornment"].([]bool)...))
}

func (m *magicSetsEvents) take() [][]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	made := m.bound
	m.bound = nil
	return made
}

// A query run under a rule set reports the rewrites its own evaluation makes
// (annotations.RulesMagicSets), whichever equal set planned the plan the
// cache serves it, whether the set is passed parsed or as its text: a free
// invocation reports none, at the top level, inside an or-join branch and
// inside a subquery; an invocation a pattern or an input binds reports its
// adornment once; and a free invocation of a predicate reaching one with
// required positions, rewritten with its head all free, reports that
// rewrite once.
func TestPlanCache_ARuleSetEqualToThePlanningOneReportsItsOwnRewrites(t *testing.T) {
	// reachingRules derives hop through step, which requires its first
	// position.
	const reachingRules = `[[(step [?n] ?m) [?n :edge ?m]]
	                        [(hop ?a ?c) [?a :edge ?b] (step ?b ?c)]]`
	passings := []struct {
		name string
		pass func(t *testing.T, rules string) interface{}
	}{
		{"parsed", func(t *testing.T, rules string) interface{} { return mustRuleSet(t, rules) }},
		{"as text", func(_ *testing.T, rules string) interface{} { return rules }},
	}
	cases := []struct {
		name   string
		rules  string
		query  string
		inputs func() []interface{}
		want   [][]bool
	}{
		{"a free invocation", closureRules, closureQuery, func() []interface{} { return nil }, nil},
		{"a free invocation inside an or-join branch", closureRules,
			`[:find ?x ?y :in $ % :where (or-join [?x ?y] (reach ?x ?y) [?x :edge ?y])]`,
			func() []interface{} { return nil }, nil},
		{"a free invocation inside a subquery", closureRules,
			`[:find ?x ?y :in $ % :where [(q [:find ?x ?y :in $ % :where (reach ?x ?y)] $ %) [[?x ?y] ...]]]`,
			func() []interface{} { return nil }, nil},
		{"an invocation a pattern binds", closureRules, `[:find ?x ?y :in $ % :where [?x :edge _] (reach ?x ?y)]`,
			func() []interface{} { return nil }, [][]bool{{true, false}}},
		{"a bound invocation", closureRules, `[:find ?y :in $ % ?start :where (reach ?start ?y)]`,
			func() []interface{} { return []interface{}{nodeID("n1")} }, [][]bool{{true, false}}},
		{"a free invocation reaching a required predicate", reachingRules, `[:find ?x ?y :in $ % :where (hop ?x ?y)]`,
			func() []interface{} { return nil }, [][]bool{{false, false}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &magicSetsEvents{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: events.handle})
			writeGraph(t, d, chain(4))
			for _, tc := range cases {
				for _, first := range passings {
					for _, second := range passings {
						t.Run(fmt.Sprintf("%s, planned under a set %s, run under a set %s", tc.name, first.name, second.name), func(t *testing.T) {
							d.ClearPlanCache()
							require.NoError(t, runToEnd(d, tc.query, append([]interface{}{first.pass(t, tc.rules)}, tc.inputs()...)...))
							require.Equal(t, tc.want, events.take(), "the run that planned the query")
							rel, err := d.Query(tc.query, append([]interface{}{second.pass(t, tc.rules)}, tc.inputs()...)...)
							require.NoError(t, err)
							require.NoError(t, executor.ForEach(rel, func(executor.Tuple) error { return nil }))
							require.Equal(t, tc.want, events.take(),
								"%s under a set %s, the plan cache holding the plan made under a set %s", tc.query, second.name, first.name)
						})
					}
				}
			}
		})
	}
}
