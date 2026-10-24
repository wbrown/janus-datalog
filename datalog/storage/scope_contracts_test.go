package storage

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The contracts of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7 that a
// query through the database pins, each the subject of one test.

// A branch sees the required run and nothing else of the enclosing scope: a
// branch variable sharing an outer variable's name is the branch's own, so
// the branch over :t/a holds for an entity whatever its :t/n, and the
// entity's :t/n is answered beside it.
func TestOrJoinBranchSeesOnlyItsHeader(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(2)}},
		"e2": {{":t/n", int64(2)}, {":t/a", int64(1)}},
		"e3": {{":t/n", int64(3)}},
	}
	clauses := []string{`[?e :t/n ?x]`, `(or-join [?e] [?e :t/a ?x] [?e :t/b ?x])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?x", clauses, []string{"e1 1", "e2 2"})
		})
	}
}

// An invocation of a predicate with required positions inside a branch is
// bound by the branch's own clauses before it: the first branch binds ?n from
// the entity's :t/n and invokes succ on it, and the second answers the
// entity's :t/fixed.
func TestInvocationInABranchSeesTheBranchBindingsBeforeIt(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(5)}, {":t/fixed", int64(9)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?m :in $ % :where [?e :t/n _] (or-join [[?e] ?m] (and [?e :t/n ?n] (succ ?n ?m)) [?e :t/fixed ?m])]`,
				mustRuleSet(t, `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1 2", "e2 6", "e2 9"}, renderedRows(t, rel, names))
		})
	}
}

// An output the subject carries is joined with the branch's value, never
// superseded by it: an entity whose :t/a disagrees with its :t/q is dropped,
// and one with no branch value is dropped too.
func TestOrJoinOutputIsJoinedWithTheOuter(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/q", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/q", int64(2)}, {":t/a", int64(3)}},
		"e3": {{":t/q", int64(4)}},
	}
	clauses := []string{`[?e :t/q ?q]`, `(or-join [?e ?q] [?e :t/a ?q] [?e :t/b ?q])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?q", clauses, []string{"e1 1"})
		})
	}
}

// A subquery branch answers at most one tuple per subject tuple when its
// find is aggregates and variables the subject carries, whatever the
// binding's spelling: a tuple-spelled inner query answering every entity
// sharing the subject's :t/n fans out, so the fallback's result claims no
// key and the find projection onto ?e yields each entity once.
func TestFallbackOverAFanningTupleSpelledSubqueryClaimsNoKey(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(1)}},
		"e3": {{":t/n", int64(2)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e :where [?e :t/n _] (or-default-join [[?e] ?v] [(q [:find ?x :in $ ?e :where [?e :t/n ?n] [?x :t/n ?n]] $ ?e) [[?v]]] [(ground 0) ?v])]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1", "e2", "e3"}, renderedRows(t, rel, names))
		})
	}
}

// A ground under the relation form binds the relation over its vector of
// vectors (NESTED_SCOPES_AS_QUERIES.md goal 12): over the entities it pairs
// each with every inner vector, and an inner vector the entity's :t/n unifies
// with keeps that entity's tuple alone.
func TestRelationGroundBindsOneTuplePerInnerVector(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?a ?b",
				[]string{`[?e :t/n _]`, `[(ground [[1 2] [3 4]]) [[?a ?b]]]`},
				[]string{"e1 1 2", "e1 3 4", "e2 1 2", "e2 3 4"})
			everyOrder(t, d, names, "?e ?b",
				[]string{`[?e :t/n ?n]`, `[(ground [[1 2] [3 4]]) [[?n ?b]]]`},
				[]string{"e1 2", "e2 4"})
		})
	}
}

// A ground under the collection form binds the relation over its vector, one
// tuple per element: over the entities it pairs each with every element, and
// an element the entity's :t/n unifies with keeps that entity's tuple alone.
func TestCollectionGroundBindsOneTuplePerElement(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v",
				[]string{`[?e :t/n _]`, `[(ground [1 2 3]) [?v ...]]`},
				[]string{"e1 1", "e1 2", "e1 3", "e2 1", "e2 2", "e2 3"})
			everyOrder(t, d, names, "?e ?n",
				[]string{`[?e :t/n ?n]`, `[(ground [1 2 3]) [?n ...]]`},
				[]string{"e1 1", "e2 3"})
		})
	}
}

// An output the environment binds is joined with the environment's tuple:
// with ?q given as 1, the entity whose :t/a is 1 is kept, and the one whose
// :t/a is 3 is dropped.
func TestOutputTheEnvironmentBindsIsJoinedWithItsTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/a", int64(3)}},
		"e3": {{":t/n", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e :in $ ?q :where [?e :t/n _] (or-join [?e ?q] [?e :t/a ?q] [?e :t/b ?q])]`, int64(1))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1"}, renderedRows(t, rel, names))
		})
	}
}

// A fallback branch providing a required variable the environment alone
// binds, without reading it, answers as the per-tuple path does: the run
// carries the input's value in every tuple, so the branch grounding ?z to 2
// meets the given ?z of 1 and keeps no tuple, and the default answers.
func TestFallbackBranchProvidingAnEnvironmentBoundRequiredVariableMeetsItsValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			rel, err := d.Query(`[:find ?v :in $ ?z :where (or-default-join [[?z] ?v] (and [(ground 2) ?z] [(ground 7) ?v]) [(ground 0) ?v])]`, int64(1))
			require.NoError(t, err)
			require.Equal(t, []string{"0"}, renderedRows(t, rel, nil))
		})
	}
}

// A union branch mentioning no header symbol under a header of required
// variables alone is a verdict: built once over the unit relation, its body
// having a match answers every tuple of the required run, and none answers
// none.
func TestUnionBranchReadingNothingUnderARequiredOnlyHeaderIsAVerdict(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}},
		"e3": {{":t/b", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", []string{`[?e :t/n _]`, `(or-join [[?e]] [?e :t/a _] [?m :t/b _])`}, []string{"e1", "e2"})
			everyOrder(t, d, names, "?e", []string{`[?e :t/n _]`, `(or-join [[?e]] [?e :t/a _] [?m :t/c _])`}, []string{"e1"})
		})
	}
}

// An or-default-join decides its fallback per tuple of the required run: an
// entity with :t/a answers it, and one without takes the default.
func TestOrDefaultJoinFallsBackPerRequiredGroup(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(10)}},
		"e2": {{":t/n", int64(2)}},
		"e3": {{":t/n", int64(3)}, {":t/a", int64(30)}},
	}
	clauses := []string{`[?e :t/n _]`, `(or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e1 10", "e2 0", "e3 30"})
		})
	}
}

// A not-join anti-joins the subject with its body's result on the header: on
// the entity alone, every entity carrying :t/a is dropped; on the entity and
// its :t/n, only the entity whose :t/a equals its :t/n is.
func TestNotJoinAntiJoinsOnItsHeader(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}},
		"e3": {{":t/n", int64(3)}, {":t/a", int64(4)}},
	}
	headers := []struct {
		form string
		want []string
	}{
		{`(not-join [?e] [?e :t/a _])`, []string{"e2"}},
		{`(not-join [?e ?x] [?e :t/a ?x])`, []string{"e2", "e3"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			for _, h := range headers {
				everyOrder(t, d, names, "?e", []string{`[?e :t/n ?x]`, h.form}, h.want)
			}
		})
	}
}

// A scope's plan is served from the plan cache: a form's second execution
// plans no branch anew, and each branch's plan is a hit.
func TestScopePlanIsServedFromTheCache(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/b", int64(2)}},
	}
	const text = `[:find ?e :where [?e :t/n _] (or-join [?e] [?e :t/a _] [?e :t/b _])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			require.NotNil(t, d.PlanCache(), "the database plans through a cache")
			rel, err := d.Query(text)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1", "e2"}, renderedRows(t, rel, names))
			hitsBefore, missesBefore, _ := d.PlanCache().Stats()

			rel, err = d.Query(text)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1", "e2"}, renderedRows(t, rel, names))
			hitsAfter, missesAfter, _ := d.PlanCache().Stats()
			require.Equal(t, missesBefore, missesAfter, "the second execution plans nothing anew")
			require.GreaterOrEqual(t, hitsAfter-hitsBefore, int64(2), "each branch's plan is a hit on the second execution; hits %d before, %d after", hitsBefore, hitsAfter)
		})
	}
}

// A scope inside a rule body reads the stratum's relations: a rule whose
// body binds its node and then holds an or-join one branch of which invokes
// the rule's own predicate derives the closure of the chain.
func TestScopeInsideABodySeesTheStratum(t *testing.T) {
	const rules = `[[(reach ?x ?z) [?x :type :node] (or-join [[?x] ?z] [?x :edge ?z] (and [?x :edge ?y] (reach ?y ?z)))]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			g := writeGraph(t, d, chain(4))
			rel, err := d.Query(`[:find ?x ?z :in $ % :where (reach ?x ?z)]`, mustRuleSet(t, rules))
			require.NoError(t, err)
			require.ElementsMatch(t, chainClosure(4), g.tuples(t, rel))
		})
	}
}

// The fallback decision for a run tuple reads the branch's result before the
// filter on the carried output: an entity whose :t/want disagrees with its
// :t/a is answered by the first branch and dropped, never passed to the
// default; one with no :t/a takes the default, kept when it equals its :t/want.
func TestFallbackDecisionReadsTheBranchBeforeTheCarriedOutputFilter(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/want", int64(0)}, {":t/a", int64(5)}},
		"e2": {{":t/n", int64(2)}, {":t/want", int64(0)}},
		"e3": {{":t/n", int64(3)}, {":t/want", int64(7)}, {":t/a", int64(7)}},
	}
	clauses := []string{`[?e :t/n _]`, `[?e :t/want ?v]`, `(or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e2 0", "e3 7"})
		})
	}
}

// windowCapNames names the window caps from one tuple to the subject's size
// for the subjects of four tuples the window tests read, indexed by the cap.
var windowCapNames = [...]string{"", "cap of one tuple", "cap of two", "cap of three", "cap of the subject's size"}

// Every window size answers the same tuples: under caps from one tuple to the
// subject's size, the fallback whose keyed branch holds an invocation answers
// each node's reachable nodes and the sink's dead end, and the union of that
// branch with a ground answers the reachable nodes and every node's :self,
// the union taking every branch's answer. A later fallback branch never runs
// for a key an earlier branch answered: with a ground first, which reads
// nothing and answers every key, the branch holding the invocation is built
// for no window and no stratum begins.
func TestEveryWindowSizeAnswersTheSameTuples(t *testing.T) {
	const nodes = 4
	underFallback := append(chainClosure(nodes), nodeName(nodes)+">:dead-end")
	underUnion := chainClosure(nodes)
	var everyNodeOnce []string
	for i := 1; i <= nodes; i++ {
		underUnion = append(underUnion, nodeName(i)+">:self")
		everyNodeOnce = append(everyNodeOnce, nodeName(i)+">1")
	}
	forms := []struct {
		name, text string
		want       []string
	}{
		{"fallback", `[:find ?n ?m :in $ % :where [?n :type :node] (or-default-join [[?n] ?m] (reach ?n ?m) [(ground :dead-end) ?m])]`, underFallback},
		{"union", `[:find ?n ?m :in $ % :where [?n :type :node] (or-join [[?n] ?m] (reach ?n ?m) [(ground :self) ?m])]`, underUnion},
	}
	for _, mode := range optimizerModes {
		for windowCap := 1; windowCap <= nodes; windowCap++ {
			for _, form := range forms {
				t.Run(mode.name+"/"+form.name+"/"+windowCapNames[windowCap], func(t *testing.T) {
					opts := mode.plannerOptions()
					opts.ScopeWindowCap = windowCap
					d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
					g := writeGraph(t, d, chain(nodes))
					rel, err := d.Query(form.text, mustRuleSet(t, closureRules))
					require.NoError(t, err)
					require.ElementsMatch(t, form.want, g.tuples(t, rel))
				})
			}
			t.Run(mode.name+"/ground first/"+windowCapNames[windowCap], func(t *testing.T) {
				var invocationBuilds, strata atomic.Int64
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = windowCap
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					switch e.Name {
					case annotations.ScopeBranchBuild:
						if e.Data["branch"].(int) == 1 {
							invocationBuilds.Add(1)
						}
					case annotations.RulesStratumBegin:
						strata.Add(1)
					}
				}})
				g := writeGraph(t, d, chain(nodes))
				rel, err := d.Query(`[:find ?x ?k :in $ % :where [?x :type :node] (or-default-join [[?x] ?k] [(ground 1) ?k] (and (reach ?x ?z) [(ground 2) ?k]))]`, mustRuleSet(t, closureRules))
				require.NoError(t, err)
				require.ElementsMatch(t, everyNodeOnce, g.tuples(t, rel))
				require.Zero(t, invocationBuilds.Load(), "the branch holding the invocation is built for no window, the ground before it having answered every key")
				require.Zero(t, strata.Load(), "no stratum begins, the invocation never evaluated")
			})
		}
	}
}

// A branch is evaluated once per window and probed per tuple of the window:
// the branch holding the invocation is built once per window of the subject,
// the ground, reading nothing, is built once over the unit relation, and the
// subject's tuples are reported once each. The chain of four is read in more
// than one window under the default cap, so a build per window is told from
// a build once.
func TestBranchIsEvaluatedOncePerWindowAndProbedPerTuple(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var invocationBuilds, groundBuilds, outerTuples, windows atomic.Int64
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				switch e.Name {
				case annotations.ScopeBranchBuild:
					switch e.Data["branch"].(int) {
					case 0:
						invocationBuilds.Add(1)
					case 1:
						groundBuilds.Add(1)
					}
				case annotations.ScopeSubjectTuple:
					outerTuples.Add(1)
				case annotations.ScopeWindow:
					windows.Add(1)
				}
			}})
			g := writeGraph(t, d, chain(4))
			rel, err := d.Query(`[:find ?n ?m :in $ % :where [?n :type :node] (or-default-join [[?n] ?m] (reach ?n ?m) [(ground :dead-end) ?m])]`, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, append(chainClosure(4), "n4>:dead-end"), g.tuples(t, rel))
			require.Greater(t, windows.Load(), int64(1), "the subject is read in more than one window")
			require.Equal(t, windows.Load(), invocationBuilds.Load(), "the branch holding the invocation is built once per window")
			require.Equal(t, int64(1), groundBuilds.Load(), "the ground, reading nothing, is built once over the unit relation")
			require.Equal(t, int64(4), outerTuples.Load(), "the built branches are probed once per subject tuple")
		})
	}
}

// The outer-tuple event fires once per subject tuple whatever the window: an
// or-join of data-pattern branches over four entities reports four subject
// tuples under each cap, and each branch is built once per window, never
// per tuple.
func TestOuterTupleEventFiresPerSubjectTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/b", int64(2)}},
		"e3": {{":t/n", int64(3)}},
		"e4": {{":t/n", int64(4)}, {":t/a", int64(4)}},
	}
	for _, mode := range optimizerModes {
		for windowCap := 1; windowCap <= len(entities); windowCap++ {
			t.Run(mode.name+"/"+windowCapNames[windowCap], func(t *testing.T) {
				var outerTuples, cacheBuilds, windows atomic.Int64
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = windowCap
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					switch e.Name {
					case annotations.ScopeSubjectTuple:
						outerTuples.Add(1)
					case annotations.ScopeBranchBuild:
						cacheBuilds.Add(1)
					case annotations.ScopeWindow:
						windows.Add(1)
					}
				}})
				names := namedEntities(t, d, entities)
				rel, err := d.Query(`[:find ?e :where [?e :t/n _] (or-join [?e] [?e :t/a _] [?e :t/b _])]`)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"e1", "e2", "e4"}, renderedRows(t, rel, names))
				require.Equal(t, int64(4), outerTuples.Load(), "the subject's tuples are reported once each")
				require.Equal(t, 2*windows.Load(), cacheBuilds.Load(), "each of the two branches is built once per window")
			})
		}
	}
}

// A written flat or-join keeps its subject's keys through branches that
// answer at most one tuple per subject tuple, replaces the groups it
// consumed, and seeks on the subject's values.
func TestFlatOrJoinKeepsItsSubjectsPropertiesAndReplacesItsGroups(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/b", int64(2)}},
		"e3": {{":t/n", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var replaced atomic.Int64
			var events []annotations.Event
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				if e.Name == annotations.OrOuterReplaced {
					replaced.Add(1)
				}
				events = append(events, e)
			}})
			names := namedEntities(t, d, entities)
			subject, err := d.Query(`[:find ?e ?v :where [?e :t/n ?v]]`)
			require.NoError(t, err)
			subjectKeys := subject.Properties().Keys
			require.NotEmpty(t, subjectKeys, "the subject carries a key for the contract to be testable")

			rel, err := d.Query(`[:find ?e ?v :where [?e :t/n ?v] (or-join [?e] [?e :t/a _] [?e :t/b _])]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1 1", "e2 2"}, renderedRows(t, rel, names))
			for _, key := range subjectKeys {
				require.True(t, hasKey(rel.Properties().Keys, key), "the result keeps the subject's key %v; keys=%v", key, rel.Properties().Keys)
			}
			require.GreaterOrEqual(t, replaced.Load(), int64(1), "the form's result replaces the consumed groups")
			// A branch that seeks on the subject's entity reaches no
			// attribute-primary index selection; a scan of the attribute's
			// whole extent selects one.
			attrPrimary := map[string]bool{"AETV": true, "AEVT": true, "AVET": true, "ATEV": true}
			for _, attr := range []string{":t/a", ":t/b"} {
				index, bound := indexForAttrPattern(events, attr)
				require.False(t, attrPrimary[index], "the branch over %s seeks on the subject's values; index=%s bound=%s", attr, index, bound)
			}
		})
	}
}

// hasKey reports whether keys holds a key of the same symbols as key.
func hasKey(keys [][]query.Symbol, key []query.Symbol) bool {
	for _, candidate := range keys {
		if len(candidate) != len(key) {
			continue
		}
		same := true
		for _, sym := range key {
			if !query.ContainsSymbol(candidate, sym) {
				same = false
			}
		}
		if same {
			return true
		}
	}
	return false
}

// A flat-header or-default-join decides its fallback once: when the first
// branch answers for any tuple, the default fires for none, and every entity
// is paired with the first branch's values.
func TestFlatHeaderOrDefaultJoinDecidesOnce(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(5)}},
		"e2": {{":t/n", int64(2)}},
	}
	clauses := []string{`[?e :t/n _]`, `(or-default-join [?v] [?x :t/a ?v] [(ground 0) ?v])`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e1 5", "e2 5"})
		})
	}
}

// A :where of one form whose required run nothing binds is refused on both
// planner paths under one message, naming the form and the variable and
// saying that no input and no clause binds it
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3).
func TestOneFormWhereWithNothingBindingItsRequiredRunIsRefused(t *testing.T) {
	forms := []struct{ head, text string }{
		{"(or-join", `[:find ?e :where (or-join [[?x] ?e] [?e :t/a ?x] [?e :t/b ?x])]`},
		{"(or-default-join", `[:find ?e :where (or-default-join [[?x] ?e] [?e :t/a ?x] [(ground :none) ?e])]`},
		{"(not-join", `[:find ?x :where (not-join [?x] [?x :t/a _])]`},
	}
	stalls := map[string][]string{}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			for _, form := range forms {
				_, err := executor.CollectTuples(d.Query(form.text))
				require.Error(t, err, form.text)
				require.Contains(t, err.Error(), form.head, form.text)
				require.Contains(t, err.Error(), "waits on ?x (no input or clause binds it)", form.text)
				at := strings.Index(err.Error(), "cannot order clauses: ")
				require.GreaterOrEqual(t, at, 0, "%s: the refusal carries the stall's prefix: %s", form.text, err)
				stalls[form.head] = append(stalls[form.head], err.Error()[at:])
			}
		})
	}
	for head, rendered := range stalls {
		for _, stall := range rendered[1:] {
			require.Equal(t, rendered[0], stall, "%s: one message on every path", head)
		}
	}
}

// The refusal of a required run no order binds is one text from "cannot
// order clauses:" on, on both planner paths and every backend, for each
// shape of stall (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3): a
// cycle of two expressions, a predicate's input nothing binds, an
// expression's input nothing binds, a subquery's argument nothing binds, a
// required invocation position nothing binds, a union's required variable
// nothing binds, and a cycle of an invocation and an expression. The paths
// differ in the text before the stall alone.
func TestNoOrderRefusalIsOneTextOnBothPathsForEveryStallShape(t *testing.T) {
	entities := map[string][]fact{
		"n1": {{":x", int64(1)}, {":y", int64(10)}},
		"n2": {{":x", int64(2)}, {":y", int64(20)}},
		"n3": {{":x", int64(3)}, {":y", int64(30)}},
	}
	const succ = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`
	shapes := []struct {
		name, text string
		rules      bool
	}{
		{"a cycle of two expressions", `[:find ?w :where [?q :x _] [(+ ?w 1) ?v] [(+ ?v 1) ?w]]`, false},
		{"a predicate's input nothing binds", `[:find ?e :where [?e :x _] [(> ?zz 1)]]`, false},
		{"an expression's input nothing binds", `[:find ?e ?w :where [?e :x _] [(+ ?zz 1) ?w]]`, false},
		{"a subquery's argument nothing binds", `[:find ?e ?n :where [?e :x _] [(q [:find (count ?f) :in $ ?k :where [?f :x ?k]] $ ?zz) [[?n]]]]`, false},
		{"a required invocation position nothing binds", `[:find ?m :in $ % :where [?e :x _] (succ ?zz ?m)]`, true},
		{"a union's required variable nothing binds", `[:find ?e :where [?e :x _] (or-join [[?zz] ?e] [?e :y ?zz] [?e :x ?zz])]`, false},
		{"a cycle of an invocation and an expression", `[:find ?m :in $ % :where [?e :x _] (succ ?k ?m) [(- ?m 1) ?k]]`, true},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			var stalls []string
			for _, mode := range optimizerModes {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				namedEntities(t, d, entities)
				var inputs []interface{}
				if shape.rules {
					inputs = append(inputs, mustRuleSet(t, succ))
				}
				_, err := executor.CollectTuples(d.Query(shape.text, inputs...))
				require.Error(t, err, "%s: %s", mode.name, shape.text)
				at := strings.Index(err.Error(), "cannot order clauses: ")
				require.GreaterOrEqual(t, at, 0, "%s: the refusal carries the stall's prefix: %s", mode.name, err)
				stalls = append(stalls, err.Error()[at:])
			}
			for _, stall := range stalls[1:] {
				require.Equal(t, stalls[0], stall, "one message on every path")
			}
		})
	}
}

// A handler sees, in one event after Prepare, the prepared query, whether the
// inference rewrote a form, and preparation's time
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.9): a query holding a
// headerless or is reported with the or-join the inference wrote and the
// rewrite flagged, a query of header forms with its own text and no rewrite,
// and a query preparation refuses with the invocation and the completion
// alone. The event stands between the invocation and the plan.
func TestPreparationEventCarriesThePreparedQueryAndWhetherItWasRewritten(t *testing.T) {
	entities := map[string][]fact{"e1": {{":t/n", int64(1)}, {":t/a", int64(2)}}}
	cases := []struct {
		name, text, rendered string
		rewritten            bool
	}{
		{"a headerless or", `[:find ?e :where [?e :t/n _] (or [?e :t/a ?x] [?e :t/b ?x])]`, "(or-join [?e] [?e :t/a ?x] [?e :t/b ?x])", true},
		{"a header form", `[:find ?e :where [?e :t/n _] (or-join [?e] [?e :t/a ?x] [?e :t/b ?x])]`, "(or-join [?e] [?e :t/a ?x] [?e :t/b ?x])", false},
		{"a headerless not inside a subquery", `[:find ?e ?c :where [?e :t/n ?v] [(q [:find (count ?x) :in $ ?v :where [?x :t/n ?v] (not [?x :t/a 1])] $ ?v) [[?c]]]]`, "(not-join [?x] [?x :t/a 1])", true},
		{"a query of no form", `[:find ?e :where [?e :t/n 1]]`, "[?e :t/n 1]", false},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var mu sync.Mutex
			var events []annotations.Event
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				mu.Lock()
				events = append(events, e)
				mu.Unlock()
			}})
			namedEntities(t, d, entities)
			lifecycle := func() (invoked, prepared, planned, completed int, event annotations.Event) {
				invoked, prepared, planned, completed = -1, -1, -1, -1
				mu.Lock()
				defer mu.Unlock()
				for i, e := range events {
					switch e.Name {
					case annotations.QueryInvoked:
						invoked = i
					case annotations.QueryPrepared:
						prepared, event = i, e
					case annotations.QueryPlanCreated:
						planned = i
					case annotations.QueryComplete:
						completed = i
					}
				}
				return
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					mu.Lock()
					events = nil
					mu.Unlock()
					_, err := executor.CollectTuples(d.Query(c.text))
					require.NoError(t, err)
					invoked, prepared, planned, _, event := lifecycle()
					require.GreaterOrEqual(t, invoked, 0, "the invocation is reported")
					require.Greater(t, prepared, invoked, "the preparation is reported after the invocation")
					require.Greater(t, planned, prepared, "the plan is reported after the preparation")
					require.Contains(t, event.Data["query"], c.rendered, "the event carries the prepared query")
					require.Equal(t, c.rewritten, event.Data["rewritten"], "the event says whether the inference rewrote a form")
					require.False(t, event.End.IsZero(), "the event carries preparation's end")
					require.GreaterOrEqual(t, event.Latency, time.Duration(0), "the event carries preparation's time")
				})
			}
			t.Run("a query preparation refuses", func(t *testing.T) {
				mu.Lock()
				events = nil
				mu.Unlock()
				rules := mustRuleSet(t, `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`)
				_, err := d.Query(`[:find ?m :in $ % :where (or-join [?m] (succ ?n ?m))]`, rules)
				require.Error(t, err, "a branch reading a required position nothing binds is refused at preparation under the rule set")
				invoked, prepared, _, completed, _ := lifecycle()
				require.GreaterOrEqual(t, invoked, 0, "the invocation is reported")
				require.Equal(t, -1, prepared, "no preparation event is reported for a query preparation refuses")
				require.Greater(t, completed, invoked, "the completion carries the refusal")
			})
			t.Run("a query whose input count the executor's entry refuses", func(t *testing.T) {
				mu.Lock()
				events = nil
				mu.Unlock()
				q, err := parser.ParseQuery(`[:find ?e :in $ ?v :where [?e :t/n ?v]]`)
				require.NoError(t, err)
				_, err = d.NewExecutor().Execute(q)
				require.Error(t, err, "an input the :in binds and the call does not pass is refused")
				invoked, prepared, planned, completed, _ := lifecycle()
				require.Greater(t, prepared, invoked, "the preparation is reported, since the entry checks the count after it")
				require.Equal(t, -1, planned, "no plan is reported for a query the count refuses")
				require.Greater(t, completed, prepared, "the completion carries the refusal")
			})
		})
	}
}

// With EnableEntityPrefetch off no prefetch runs: a query of two patterns
// over the same entity triggers no prefetch and populates no cache entry on
// anyone's behalf.
func TestEntityPrefetchOffRunsNoPrefetch(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/a", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/a", int64(2)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var triggers, populated atomic.Int64
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				switch e.Name {
				case annotations.PrefetchTrigger:
					triggers.Add(1)
				case annotations.StorageResolveComplete:
					if _, ok := e.Data[annotations.KeyEntriesPopulated]; ok {
						populated.Add(1)
					}
				}
			}})
			require.False(t, mode.plannerOptions().EnableEntityPrefetch, "the option is off by default")
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?v :where [?e :t/n _] [?e :t/a ?v]]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1 1", "e2 2"}, renderedRows(t, rel, names))
			require.Zero(t, triggers.Load(), "no prefetch triggers with the option off")
			require.Zero(t, populated.Load(), "no read populates the cache on another's behalf with the option off")
		})
	}
}

// Under EnableEntityPrefetch a fallback whose branch reads the subject's
// entity symbol prefetches a window above the threshold and serves the
// branch from the EA cache, and answers as the scan path does with the
// option off, on both planner paths
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.10, §7).
func TestFallbackPrefetchAnswersAsTheScanPath(t *testing.T) {
	// Under a cap of 128 the subject's 120 entities are read in windows of 1,
	// 2, 4, 8, 16, 32 and 57 tuples, the last the one window above the
	// threshold.
	const subjectSize = 120
	const lastWindow = 57
	entities := map[string][]fact{}
	var want []string
	for i := 1; i <= subjectSize; i++ {
		name := fmt.Sprintf("e%d", i)
		entities[name] = []fact{{":t/n", int64(i)}, {":t/a", int64(i)}}
		want = append(want, fmt.Sprintf("%s %d", name, i))
	}
	const text = `[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			run := func(t *testing.T, prefetch bool) []annotations.Event {
				var triggersMu sync.Mutex
				var triggers []annotations.Event
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = 128
				opts.EnableEntityPrefetch = prefetch
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					if e.Name == annotations.PrefetchTrigger {
						triggersMu.Lock()
						triggers = append(triggers, e)
						triggersMu.Unlock()
					}
				}})
				names := namedEntities(t, d, entities)
				rel, err := d.Query(text)
				require.NoError(t, err)
				require.ElementsMatch(t, want, renderedRows(t, rel, names))
				return triggers
			}
			require.Empty(t, run(t, false), "no trigger with the option off")
			var fallbackTriggers []annotations.Event
			for _, e := range run(t, true) {
				if e.Data[annotations.KeyForm] == "or-default-join" {
					fallbackTriggers = append(fallbackTriggers, e)
				}
			}
			require.Len(t, fallbackTriggers, 1, "the fallback prefetches its one window above the threshold")
			require.Equal(t, lastWindow, fallbackTriggers[0].Data["entity_count"], "the trigger reports the window's entity count")
		})
	}
}

// Under EnableEntityPrefetch a fallback whose pattern branches read the
// subject's entity symbol and its value symbol, both required, answers as
// the scan path does: a tuple is kept when the entity's :t/a is its :t/n, or
// failing that when its :t/b is, and dropped when neither is, on both
// planner paths, with the window above the threshold prefetched once
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.6, §7).
func TestFallbackPrefetchAnswersAsTheScanPathForARequiredValue(t *testing.T) {
	// Under a cap of 128 the subject's 120 entities are read in windows of 1,
	// 2, 4, 8, 16, 32 and 57 tuples, the last the one window above the
	// threshold.
	const subjectSize = 120
	const lastWindow = 57
	entities := map[string][]fact{}
	var want []string
	for i := 1; i <= subjectSize; i++ {
		name := fmt.Sprintf("e%d", i)
		facts := []fact{{":t/n", int64(i)}}
		switch {
		case i%2 == 0:
			facts = append(facts, fact{":t/a", int64(i)})
			want = append(want, fmt.Sprintf("%s %d", name, i))
		case i%4 == 1:
			facts = append(facts, fact{":t/a", int64(i + 1000)}, fact{":t/b", int64(i)})
			want = append(want, fmt.Sprintf("%s %d", name, i))
		default:
			facts = append(facts, fact{":t/a", int64(i + 1000)})
		}
		entities[name] = facts
	}
	const text = `[:find ?e ?v :where [?e :t/n ?v] (or-default-join [[?e ?v]] [?e :t/a ?v] [?e :t/b ?v])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			run := func(t *testing.T, prefetch bool) []annotations.Event {
				var triggersMu sync.Mutex
				var triggers []annotations.Event
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = 128
				opts.EnableEntityPrefetch = prefetch
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					if e.Name == annotations.PrefetchTrigger {
						triggersMu.Lock()
						triggers = append(triggers, e)
						triggersMu.Unlock()
					}
				}})
				names := namedEntities(t, d, entities)
				rel, err := d.Query(text)
				require.NoError(t, err)
				require.ElementsMatch(t, want, renderedRows(t, rel, names))
				return triggers
			}
			require.Empty(t, run(t, false), "no trigger with the option off")
			var fallbackTriggers []annotations.Event
			for _, e := range run(t, true) {
				if e.Data[annotations.KeyForm] == "or-default-join" {
					fallbackTriggers = append(fallbackTriggers, e)
				}
			}
			require.Len(t, fallbackTriggers, 1, "the fallback prefetches its one window above the threshold")
			require.Equal(t, lastWindow, fallbackTriggers[0].Data["entity_count"], "the trigger reports the window's entity count")
		})
	}
}

// A subquery reads a form's result once: the planner orders a subquery after
// the form that provides its input, the form's result stands in its
// streaming subject's place, and the subquery's binding enters the scope as
// a relation over its symbols, joined on them. Each entity carrying :t/p is
// answered beside its count of :t/s referrers, in every written order on
// both planner paths, and the entity with no referrer takes no tuple, since a
// count over nothing binds none.
func TestSubqueryAfterAFormAnswersOverItsStreamingResult(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/p", ref("e4")}},
		"e2": {{":t/p", ref("e5")}},
		"e3": {{":t/p", ref("e4")}},
		"r1": {{":t/s", ref("e1")}},
		"r2": {{":t/s", ref("e2")}},
		"r3": {{":t/s", ref("e2")}},
	}
	clauses := []string{
		`[?e :t/p ?y]`,
		`[(q [:find (count ?s) :in $ ?e :where [?s :t/s ?e]] $ ?e) [[?n] ...]]`,
		`(or-join [[?y] ?e] [?e :t/p ?m] [?y :t/q ?e])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?n", clauses, []string{"e1 1", "e2 2"})
		})
	}
}

// A subquery whose inner query holds an invocation is built once per window
// and rewritten once over the run: under a cap of two tuples the five nodes
// are three windows, the inner query is built once over each, and the
// invocation seeds one rewrite from every node at once, its fixpoint
// continued across the windows. Each node is answered beside the count of
// nodes it reaches, grouped by the input; the sink, reaching none, takes no
// tuple.
func TestSubqueryHoldingAnInvocationIsBuiltOncePerWindow(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			counts := &ruleEventCounts{}
			var builds, windows atomic.Int64
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 2
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
				counts.handle(e)
				if e.Data[annotations.KeyForm] != "subquery" {
					return
				}
				switch e.Name {
				case annotations.ScopeBranchBuild:
					builds.Add(1)
				case annotations.ScopeWindow:
					windows.Add(1)
				}
			}})
			g := writeGraph(t, d, chain(5))
			rel, err := d.Query(`[:find ?n ?k :in $ % :where [?n :type :node] [(q [:find (count ?m) :in $ % ?n :where (reach ?n ?m)] $ % ?n) [[?k] ...]]]`, mustRuleSet(t, closureRules))
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1>4", "n2>3", "n3>2", "n4>1"}, g.tuples(t, rel))
			require.Equal(t, 1, counts.snapshot()[0], "the invocation inside the inner query rewrites once, seeded from every node; the events:\n%s", counts.events())
			require.Equal(t, int64(3), windows.Load(), "under a cap of two the five nodes are three windows")
			require.Equal(t, int64(3), builds.Load(), "the inner query holding the invocation is built once per window")
		})
	}
}

// A subquery's inner query is ordered by the planner, never run in its
// written order: an expression written before the pattern that binds its
// input is evaluated after it, and each entity is answered beside its :t/n
// plus one.
func TestSubqueryBodyIsOrderedByThePlanner(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(5)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?v :where [?e :t/n _] [(q [:find ?v :in $ ?e :where [(+ ?n 1) ?v] [?e :t/n ?n]] $ ?e) [[?v] ...]]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"e1 2", "e2 6"}, renderedRows(t, rel, names))
		})
	}
}

// A subquery over its combination relation gives the per-combination
// tuples: two entities sharing a :t/g each take the inner tuples of that
// group, and an entity whose group has none takes none.
func TestSubqueryOverItsCombinationsGivesThePerCombinationTuples(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/g", int64(1)}},
		"e2": {{":t/g", int64(1)}},
		"e3": {{":t/g", int64(2)}},
		"m1": {{":t/in", int64(1)}, {":t/v", "a"}},
		"m2": {{":t/in", int64(1)}, {":t/v", "b"}},
	}
	clauses := []string{`[?e :t/g ?g]`, `[(q [:find ?v :in $ ?g :where [?m :t/in ?g] [?m :t/v ?v]] $ ?g) [[?v] ...]]`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e1 a", "e1 b", "e2 a", "e2 b"})
		})
	}
}

// A pure aggregate, whose inner query reads no input, over a body matching
// nothing binds no tuple: with no :t/absent to count, no entity is answered.
func TestPureAggregateOverACombinationMatchingNothingBindsNoTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(2)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?k :where [?e :t/n _] [(q [:find (count ?s) :in $ ?e :where [?s :t/absent _]] $ ?e) [[?k] ...]]]`)
			require.NoError(t, err)
			require.Empty(t, renderedRows(t, rel, names), "a count over nothing binds no tuple")
		})
	}
}

// An aggregate whose body does not read the input is a pure aggregate,
// aggregated once and paired with every run tuple: each entity is answered
// beside the count of every :t/s in the store.
func TestAggregateNotReadingTheInputIsPairedWithEveryRunTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(2)}},
		"s1": {{":t/s", int64(1)}},
		"s2": {{":t/s", int64(2)}},
		"s3": {{":t/s", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?k :where [?e :t/n _] [(q [:find (count ?s) :in $ ?e :where [?s :t/s _]] $ ?e) [[?k] ...]]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"e1 3", "e2 3"}, renderedRows(t, rel, names))
		})
	}
}

// A subquery whose inputs span two groups reads their product once: each
// pair of an :t/x holder and a :t/y holder is answered beside the holder of
// their sum, in every written order on both planner paths.
func TestSubqueryInputsSpanningTwoGroupsAreReadAsTheirProduct(t *testing.T) {
	entities := map[string][]fact{
		"a1":  {{":t/x", int64(1)}},
		"a2":  {{":t/x", int64(2)}},
		"b1":  {{":t/y", int64(10)}},
		"h11": {{":t/sum", int64(11)}},
		"h12": {{":t/sum", int64(12)}},
	}
	clauses := []string{
		`[?a :t/x ?x]`,
		`[?b :t/y ?y]`,
		`[(q [:find ?h :in $ ?x ?y :where [(+ ?x ?y) ?s] [?h :t/sum ?s]] $ ?x ?y) [[?h] ...]]`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?a ?b ?h", clauses, []string{"a1 b1 h11", "a2 b1 h12"})
		})
	}
}

// A binding symbol the scope already binds is joined with the subquery's
// result and never superseded, under a non-aggregating inner query and an
// aggregating one: an entity is kept when its :t/n equals its :t/m, and when
// its :t/n equals its count of :t/s referrers.
func TestSubqueryBindingSymbolTheScopeBindsIsJoined(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/m", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/m", int64(3)}},
		"r1": {{":t/s", ref("e1")}},
	}
	inner := []struct {
		name    string
		clauses []string
	}{
		{"non-aggregating", []string{`[?e :t/n ?c]`, `[(q [:find ?c :in $ ?e :where [?e :t/m ?c]] $ ?e) [[?c] ...]]`}},
		{"aggregating", []string{`[?e :t/n ?c]`, `[(q [:find (count ?s) :in $ ?e :where [?s :t/s ?e]] $ ?e) [[?c] ...]]`}},
	}
	for _, mode := range optimizerModes {
		for _, q := range inner {
			t.Run(mode.name+"/"+q.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				names := namedEntities(t, d, entities)
				everyOrder(t, d, names, "?e ?c", q.clauses, []string{"e1 1"})
			})
		}
	}
}

// A subquery over an empty combination relation yields nothing: with no
// entity carrying :t/absent, the inner query is given no combination, and the
// query answers no tuple and no error.
func TestSubqueryOverAnEmptyCombinationRelationYieldsNothing(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?k :where [?e :t/absent _] [(q [:find ?k :in $ ?e :where [?e :t/n ?k]] $ ?e) [[?k] ...]]]`)
			require.NoError(t, err)
			require.Empty(t, renderedRows(t, rel, names))
		})
	}
}

// A subquery answers under the enclosing names whatever the inner query's
// names: the inner :in symbol stands for the call's argument and the inner
// :find element for the binding's variable, and an inner name that is an
// enclosing name in another role is the inner query's own. Each entity
// carrying :t/n is answered beside its :t/m, in every written order on both
// planner paths.
func TestSubqueryAnswersUnderTheEnclosingNamesWhateverTheInnerNames(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/m", int64(10)}},
		"e2": {{":t/n", int64(2)}, {":t/m", int64(20)}},
	}
	inner := []struct {
		name    string
		clauses []string
	}{
		{"names of its own", []string{`[?e :t/n _]`, `[(q [:find ?v :in $ ?x :where [?x :t/m ?v]] $ ?e) [[?c] ...]]`}},
		{"the enclosing names in other roles", []string{`[?e :t/n _]`, `[(q [:find ?e :in $ ?c :where [?c :t/m ?e]] $ ?e) [[?c] ...]]`}},
	}
	for _, mode := range optimizerModes {
		for _, q := range inner {
			t.Run(mode.name+"/"+q.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				names := namedEntities(t, d, entities)
				everyOrder(t, d, names, "?e ?c", q.clauses, []string{"e1 10", "e2 20"})
			})
		}
	}
}

// One enclosing variable passed at two :in places equates the inner symbols
// it stands for: an entity is answered its :t/a when its :t/b equals it, and
// the entity whose two differ takes no tuple.
func TestSubqueryPassingOneVariableAtTwoPlacesEquatesThem(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/a", int64(1)}, {":t/b", int64(1)}},
		"e2": {{":t/a", int64(2)}, {":t/b", int64(3)}},
	}
	clauses := []string{`[?e :t/a _]`, `[(q [:find ?v :in $ ?x ?y :where [?x :t/a ?v] [?y :t/b ?v]] $ ?e ?e) [[?v] ...]]`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?v", clauses, []string{"e1 1"})
		})
	}
}

// A constant argument is a selection on its value: the inner query reads the
// constant at its :in symbol, and the entity whose :t/n is the constant is
// the one the binding, a symbol the scope binds, is joined with.
func TestSubqueryWithAConstantArgumentSelectsOnItsValue(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(2)}},
	}
	clauses := []string{`[?e :t/n _]`, `[(q [:find ?h :in $ ?k :where [?h :t/n ?k]] $ 2) [[?e] ...]]`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", clauses, []string{"e2"})
		})
	}
}

// A combination a later window carries again is read from the index and not
// run: under a cap of one tuple, three entities over two groups are three
// windows, and the inner query is built twice, once per group, the second
// sight of a group answered from the build its first sight made. Each entity
// is answered its group's tuples.
func TestSubqueryRepeatedCombinationIsReadFromTheIndexNotRun(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/g", int64(1)}},
		"e2": {{":t/g", int64(1)}},
		"e3": {{":t/g", int64(2)}},
		"m1": {{":t/in", int64(1)}, {":t/v", "a"}},
		"m2": {{":t/in", int64(2)}, {":t/v", "b"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var builds, windows atomic.Int64
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 1
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
				if e.Data[annotations.KeyForm] != "subquery" {
					return
				}
				switch e.Name {
				case annotations.ScopeBranchBuild:
					builds.Add(1)
				case annotations.ScopeWindow:
					windows.Add(1)
				}
			}})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?v :where [?e :t/g ?g] [(q [:find ?v :in $ ?g :where [?m :t/in ?g] [?m :t/v ?v]] $ ?g) [[?v] ...]]]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1 a", "e2 a", "e3 b"}, renderedRows(t, rel, names))
			require.Equal(t, int64(3), windows.Load(), "under a cap of one each subject tuple is a window")
			require.Equal(t, int64(2), builds.Load(), "the inner query is built once per group, and a group a later window carries again is read from the index")
		})
	}
}

// The collection spelling of a binding answers the relation over its symbol:
// each entity carrying :t/n is answered beside its :t/m, as under the
// relation spelling.
func TestSubqueryCollectionBindingAnswersTheRelationOverItsSymbol(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/m", int64(10)}},
		"e2": {{":t/n", int64(2)}, {":t/m", int64(20)}},
	}
	clauses := []string{`[?e :t/n _]`, `[(q [:find ?v :in $ ?x :where [?x :t/m ?v]] $ ?e) [?c ...]]`}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?c", clauses, []string{"e1 10", "e2 20"})
		})
	}
}

// A subquery inside a branch runs over the branch's window: the count with a
// zero default answers each category its count or zero under every cap from
// one tuple to the subject's size.
func TestSubqueryInsideABranchRunsOverTheBranchsWindow(t *testing.T) {
	entities := map[string][]fact{
		"c1": {{":category/name", "books"}},
		"c2": {{":category/name", "tools"}},
		"c3": {{":category/name", "wire"}},
		"p1": {{":product/category", ref("c1")}},
		"p2": {{":product/category", ref("c1")}},
		"p3": {{":product/category", ref("c3")}},
	}
	const text = `[:find ?category ?count :where [?c :category/name ?category] (or-default [(q [:find (count ?p) :in $ ?c :where [?p :product/category ?c]] $ ?c) [[?count] ...]] [(ground 0) ?count])]`
	for _, mode := range optimizerModes {
		for windowCap := 1; windowCap <= 3; windowCap++ {
			t.Run(mode.name+"/"+windowCapNames[windowCap], func(t *testing.T) {
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = windowCap
				d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
				names := namedEntities(t, d, entities)
				rel, err := d.Query(text)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"books 2", "tools 0", "wire 1"}, renderedRows(t, rel, names))
			})
		}
	}
}

// A count subquery inside a union branch, under a header whose output an
// earlier clause binds, runs over the branch's window of the streaming
// subject: the form's result stands in the subject's place and the subject
// is read once, in every written order on both planner paths. The branch
// holding the subquery binds the output to another value and answers
// nothing, and an entity carrying :t/r and :t/q is kept when a :t/p referrer
// reaches it through the other branch.
func TestSubqueryInsideAUnionBranchUnderACarriedOutputReadsTheSubjectOnce(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/r", int64(1)}, {":t/q", int64(1)}},
		"e2": {{":t/r", int64(1)}, {":t/q", int64(1)}},
		"e3": {{":t/r", int64(1)}},
		"l1": {{":t/p", ref("e1")}},
	}
	clauses := []string{
		`[?e :t/r ?n]`,
		`[?e ?a _]`,
		`[(ground :t/q) ?a]`,
		`(or (and [(ground :t/r) ?a] [(q [:find (count ?s) :in $ ?e :where [?s :t/p ?e]] $ ?e) [[?c] ...]]) (and [?l :t/p ?e] [?e ?a ?m]))`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", clauses, []string{"e1"})
		})
	}
}

// A not reading two required variables followed by an or whose branches
// read the same groups runs each form over the streaming result of the
// clauses before it: each form's result stands in its subject's place and
// is read once, in every written order on both planner paths. A referrer's
// target is answered beside every other referrer, since the not drops the
// pair of a referrer with itself and the or's first branch holds for every
// pair.
func TestANotThenAnOrOverTheSameGroupsReadEachSubjectOnce(t *testing.T) {
	entities := map[string][]fact{
		"e1": {},
		"e2": {},
		"x1": {{":t/p", ref("e1")}},
		"x2": {{":t/p", ref("e2")}},
	}
	clauses := []string{
		`[?x :t/p ?e]`,
		`[?y ?a _]`,
		`[(ground :t/p) ?a]`,
		`(not [(identity ?x) ?y] [(ground :t/p) ?a])`,
		`(or [?m ?a ?l] (and [?m ?a ?x] [?e :t/p ?x]))`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e ?y", clauses, []string{"e1 x2", "e2 x1"})
		})
	}
}

// A branch reading a required variable after an uncorrelated subquery runs
// per tuple, never as a constant: the predicate on the subquery's result
// reads the required ?e, so the branch is answered per entity, and an
// entity with a :t/q referrer or a :t/flag is kept, in every written order
// on both planner paths.
func TestBranchReadingARequiredVariableAfterAnUncorrelatedSubqueryRunsPerTuple(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/flag", true}},
		"e3": {{":t/n", int64(3)}},
		"r1": {{":t/q", ref("e1")}},
	}
	clauses := []string{
		`[?e :t/n _]`,
		`(or-join [[?e]] (and [(q [:find ?y (count ?s) :in $ :where [?s :t/q ?y]] $) [[?y ?c] ...]] [(= ?y ?e)]) [?e :t/flag true])`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", clauses, []string{"e1", "e2"})
		})
	}
}

// A collection input of a subquery binds its members as a group inside the
// scope: an entity's ordered set, passed to an inner query declaring a
// collection input, pairs the entity with the holder of each member and no
// other.
func TestCollectionInputBindsItsMembersAsAGroup(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/ordered", "a"}, {":t/ordered", "b"}},
		"h1": {{":t/string", "a"}},
		"h2": {{":t/string", "b"}},
		"h3": {{":t/string", "c"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?e ?h :where [?e :t/ordered ?members] [(q [:find ?h :in $ [?m ...] :where [?h :t/string ?m]] $ ?members) [[?h] ...]]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"e1 h1", "e1 h2"}, renderedRows(t, rel, names))
		})
	}
}

// An inner symbol that is both an input and a find element stands for two
// enclosing names, the argument's and the binding's: the binding names the
// inner query's outputs, so the enclosing scope reads the input's value under
// the binding's name beside the argument. Through a scalar input each holder
// is answered beside the value it holds under the binding's name, and through
// a collection input the binding's name carries the member the holder holds,
// one tuple per member held. A binding's name the scope already binds is
// joined with that value, and a binding's name that is the argument's own is
// the argument, joined as any binding symbol the subject carries.
func TestSubqueryBindingNamesAnInputTheFindAlsoNames(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/also", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/also", int64(3)}},
		"h1": {{":t/holds", int64(1)}},
		"h2": {{":t/holds", int64(2)}},
		"h3": {{":t/holds", int64(2)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name+"/scalar input", func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			clauses := []string{`[?e :t/n ?n]`, `[(q [:find ?h ?k :in $ ?k :where [?h :t/holds ?k]] $ ?n) [[?h ?kk] ...]]`}
			everyOrder(t, d, names, "?e ?h ?kk", clauses, []string{"e1 h1 1", "e2 h2 2", "e2 h3 2"})
		})
		t.Run(mode.name+"/binding name the scope binds", func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			clauses := []string{`[?e :t/n ?n]`, `[?e :t/also ?kk]`, `[(q [:find ?h ?k :in $ ?k :where [?h :t/holds ?k]] $ ?n) [[?h ?kk] ...]]`}
			everyOrder(t, d, names, "?e ?h ?kk", clauses, []string{"e1 h1 1"})
		})
		t.Run(mode.name+"/binding name the argument's own", func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			clauses := []string{`[?e :t/n ?n]`, `[(q [:find ?h ?k :in $ ?k :where [?h :t/holds ?k]] $ ?n) [[?h ?n] ...]]`}
			everyOrder(t, d, names, "?e ?h ?n", clauses, []string{"e1 h1 1", "e2 h2 2", "e2 h3 2"})
		})
		t.Run(mode.name+"/collection input", func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			rel, err := d.Query(`[:find ?h ?mm :in $ ?ks :where [(q [:find ?h ?m :in $ [?m ...] :where [?h :t/holds ?m]] $ ?ks) [[?h ?mm] ...]]]`, []interface{}{int64(1), int64(2)})
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"h1 1", "h2 2", "h3 2"}, renderedRows(t, rel, names))
		})
	}
}

// A binding naming one variable at two places unifies the two outputs it
// names, as an invocation's repeated argument does: a tuple whose two outputs
// are equal enters the scope under the one name, once, and a tuple whose
// outputs differ is dropped. e1's :t/n and :t/also agree and e2's differ, so
// e1 alone is answered, under ?h.
func TestSubqueryBindingNamingOneVariableTwiceSelectsOnEquality(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/also", int64(1)}},
		"e2": {{":t/n", int64(2)}, {":t/also", int64(3)}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			clauses := []string{`[?e :t/n _]`, `[(q [:find ?x ?y :in $ ?e :where [?e :t/n ?x] [?e :t/also ?y]] $ ?e) [[?h ?h] ...]]`}
			everyOrder(t, d, names, "?e ?h", clauses, []string{"e1 1"})
		})
	}
}

// A form whose required run and output lie in two groups sharing no symbol
// has their product as its subject, and a second form over one of those
// groups reads the first's result: the query answers the same tuples in
// every written order. The or's required run is ?a and ?e from the attribute
// scan and its output ?x from the :q scan; the or-join over ?e then reads the
// or's result. e1, e2 and e3 carry :q and are each a :q target or carry :r,
// and e4 carries only an attribute no clause reads.
func TestFormsOverTwoGroupsSharingNoSymbolAnswerInEveryOrder(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":p", ref("e2")}, {":r", ref("e3")}, {":q", ref("e3")}},
		"e2": {{":q", ref("e1")}},
		"e3": {{":q", ref("e2")}},
		"e4": {{":s", ref("e1")}},
	}
	clauses := []string{
		`[?e ?a _]`,
		`[?x :q _]`,
		`[?w :p _]`,
		`(or (and [?x ?a ?l] [(q [:find (count ?s) :in $ ?x :where [?s :q ?x]] $ ?x) [[?c]]]) (and [?e :p _] [?x :r ?n]))`,
		`(or-join [?e] [?l :q ?e] (and [?e :r ?n] [(ground :r) ?a]))`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := namedEntities(t, d, entities)
			everyOrder(t, d, names, "?e", clauses, []string{"e1", "e2", "e3"})
		})
	}
}

// A top-level invocation whose two arguments two groups sharing no symbol
// bind has their product as its subject, run as the union form over the
// window: in every written order the query answers the pairs the closure
// holds, and the invocation's windows fire under its own form. e1, e2 and e5
// carry :l, e2, e3 and e4 carry :r, the edges chain e1 to e4, and e5 has no
// edge; both scans carry a constant, so the planner places them before the
// invocation in every order.
func TestInvocationWhoseArgumentsTwoGroupsBindRunsOverTheirProduct(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":l", int64(1)}, {":edge", ref("e2")}},
		"e2": {{":l", int64(1)}, {":r", int64(1)}, {":edge", ref("e3")}},
		"e3": {{":r", int64(1)}, {":edge", ref("e4")}},
		"e4": {{":r", int64(1)}},
		"e5": {{":l", int64(1)}},
	}
	clauses := []string{`[?x :l 1]`, `[?y :r 1]`, `(reach ?x ?y)`}
	want := []string{"e1 e2", "e1 e3", "e1 e4", "e2 e3", "e2 e4"}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var windows atomic.Int32
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				if e.Name == annotations.ScopeWindow && e.Data[annotations.KeyForm] == "invocation" {
					windows.Add(1)
				}
			}})
			names := namedEntities(t, d, entities)
			rules := mustRuleSet(t, closureRules)
			for _, order := range orderings(len(clauses)) {
				text := fmt.Sprintf("[:find ?x ?y :in $ %% :where %s %s %s]", clauses[order[0]], clauses[order[1]], clauses[order[2]])
				windows.Store(0)
				rel, err := d.Query(text, rules)
				require.NoError(t, err, text)
				require.Equal(t, want, renderedRows(t, rel, names), text)
				require.GreaterOrEqual(t, windows.Load(), int32(1), "the invocation runs over the window of the product: %s", text)
			}
		})
	}
}

// A union branch is planned over the required run alone before the carried
// outputs are bound into it: a branch no order runs over the required run
// is the form's refusal on both planner paths, naming the branch counted
// from one, never an answer over the carried output. The cycle of two
// identities under a header binding none of it reads nothing of what it is
// given and is built once over the required run. The same cycle beside a
// read of a required variable runs per tuple, where the seek would bind the
// carried output and close the cycle.
func TestBranchNoOrderRunsUnderTheRequiredRunIsRefused(t *testing.T) {
	entities := map[string][]fact{
		"e1": {{":t/n", int64(1)}, {":t/p", ref("e2")}},
		"e2": {{":t/n", int64(2)}},
	}
	queries := []struct {
		name  string
		query string
	}{
		{"a branch reading nothing", `[:find ?e :where [?e :t/n _] (or-join [?e] (and [(identity ?m) ?e] [(identity ?e) ?m]) [?e :t/p ?m])]`},
		{"a branch run per tuple", `[:find ?e :where [?e :t/n ?r] (or-join [[?r] ?e] (and [(identity ?m) ?e] [(identity ?e) ?m] [(> ?r 0)]) [?e :t/p ?m])]`},
		{"the headerless spelling", `[:find ?e :where [?e :t/n ?r] (or (and [(identity ?m) ?e] [(identity ?e) ?m] [(> ?r 0)]) [?e :t/p ?m])]`},
	}
	for _, mode := range optimizerModes {
		for _, q := range queries {
			t.Run(mode.name+"/"+q.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				namedEntities(t, d, entities)
				_, err := executor.CollectTuples(d.Query(q.query))
				require.Error(t, err, "no order runs the first branch over the required run alone")
				require.Contains(t, err.Error(), "branch 1")
			})
		}
	}
}
