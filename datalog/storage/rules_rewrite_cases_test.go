package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// The magic rewrite's cases over one graph: four edge entities, 1→2, 2→3,
// 3→4 and 4→2, each carrying :edge/from and :edge/to as longs, and six
// nodes 0 to 5 carrying :node/id, nodes 2 and 4 red and the rest blue. The
// closure of the edges is every pair over 1 to 4 but those leaving for 1.
// Each case is a query over a rule set and its inputs, and the tuples it
// answers on both planner paths, or its refusal.

// writeRewriteGraph commits the graph into d and returns its entities'
// names by identity.
func writeRewriteGraph(t testing.TB, d *Database) map[datalog.Identity]string {
	t.Helper()
	names := map[datalog.Identity]string{}
	tx := d.NewTransaction()
	for i, edge := range [][2]int64{{1, 2}, {2, 3}, {3, 4}, {4, 2}} {
		id := datalog.NewIdentity(fmt.Sprintf("edge%d", i))
		names[id] = fmt.Sprintf("edge%d", i)
		require.NoError(t, tx.Add(id, kw(":edge/from"), edge[0]))
		require.NoError(t, tx.Add(id, kw(":edge/to"), edge[1]))
	}
	for i := int64(0); i < 6; i++ {
		id := datalog.NewIdentity(fmt.Sprintf("node%d", i))
		names[id] = fmt.Sprintf("node%d", i)
		color := "blue"
		if i == 2 || i == 4 {
			color = "red"
		}
		require.NoError(t, tx.Add(id, kw(":node/id"), i))
		require.NoError(t, tx.Add(id, kw(":node/color"), color))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
	return names
}

const (
	rewriteEdge = `[(e ?a ?b) [?x :edge/from ?a] [?x :edge/to ?b]]`
	rewriteRed  = `[(red ?a) [?n :node/id ?a] [?n :node/color "red"]]`
	rewriteNode = `[(node ?a) [?n :node/id ?a]]`
	rewriteSucc = `[(succ [?n] ?m) [(+ ?n 1) ?m]]`
	// rewriteReach derives the closure through the patterns themselves.
	rewriteReach = `[[(reach ?a ?b) [?x :edge/from ?a] [?x :edge/to ?b]] [(reach ?a ?c) [?x :edge/from ?a] [?x :edge/to ?b] (reach ?b ?c)]]`
	// rewriteStep derives stepw, the successor of an edge's target, with
	// the required invocation written before the pattern binding it.
	rewriteStep = `[` + rewriteEdge + ` ` + rewriteSucc + ` [(stepw ?a ?b) (succ ?x ?b) (e ?a ?x)]]`
	// rewriteStepBinderFirst is rewriteStep with the binder written first.
	rewriteStepBinderFirst = `[` + rewriteEdge + ` ` + rewriteSucc + ` [(stepw ?a ?b) (e ?a ?x) (succ ?x ?b)]]`
	// rewriteMimic is the magic stratum of a bound closure written by hand
	// under plain names, seeded at 1.
	rewriteMimic = `[[(mr ?a) [(ground 1) ?a]] [(mr ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(me ?b) (mr ?a) (ae ?a ?b)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`
)

// rewriteClosure is the closure of the four edges.
var rewriteClosure = []string{"1 2", "1 3", "1 4", "2 2", "2 3", "2 4", "3 2", "3 3", "3 4", "4 2", "4 3", "4 4"}

// rewriteClosureWithZero is the closure beside 0 reached from every source.
var rewriteClosureWithZero = append(append([]string{}, rewriteClosure...), "1 0", "2 0", "3 0", "4 0")

// rewriteEdges is the four edges.
var rewriteEdges = []string{"1 2", "2 3", "3 4", "4 2"}

// rewriteCase is one query over a rule set, its inputs after the rule set,
// and the tuples it answers, or that it is refused, with a part of the
// refusal's message when the records state one.
type rewriteCase struct {
	name, rules, text string
	inputs            []interface{}
	refused           bool
	message           string
	want              []string
}

// orRecursionCases are a rule set deriving reach through an or over the
// recursion, invoked whole and bound at both positions, at the second and at
// the first, under the rule's names and under the query's own. Whole answers
// the closure, and where the or's other branch grounds 0 for a red node,
// every source reaches 0 too; each bound invocation answers its selection
// of the whole. A refused set is refused under every spelling.
func orRecursionCases(label, or string, withZero, refused bool) []rewriteCase {
	rules := `[` + rewriteEdge + ` ` + rewriteRed + ` [(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) ` + or + `]]`
	whole := rewriteClosure
	fromOne := []string{"1 2", "1 3", "1 4"}
	var atZero, both []string
	if withZero {
		whole = rewriteClosureWithZero
		fromOne = append(fromOne, "1 0")
		atZero = []string{"1 0", "2 0", "3 0", "4 0"}
		both = []string{"1 0"}
	}
	one, zero := int64(1), int64(0)
	return []rewriteCase{
		{label + ", whole", rules, `[:find ?a ?c :in $ % :where (reach ?a ?c)]`, nil, refused, "", whole},
		{label + ", bound at both", rules, `[:find ?a ?c :in $ % ?a ?c :where (reach ?a ?c)]`, []interface{}{one, zero}, refused, "", both},
		{label + ", bound at the second", rules, `[:find ?a ?c :in $ % ?c :where (reach ?a ?c)]`, []interface{}{zero}, refused, "", atZero},
		{label + ", bound at the first", rules, `[:find ?a ?c :in $ % ?a :where (reach ?a ?c)]`, []interface{}{one}, refused, "", fromOne},
		{label + ", bound at the first under the query's names", rules, `[:find ?p ?q :in $ % ?p :where (reach ?p ?q)]`, []interface{}{one}, refused, "", fromOne},
		{label + ", bound at the second under the query's names", rules, `[:find ?p ?q :in $ % ?q :where (reach ?p ?q)]`, []interface{}{zero}, refused, "", atZero},
	}
}

func rewriteCases() []rewriteCase {
	one, two, three, four, zero := int64(1), int64(2), int64(3), int64(4), int64(0)
	declRequired := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q [?x] ?z) (e ?x ?z)] [(q [?x] ?z) (e ?x ?y) (q ?y ?w) (red ?w) (e ?w ?z)] [(q [?x] ?z) (e ?x ?y) (not (red ?y)) (q ?y ?z)]]`
	declFree := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (red ?w) (e ?w ?z)] [(q ?x ?z) (e ?x ?y) (not (red ?y)) (q ?y ?z)]]`
	walkSucc := `[` + rewriteEdge + ` ` + rewriteRed + ` ` + rewriteSucc + ` [(walk ?a ?b) (e ?a ?b)] [(walk ?a ?c) (e ?a ?b) (succ ?b ?k) [(< ?k 100)] (walk ?b ?c)]]`
	walkSuccNegated := `[` + rewriteEdge + ` ` + rewriteRed + ` ` + rewriteSucc + ` [(walk ?a ?b) (e ?a ?b)] [(walk ?a ?c) (e ?a ?b) (not (red ?b)) (succ ?b ?k) [(< ?k 100)] (walk ?b ?c)]]`
	walkHop := `[` + rewriteEdge + ` [(walk ?a ?c) (e ?a ?b) (hop ?b ?c)] [(hop [?b] ?c) (e ?b ?c)] [(hop [?b] ?c) (walk ?b ?c)]]`
	negatedFree := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (not (red ?y)) (q ?y ?z)]]`
	negatedRequired := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q [?x] ?z) (e ?x ?z)] [(q [?x] ?z) (e ?x ?y) (not (red ?y)) (q ?y ?z)]]`
	negatedInOrFree := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (or (not (red ?y)) [(= ?y 9)]) (q ?y ?z)]]`
	negatedInOrRequired := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q [?x] ?z) (e ?x ?z)] [(q [?x] ?z) (e ?x ?y) (or (not (red ?y)) [(= ?y 9)]) (q ?y ?z)]]`
	negatedInOrAfterRequired := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q [?x] ?z) (e ?x ?z)] [(q [?x] ?z) (e ?x ?y) (q ?y ?z) (or (not (red ?y)) [(= ?y 9)])]]`
	twoInvocationsRequired := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q [?x] ?z) (e ?x ?z)] [(q [?x] ?z) (e ?x ?y) (q ?y ?w) (q ?w ?z) (not (red ?y))]]`
	twoInvocationsFree := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (q ?w ?z) (not (red ?y))]]`
	orDefaultThenSucc := `[` + rewriteRed + ` ` + rewriteNode + ` ` + rewriteSucc + ` [(r ?a ?m) (node ?a) (or-default (and (red ?a) [(ground 10) ?k]) [(ground 20) ?k]) (succ ?k ?m)]]`
	orDefaultJoinThenSucc := `[` + rewriteRed + ` ` + rewriteNode + ` ` + rewriteSucc + ` [(r ?a ?m) (node ?a) (or-default-join [[?a] ?k] (and (red ?a) [(ground 10) ?k]) [(ground 20) ?k]) (succ ?k ?m)]]`
	walkLabelled := `[` + rewriteEdge + ` ` + rewriteRed + ` [(walk ?a ?b ?l) (e ?a ?b) [(ground "e") ?l]] [(walk ?a ?c ?l) (e ?a ?b) (or-default (and (red ?b) [(ground "r") ?l]) [(ground "b") ?l]) (walk ?b ?c _)]]`
	walkNegated := `[` + rewriteEdge + ` ` + rewriteRed + ` [(walk ?a ?b) (e ?a ?b)] [(walk ?a ?c) (e ?a ?b) (not (red ?b)) (walk ?b ?c)]]`
	orNotFree := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (or (not (red ?y)) (red ?w)) (q ?w ?z)]]`
	orNotRequired := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q [?x] ?z) (e ?x ?z)] [(q [?x] ?z) (e ?x ?y) (q ?y ?w) (or (not (red ?y)) (red ?w)) (q ?w ?z)]]`
	orNotRequiredAfter := `[` + rewriteEdge + ` ` + rewriteRed + ` [(q [?x] ?z) (e ?x ?z)] [(q [?x] ?z) (e ?x ?y) (q ?y ?w) (q ?w ?z) (or (not (red ?y)) (red ?w))]]`
	reachOverEdges := rewriteEdge + ` ` + rewriteRed + ` [(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)]`
	predicateBeforeBinder := `[` + reachOverEdges + ` [(t ?a ?c) [(< ?w 3)] (reach ?a ?c) (e ?w ?c)]]`
	expressionBeforeBinder := `[` + reachOverEdges + ` [(t ?a ?d) [(+ ?w 0) ?w2] (reach ?a ?d) (e ?w ?d) [(= ?w ?w2)]]]`
	negationBeforeBinder := `[` + reachOverEdges + ` [(t ?a ?c) (not-join [?w] (red ?w)) (reach ?a ?c) (e ?w ?c)]]`
	stepFanIn := `[` + rewriteEdge + ` ` + rewriteSucc + ` [(stepw ?a ?b) (e ?a ?x) (succ ?x ?b)] [(cnt ?a ?k) (e ?a _) [(q [:find (count ?y) :in $ % ?a :where (stepw ?a ?y)] $ % ?a) [[?k]]]]]`
	stepTwo := `[` + rewriteEdge + ` ` + rewriteSucc + ` [(stepw ?a ?b) (e ?a ?x) (succ ?x ?b)] [(two ?c ?z ?y) (stepw ?z ?y) (e ?z ?c)]]`
	existentialNot := `[[(r ?a) [?n :node/id ?a] (not [?x :edge/from ?a] [?x :edge/to ?z])]]`
	existentialNotInOr := `[[(r ?a) [?n :node/id ?a] (or (and [?n :node/color "red"] (not [?x :edge/from ?a] [?x :edge/to ?z])) [?n :node/color "blue"])]]`
	existentialNotInFlatOrJoin := `[[(r ?a) [?n :node/id ?a] (or-join [?n ?a] (and [?n :node/color "red"] (not [?x :edge/from ?a] [?x :edge/to ?z])) [?n :node/color "blue"])]]`
	existentialNotJoinInOr := `[[(r ?a) [?n :node/id ?a] (or (and [?n :node/color "red"] (not-join [?a] [?x :edge/from ?a] [?x :edge/to ?z])) [?n :node/color "blue"])]]`
	reachWithZero := `[` + rewriteEdge + ` ` + rewriteRed + ` [(reach ?a ?c) (e ?a ?b) (or (reach ?b ?c) (and (red ?b) [(ground 0) ?c]))]]`
	reachWithZeroSplit := `[` + rewriteEdge + ` ` + rewriteRed + ` [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(reach ?a ?c) (e ?a ?b) (red ?b) [(ground 0) ?c]]]`
	groundInOrRule := `[` + rewriteEdge + ` [(t ?b ?c) [?x :edge/from ?b] [?y :node/id ?c] (or (and [?n :node/id ?b] [?n :node/color "red"] [(ground 0) ?c]) [?y :node/color "nope"])]]`
	groundInOrRuleInvoking := `[` + rewriteRed + ` [(t ?b ?c) [?x :edge/from ?b] [?y :node/id ?c] (or (and (red ?b) [(ground 0) ?c]) [?y :node/color "nope"])]]`
	nodesReaching := []string{"1 2", "1 3", "1 4", "2 2", "2 3", "2 4", "3 2", "3 3", "3 4"}
	stepTuples := []string{"1 3", "2 4", "3 5", "4 3"}

	cases := []rewriteCase{
		{"cap: bound through an input named ?start", rewriteReach, `[:find ?y :in $ % ?start :where (reach ?start ?y)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},
		{"cap: bound through an input named as the rule's variable", rewriteReach, `[:find ?y :in $ % ?a :where (reach ?a ?y)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},
		{"cap: bound at the second position", rewriteReach, `[:find ?y :in $ % ?b :where (reach ?y ?b)]`, []interface{}{three}, false, "", []string{"1", "2", "3", "4"}},
		{"cap: free beside an input named as a rule's variable", rewriteReach, `[:find ?y ?z :in $ % ?c :where (reach ?y ?z) [(< ?y ?c)]]`, []interface{}{four}, false, "", nodesReaching},
		{"cap: free beside an input", rewriteReach, `[:find ?y ?z :in $ % ?w :where (reach ?y ?z) [(< ?y ?w)]]`, []interface{}{four}, false, "", nodesReaching},

		{"decl: a required predicate under a negation, bound", declRequired, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3"}},
		{"decl: the same predicate free, bound", declFree, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3"}},
		{"decl: the same predicate free, whole", declFree, `[:find ?p ?q :in $ % :where (q ?p ?q)]`, nil, false, "", []string{"1 2", "1 3", "2 2", "2 3", "2 4", "3 3", "3 4", "4 2", "4 3"}},

		{"decreq: a required predicate inside the recursion, bound", walkSucc, `[:find ?q :in $ % ?p :where (walk ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},
		{"decreq: a negation before the required predicate, bound", walkSuccNegated, `[:find ?q :in $ % ?p :where (walk ?p ?q)]`, []interface{}{one}, false, "", []string{"2"}},

		{"mreq: a free predicate mutually recursive with a required one, whole", walkHop, `[:find ?p ?q :in $ % :where (walk ?p ?q)]`, nil, false, "", rewriteClosure},
		{"mreq: the free predicate bound", walkHop, `[:find ?q :in $ % ?p :where (walk ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},
		{"mreq: the required predicate bound", walkHop, `[:find ?q :in $ % ?p :where (hop ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},

		{"mvnn: a not with existentials at a body's top level", existentialNot, `[:find ?p :in $ % :where (r ?p)]`, nil, false, "", []string{"0", "5"}},
		{"mvnn: the same not inside an or branch", existentialNotInOr, `[:find ?p :in $ % :where (r ?p)]`, nil, false, "", []string{"0", "1", "3", "5"}},
		{"mvnn: the same not inside a flat or-join whose second branch binds not every output", existentialNotInFlatOrJoin, `[:find ?p :in $ % :where (r ?p)]`, nil, true, "does not bind ?a", nil},
		{"mvnn: the same not inside an or-join under a required-run header", `[[(r ?a) [?n :node/id ?a] (or-join [[?n ?a]] (and [?n :node/color "red"] (not [?x :edge/from ?a] [?x :edge/to ?z])) [?n :node/color "blue"])]]`, `[:find ?p :in $ % :where (r ?p)]`, nil, false, "", []string{"0", "1", "3", "5"}},
		{"mvnn: the same not inside an or branch of a query", "", `[:find ?a :where [?n :node/id ?a] (or (and [?n :node/color "red"] (not [?x :edge/from ?a] [?x :edge/to ?z])) [?n :node/color "blue"])]`, nil, false, "", []string{"0", "1", "3", "5"}},
		{"mvnn: the same not inside an or-join under a required-run header of a query", "", `[:find ?a :where [?n :node/id ?a] (or-join [[?n ?a]] (and [?n :node/color "red"] (not [?x :edge/from ?a] [?x :edge/to ?z])) [?n :node/color "blue"])]`, nil, false, "", []string{"0", "1", "3", "5"}},
		{"mvnn: the not at a query's top level", "", `[:find ?a :where [?n :node/id ?a] (not [?x :edge/from ?a] [?x :edge/to ?z])]`, nil, false, "", []string{"0", "5"}},
		{"mvnn: a not-join inside an or branch", existentialNotJoinInOr, `[:find ?p :in $ % :where (r ?p)]`, nil, false, "", []string{"0", "1", "3", "5"}},

		{"mvnot: a negation before the recursion, free, whole", negatedFree, `[:find ?p ?q :in $ % :where (q ?p ?q)]`, nil, false, "", []string{"1 2", "2 3", "2 4", "3 4", "4 2"}},
		{"mvnot: a negation before the recursion, required, bound", negatedRequired, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2"}},
		{"mvnot: the negation inside an or, free, whole", negatedInOrFree, `[:find ?p ?q :in $ % :where (q ?p ?q)]`, nil, false, "", []string{"1 2", "2 3", "2 4", "3 4", "4 2"}},
		{"mvnot: the negation inside an or, required, bound", negatedInOrRequired, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2"}},
		{"mvnot: the or written after the recursion, required, bound", negatedInOrAfterRequired, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2"}},
		{"mvnot: two recursive invocations before the negation, required, bound", twoInvocationsRequired, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2"}},
		{"mvnot: two recursive invocations before the negation, free, whole", twoInvocationsFree, `[:find ?p ?q :in $ % :where (q ?p ?q)]`, nil, false, "", []string{"1 2", "2 2", "2 3", "3 4", "4 2"}},

		{"mvord: a required invocation bound by an or-default, constant head position", orDefaultThenSucc, `[:find ?q :in $ % :where (r 2 ?q)]`, nil, false, "", []string{"11"}},
		{"mvord: a required invocation bound by an or-default, an earlier clause binding the head position", orDefaultThenSucc, `[:find ?q :in $ % :where [?n :node/id 2] [?n :node/id ?p] (r ?p ?q)]`, nil, false, "", []string{"11"}},

		{"names: a hand-written magic stratum under the engine's names", `[[(magic.bf/reach ?a) [(ground 1) ?a]] [(magic.bf/reach ?b) (magic.bf/reach ?a) (adorned.bf/e ?a ?b)] [(magic.bf/e ?a) (magic.bf/reach ?a)] [(magic.bf/e ?a) (magic.bf/reach ?a)] [(magic.bf/e ?b) (magic.bf/reach ?a) (adorned.bf/e ?a ?b)] [(adorned.bf/e ?a ?b) (magic.bf/e ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`, `[:find ?a ?b :in $ % :where (adorned.bf/e ?a ?b)]`, nil, true, "the engine's", nil},
		{"names: the same stratum under plain names with a rule written twice", `[[(mr ?a) [(ground 1) ?a]] [(mr ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(me ?a) (mr ?a)] [(me ?b) (mr ?a) (ae ?a ?b)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`, `[:find ?a ?b :in $ % :where (ae ?a ?b)]`, nil, false, "", rewriteEdges},
		{"names: the same stratum under plain names", rewriteMimic, `[:find ?a ?b :in $ % :where (ae ?a ?b)]`, nil, false, "", rewriteEdges},

		{"or: a ground in an or branch over an input the query binds, beside a rule set", `[[(dummy ?x) [?x :node/id _]]]`, `[:find ?c :in $ % ?c :where [?n :node/id ?c] (or [(ground 0) ?c] [?n :node/color "nope"])]`, []interface{}{zero}, false, "", []string{"0"}},
		{"or: a ground in an or branch over an input the query binds", "", `[:find ?c :in $ ?c :where [?n :node/id ?c] (or [(ground 0) ?c] [?n :node/color "nope"])]`, []interface{}{zero}, false, "", []string{"0"}},

		{"or2: a ground in an or branch over a pattern-bound variable", "", `[:find ?n ?c :where [?n :node/id ?c] (or [(ground 0) ?c] [?n :node/color "nope"])]`, nil, false, "", []string{"node0 0"}},
		{"or2: a pattern beside the ground in the branch", "", `[:find ?c :where [?m :node/id ?c] (or (and [?n :node/color "red"] [(ground 0) ?c]) [?m :node/color "nope"])]`, nil, false, "", []string{"0"}},
		{"or2: a pattern on a second pattern-bound variable beside the ground", "", `[:find ?b ?c :where [?x :edge/from ?b] [?y :node/id ?c] (or (and [?n :node/id ?b] [?n :node/color "red"] [(ground 0) ?c]) [?y :node/color "nope"])]`, nil, false, "", []string{"2 0", "4 0"}},
		{"or2: the same in a rule body", groundInOrRule, `[:find ?b ?c :in $ % :where (t ?b ?c)]`, nil, false, "", []string{"2 0", "4 0"}},
		{"or2: the same in a rule body with an invocation in the branch", groundInOrRuleInvoking, `[:find ?b ?c :in $ % :where (t ?b ?c)]`, nil, false, "", []string{"2 0", "4 0"}},

		{"ord: an or-default over a lower predicate in a recursive rule, whole", walkLabelled, `[:find ?p ?q ?l :in $ % :where (walk ?p ?q ?l)]`, nil, false, "", []string{"1 2 e", "2 3 e", "3 4 e", "4 2 e", "1 2 r", "1 3 r", "1 4 r", "2 2 b", "2 3 b", "2 4 b", "3 2 r", "3 3 r", "3 4 r", "4 2 r", "4 3 r", "4 4 r"}},
		{"ord: the same bound", walkLabelled, `[:find ?q ?l :in $ % ?p :where (walk ?p ?q ?l)]`, []interface{}{one}, false, "", []string{"2 e", "2 r", "3 r", "4 r"}},
		{"ord: a not over a lower predicate in a recursive rule, whole", walkNegated, `[:find ?p ?q :in $ % :where (walk ?p ?q)]`, nil, false, "", []string{"1 2", "2 3", "2 4", "3 4", "4 2"}},
		{"ord: the same bound", walkNegated, `[:find ?q :in $ % ?p :where (walk ?p ?q)]`, []interface{}{one}, false, "", []string{"2"}},

		{"order: the mimic's rules with the recursive rule first", `[[(mr ?b) (mr ?a) (ae ?a ?b)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]] [(me ?a) (mr ?a)] [(me ?b) (mr ?a) (ae ?a ?b)] [(mr ?a) [(ground 1) ?a]]]`, `[:find ?a ?b :in $ % :where (ae ?a ?b)]`, nil, false, "", rewriteEdges},
		{"order: the mimic's rules with the me rules swapped", `[[(mr ?a) [(ground 1) ?a]] [(mr ?b) (mr ?a) (ae ?a ?b)] [(me ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`, `[:find ?a ?b :in $ % :where (ae ?a ?b)]`, nil, false, "", rewriteEdges},
		{"order: the mimic's rules with the me rules swapped and the recursive rule first", `[[(mr ?b) (mr ?a) (ae ?a ?b)] [(mr ?a) [(ground 1) ?a]] [(me ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`, `[:find ?a ?b :in $ % :where (ae ?a ?b)]`, nil, false, "", rewriteEdges},

		{"ordreq: a required invocation bound by an or-default written before it, whole", orDefaultThenSucc, `[:find ?p ?q :in $ % :where (r ?p ?q)]`, nil, false, "", []string{"0 21", "1 21", "2 11", "3 21", "4 11", "5 21"}},
		{"ordreq: the same bound", orDefaultThenSucc, `[:find ?q :in $ % ?p :where (r ?p ?q)]`, []interface{}{two}, false, "", []string{"11"}},
		{"ordreq: bound by an or-default-join written before it", orDefaultJoinThenSucc, `[:find ?q :in $ % ?p :where (r ?p ?q)]`, []interface{}{two}, false, "", []string{"11"}},
		{"ordreq: the same shape at the query level", `[` + rewriteRed + ` ` + rewriteNode + ` ` + rewriteSucc + `]`, `[:find ?q :in $ % ?p :where (node ?p) (or-default (and (red ?p) [(ground 10) ?k]) [(ground 20) ?k]) (succ ?k ?q)]`, []interface{}{two}, false, "", []string{"11"}},

		{"ornot: a negation inside an or after the recursion, free, whole", orNotFree, `[:find ?p ?q :in $ % :where (q ?p ?q)]`, nil, false, "", rewriteClosure},
		{"ornot: the same bound", orNotFree, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},
		{"ornot: the predicate required, bound", orNotRequired, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},
		{"ornot: the predicate required with the or written last, bound", orNotRequiredAfter, `[:find ?q :in $ % ?p :where (q ?p ?q)]`, []interface{}{one}, false, "", []string{"2", "3", "4"}},

		{"r2x: a required invocation written before its provider, free", rewriteStep, `[:find ?p ?y :in $ % :where (stepw ?p ?y)]`, nil, false, "", stepTuples},
		{"r2x: the same bound", rewriteStep, `[:find ?y :in $ % ?p :where (stepw ?p ?y)]`, []interface{}{one}, false, "", []string{"3"}},
		{"r2x: bound at the second position with the binder first", rewriteStepBinderFirst, `[:find ?y :in $ % ?p :where (stepw ?y ?p)]`, []interface{}{three}, false, "", []string{"1", "4"}},

		{"req: the provider before the required invocation, bound", rewriteStepBinderFirst, `[:find ?y :in $ % ?p :where (stepw ?p ?y)]`, []interface{}{one}, false, "", []string{"3"}},
		{"req: the required invocation before its provider, bound", rewriteStep, `[:find ?y :in $ % ?p :where (stepw ?p ?y)]`, []interface{}{one}, false, "", []string{"3"}},
		{"req: the required invocation before its provider at the query level", `[` + rewriteEdge + ` ` + rewriteSucc + `]`, `[:find ?y :in $ % ?p :where (succ ?x ?y) (e ?p ?x)]`, []interface{}{one}, false, "", []string{"3"}},
		{"req: a free invocation of a predicate invoking the required one", rewriteStepBinderFirst, `[:find ?p ?y :in $ % :where (stepw ?p ?y)]`, nil, false, "", stepTuples},
		{"req: a bound invocation whose rewrite reaches the predicate inside a subquery", stepFanIn, `[:find ?k :in $ % ?p :where (cnt ?p ?k)]`, []interface{}{one}, false, "", []string{"1"}},
		{"req: a bound invocation whose body invokes the predicate at an unbound position", stepTwo, `[:find ?z ?y :in $ % ?p :where (two ?p ?z ?y)]`, []interface{}{two}, false, "", []string{"1 3", "4 3"}},

		{"sn: the mimic's seed predicate, whole", rewriteMimic, `[:find ?a :in $ % :where (mr ?a)]`, nil, false, "", []string{"1", "2", "3", "4"}},
		{"sn: the mimic's adorned predicate, whole", rewriteMimic, `[:find ?a ?b :in $ % :where (ae ?a ?b)]`, nil, false, "", rewriteEdges},
		{"sn: the mimic without the second me rule", `[[(mr ?a) [(ground 1) ?a]] [(mr ?b) (mr ?a) (ae ?a ?b)] [(me ?a) (mr ?a)] [(ae ?a ?b) (me ?a) [?x :edge/from ?a] [?x :edge/to ?b]]]`, `[:find ?a ?b :in $ % :where (ae ?a ?b)]`, nil, false, "", rewriteEdges},

		{"sn2: a recursive or rule with no base rule", reachWithZero, `[:find ?p ?q :in $ % :where (reach ?p ?q)]`, nil, false, "", []string{"1 0", "2 0", "3 0", "4 0"}},
		{"sn2: the same or split into two rules", reachWithZeroSplit, `[:find ?p ?q :in $ % :where (reach ?p ?q)]`, nil, false, "", []string{"1 0", "2 0", "3 0", "4 0"}},

		{"unsafe: a predicate written before the clause binding its variable, whole", predicateBeforeBinder, `[:find ?p ?q :in $ % :where (t ?p ?q)]`, nil, false, "", []string{"1 2", "1 3", "2 2", "2 3", "3 2", "3 3", "4 2", "4 3"}},
		{"unsafe: the same bound", predicateBeforeBinder, `[:find ?p ?q :in $ % ?p :where (t ?p ?q)]`, []interface{}{one}, false, "", []string{"1 2", "1 3"}},
		{"unsafe: an expression written before the clause binding its input, whole", expressionBeforeBinder, `[:find ?p ?q :in $ % :where (t ?p ?q)]`, nil, false, "", rewriteClosure},
		{"unsafe: the same bound", expressionBeforeBinder, `[:find ?p ?q :in $ % ?p :where (t ?p ?q)]`, []interface{}{one}, false, "", []string{"1 2", "1 3", "1 4"}},
		{"unsafe: a not-join written before the clause binding its variable, whole", negationBeforeBinder, `[:find ?p ?q :in $ % :where (t ?p ?q)]`, nil, false, "", []string{"1 2", "1 4", "2 2", "2 4", "3 2", "3 4", "4 2", "4 4"}},
		{"unsafe: the same bound", negationBeforeBinder, `[:find ?p ?q :in $ % ?p :where (t ?p ?q)]`, []interface{}{one}, false, "", []string{"1 2", "1 4"}},
	}
	cases = append(cases, orRecursionCases("or: the recursion or a second edge", `(or (reach ?b ?c) (e ?b ?c))`, false, false)...)
	cases = append(cases, orRecursionCases("or: the recursion or a ground", `(or (reach ?b ?c) [(ground 0) ?c])`, true, false)...)
	cases = append(cases, orRecursionCases("or: the recursion or a red node's ground", `(or (reach ?b ?c) (and (red ?b) [(ground 0) ?c]))`, true, false)...)
	cases = append(cases, orRecursionCases("or: the recursion or a red pattern's ground", `(or (reach ?b ?c) (and [?n :node/id ?b] [?n :node/color "red"] [(ground 0) ?c]))`, true, false)...)
	cases = append(cases, orRecursionCases("or: the ground branch written first", `(or (and (red ?b) [(ground 0) ?c]) (reach ?b ?c))`, true, false)...)
	cases = append(cases, orRecursionCases("or: the ground written before the invocation in its branch", `(or (reach ?b ?c) (and [(ground 0) ?c] (red ?b)))`, true, false)...)
	cases = append(cases, orRecursionCases("or: a pattern binding the value the branch selects", `(or (reach ?b ?c) (and (red ?b) [?n :node/id ?c] [(= ?c 0)]))`, true, false)...)
	cases = append(cases, orRecursionCases("or: the ground branch written twice", `(or (reach ?b ?c) (and (red ?b) [(ground 0) ?c]) (and (red ?b) [(ground 0) ?c]))`, true, false)...)
	cases = append(cases, orRecursionCases("or: a branch reading the head's variable it does not bind", `(or (reach ?b ?c) (and (red ?b) [(ground 0) ?c2] [(= ?c ?c2)]))`, true, true)...)
	return cases
}

// Every isolated case of the magic rewrite answers its tuples, or is refused,
// on both planner paths: bindings through an input named as a rule's
// variable, a required predicate under a negation, inside the recursion and
// mutually recursive with a free one, existential negations inside branches,
// negations and fallbacks before and after the recursion, a hand-written
// magic stratum in every rule order, a required invocation written before
// its provider, and an or over the recursion bound at every position.
func TestRules_IsolatedRewriteCasesOnTheFourEdgeGraph(t *testing.T) {
	cases := rewriteCases()
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := writeRewriteGraph(t, d)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					var inputs []interface{}
					if c.rules != "" {
						rs, err := parser.ParseRules(c.rules)
						if c.refused && err != nil {
							if c.message != "" {
								require.Contains(t, err.Error(), c.message)
							}
							return
						}
						require.NoError(t, err)
						inputs = append(inputs, rs)
					}
					inputs = append(inputs, c.inputs...)
					if c.refused {
						_, err := executor.CollectTuples(d.Query(c.text, inputs...))
						require.Error(t, err, "the query is refused")
						if c.message != "" {
							require.Contains(t, err.Error(), c.message)
						}
						return
					}
					rel, err := d.Query(c.text, inputs...)
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, renderedTuples(t, rel, names))
				})
			}
		})
	}
}
