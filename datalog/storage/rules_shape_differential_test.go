package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// The named rule shapes of the bound-versus-whole differential: each
// predicate is derived whole, then invoked with every subset of its positions
// bound, at values the whole answers and at random ones, through scalar
// inputs one combination at a time and through one relation input over every
// combination, and each bound invocation answers the whole evaluation's
// tuples at its binding (docs/reference/RULES.md, the evaluation section: an
// invocation with a bound argument derives only what the bindings reach). The
// graph is random: twelve nodes carrying :node/id, a colour and sometimes a
// seed, and eighteen edge entities carrying :edge/from, :edge/to and :edge/w
// as longs, so a node is named by its number at every position.

// shapeGraphNodes and shapeGraphEdges size a shape differential's graph.
const shapeGraphNodes, shapeGraphEdges = 12, 18

// shapeSeeds are the graphs the differential runs on, and shapeKeysPerMask
// bounds the combinations one mask is tried at.
var shapeSeeds = []int64{1, 2}

const shapeKeysPerMask = 5

// writeShapeGraph commits a graph drawn from r into d: each node's colour and
// whether it carries a seed, then the edges' ends and weights.
func writeShapeGraph(t testing.TB, d *Database, r *rand.Rand) {
	t.Helper()
	tx := d.NewTransaction()
	for i := 0; i < shapeGraphNodes; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("node%d", i))
		require.NoError(t, tx.Add(e, kw(":node/id"), int64(i)))
		color := "blue"
		if r.Intn(3) == 0 {
			color = "red"
		}
		require.NoError(t, tx.Add(e, kw(":node/color"), color))
		if r.Intn(4) == 0 {
			require.NoError(t, tx.Add(e, kw(":node/seed"), true))
		}
	}
	for j := 0; j < shapeGraphEdges; j++ {
		e := datalog.NewIdentity(fmt.Sprintf("edge%d", j))
		a, b := r.Intn(shapeGraphNodes), r.Intn(shapeGraphNodes)
		require.NoError(t, tx.Add(e, kw(":edge/from"), int64(a)))
		require.NoError(t, tx.Add(e, kw(":edge/to"), int64(b)))
		require.NoError(t, tx.Add(e, kw(":edge/w"), int64(r.Intn(10))))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// shapeBase is the predicates every shape's rule set opens with: an edge, a
// weighted edge, a red node and a node.
const shapeBase = `[(e ?a ?b) [?x :edge/from ?a] [?x :edge/to ?b]]
[(ew ?a ?b ?w) [?x :edge/from ?a] [?x :edge/to ?b] [?x :edge/w ?w]]
[(red ?a) [?n :node/id ?a] [?n :node/color "red"]]
[(node ?a) [?n :node/id ?a]]
`

// shapeCase is one predicate of a rule set, invoked at its arity.
type shapeCase struct {
	name, rules, predicate string
	arity                  int
}

// shapeCases are the forty-three shapes: the closures and their forms, the
// negations and fallbacks around a recursion, the required predicates under
// every form, and the clauses written before their binders.
var shapeCases = []shapeCase{
	{"right-linear", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)]]`, "reach", 2},
	{"left-linear", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (reach ?a ?b) (e ?b ?c)]]`, "reach", 2},
	{"nonlinear", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (reach ?a ?b) (reach ?b ?c)]]`, "reach", 2},
	{"mutual", "[" + shapeBase + `[(odd ?a ?b) (e ?a ?b)] [(odd ?a ?c) (e ?a ?b) (even ?b ?c)] [(even ?a ?c) (e ?a ?b) (odd ?b ?c)]]`, "even", 2},
	{"weighted-pred", "[" + shapeBase + `[(light ?a ?b) (ew ?a ?b ?w) [(< ?w 6)]] [(light ?a ?c) (ew ?a ?b ?w) [(< ?w 6)] (light ?b ?c)]]`, "light", 2},
	{"expr", "[" + shapeBase + `[(r ?a ?b ?k) (e ?a ?b) [(ground 0) ?k]] [(r ?a ?c ?k2) (e ?a ?b) (r ?b ?c ?k) [(+ ?k 1) ?k2] [(< ?k2 4)]]]`, "r", 3},
	{"ground-const", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(fromk ?y ?z) [(ground 3) ?k] (reach ?k ?y) (e ?y ?z)]]`, "fromk", 2},
	{"const-inv", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(both ?y ?z) (reach 2 ?y) (reach ?y ?z)]]`, "both", 2},
	{"not-pattern", "[" + shapeBase + `[(safe ?a ?b) (e ?a ?b) (not (red ?b))] [(safe ?a ?c) (e ?a ?b) (not [?n :node/id ?b] [?n :node/color "red"]) (safe ?b ?c)]]`, "safe", 2},
	{"not-adorned", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(oneway ?a ?b) (reach ?a ?b) (not (reach ?b ?a))]]`, "oneway", 2},
	{"not-join-adorned", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(sink ?a ?b) (reach ?a ?b) (not-join [?b] (reach ?b _))]]`, "sink", 2},
	{"or", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (or (reach ?b ?c) (and (red ?b) [(ground 0) ?c]))]]`, "reach", 2},
	{"or-join", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (or-join [?b ?c] (reach ?b ?c) (and (e ?c ?b) (red ?c)))]]`, "reach", 2},
	{"or-default", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(lbl ?a ?l) (node ?a) (or-default (and (reach ?a ?a) [(ground "cyc") ?l]) [(ground "acyc") ?l])]]`, "lbl", 2},
	{"subquery", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(fan ?a ?n) (node ?a) [(q [:find (count ?y) :in $ % ?a :where (reach ?a ?y)] $ % ?a) [[?n]]]]]`, "fan", 2},
	{"self-loop-repeat", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(cyc ?a ?b) (reach ?a ?a) (e ?a ?b)]]`, "cyc", 2},
	{"same-gen", "[" + shapeBase + `[(sg ?x ?y) (node ?x) [(identity ?x) ?y]] [(sg ?x ?y) (e ?px ?x) (sg ?px ?py) (e ?py ?y)]]`, "sg", 2},
	{"triple-ffb", "[" + shapeBase + `[(p3 ?a ?b ?c) (e ?a ?b) (e ?b ?c)] [(p3 ?a ?b ?c) (e ?a ?x) (p3 ?x ?b ?c)]]`, "p3", 3},
	{"mixed-bound-free", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(hub ?y) (reach ?z ?y) (reach ?y ?z)] [(q ?a ?b) (reach ?a ?b) (not (hub ?b))]]`, "q", 2},
	{"walk-not-declined", "[" + shapeBase + `[(walk ?a ?b) (e ?a ?b)] [(walk ?a ?c) (e ?a ?b) (not (red ?b)) (walk ?b ?c)]]`, "walk", 2},
	{"walk-notjoin-declined", "[" + shapeBase + `[(walk ?a ?b) (e ?a ?b)] [(walk ?a ?c) (e ?a ?b) (not-join [?b] (red ?b)) (walk ?b ?c)]]`, "walk", 2},
	{"or-default-recursive", "[" + shapeBase + `[(walk ?a ?b ?l) (e ?a ?b) [(ground "e") ?l]] [(walk ?a ?c ?l) (e ?a ?b) (or-default (and (red ?b) [(ground "r") ?l]) [(ground "b") ?l]) (walk ?b ?c _)]]`, "walk", 3},
	{"required-head", "[" + shapeBase + `[(succ [?n] ?m) [(+ ?n 1) ?m]] [(stepw ?a ?b) (e ?a ?x) (succ ?x ?b)] [(stepw ?a ?c) (e ?a ?x) (succ ?x ?y) (stepw ?y ?c) [(< ?c 20)]]]`, "stepw", 2},
	{"pred-before", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(t ?a ?c) [(< ?w 5)] (reach ?a ?c) (e ?w ?c)]]`, "t", 2},
	{"expr-before", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(t ?a ?d) [(+ ?w 0) ?w2] (reach ?a ?d) (e ?w ?d) [(= ?w ?w2)]]]`, "t", 2},
	{"notjoin-before", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(t ?a ?c) (not-join [?w] (red ?w)) (reach ?a ?c) (e ?w ?c)]]`, "t", 2},
	{"pred-before-recursive", "[" + shapeBase + `[(w ?a ?b) (ew ?a ?b ?k)] [(w ?a ?c) [(< ?k 6)] (w ?b ?c) (ew ?a ?b ?k)]]`, "w", 2},
	{"neg-after-rec", "[" + shapeBase + `[(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (not (red ?w)) (e ?w ?z)]]`, "q", 2},
	{"neg-before-rec-var", "[" + shapeBase + `[(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (not (red ?y)) (e ?w ?z)]]`, "q", 2},
	{"neg-after-rec-chain", "[" + shapeBase + `[(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (e ?w ?u) (not (red ?u)) (e ?w ?z)]]`, "q", 2},
	{"notjoin-after-rec", "[" + shapeBase + `[(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (not-join [?w ?y] (e ?w ?y)) (e ?w ?z)]]`, "q", 2},
	{"neg-in-or-after-rec", "[" + shapeBase + `[(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?z) (or (red ?z) (not (red ?y)))]]`, "q", 2},
	{"neg-lower-adorned", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (q ?y ?w) (not (reach ?w ?x)) (e ?w ?z)]]`, "q", 2},
	{"neg-lower-before", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(q ?x ?z) (e ?x ?z)] [(q ?x ?z) (e ?x ?y) (not (reach ?y ?x)) (q ?y ?w) (e ?w ?z)]]`, "q", 2},
	{"two-not-alike", "[" + shapeBase + `[(reach ?a ?b) (e ?a ?b)] [(reach ?a ?c) (e ?a ?b) (reach ?b ?c)] [(t ?a ?b) (e ?a ?b) (not (reach ?b ?a)) (not (reach ?a ?a))]]`, "t", 2},
	{"req-under-not", "[" + shapeBase + `[(succ [?n] ?m) [(+ ?n 1) ?m]] [(nr ?a ?b) (e ?a ?b) (not (succ ?a ?b))]]`, "nr", 2},
	{"req-under-ordefault", "[" + shapeBase + `[(succ [?n] ?m) [(+ ?n 1) ?m]] [(lab ?a ?l) (node ?a) (or-default (and (succ ?a ?m) (node ?m) [(ground "s") ?l]) [(ground "n") ?l])]]`, "lab", 2},
	{"req-under-or", "[" + shapeBase + `[(succ [?n] ?m) [(+ ?n 1) ?m]] [(o ?a ?b) (e ?a ?b) (or (succ ?a ?b) (succ ?b ?a))]]`, "o", 2},
	{"req-recursive-wrapper", "[" + shapeBase + `[(dn [?n] ?m) [(> ?n 0)] [(- ?n 1) ?m]] [(dn [?n] ?m) [(> ?n 0)] [(- ?n 1) ?k] (dn ?k ?m)] [(below ?a ?m) (node ?a) [(< ?a 6)] (dn ?a ?m)]]`, "below", 2},
	{"req-mutual-wrapper", "[" + shapeBase + `[(ev [?n] ?r) [(= ?n 0)] [(ground 1) ?r]] [(ev [?n] ?r) [(> ?n 0)] [(- ?n 1) ?m] (od ?m ?r)] [(od [?n] ?r) [(> ?n 0)] [(- ?n 1) ?m] (ev ?m ?r)] [(evn ?a ?r) (node ?a) (ev ?a ?r)]]`, "evn", 2},
	{"req-in-recursion", "[" + shapeBase + `[(succ [?n] ?m) [(+ ?n 1) ?m]] [(st ?a ?b) (e ?a ?b)] [(st ?a ?c) (e ?a ?x) (succ ?x ?y) (st ?y ?c) [(< ?c 20)]]]`, "st", 2},
	{"req-chain-body", "[" + shapeBase + `[(succ [?n] ?m) [(+ ?n 1) ?m]] [(two [?n] ?m) (succ ?n ?k) (succ ?k ?m)] [(r ?a ?b) (e ?a ?x) (two ?x ?b)]]`, "r", 2},
	{"req-before-binder", "[" + shapeBase + `[(succ [?n] ?m) [(+ ?n 1) ?m]] [(stepw ?a ?b) (succ ?x ?b) (e ?a ?x)]]`, "stepw", 2},
}

// plainRows reads a query's result into one string per tuple, its values
// printed and joined by ",", sorted; the query's own error is returned before
// anything is read.
func plainRows(rel executor.Relation, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	var out []string
	err = executor.ForEach(rel, func(tuple executor.Tuple) error {
		parts := make([]string, len(tuple))
		for i, v := range tuple {
			parts[i] = fmt.Sprint(v)
		}
		out = append(out, strings.Join(parts, ","))
		return nil
	})
	sort.Strings(out)
	return out, err
}

// rowsAt keeps the whole evaluation's rows whose values at positions equal
// one of the combinations, rendered as plainRows renders them.
func rowsAt(whole [][]string, positions []int, combinations [][]int64) []string {
	var out []string
	for _, row := range whole {
		for _, values := range combinations {
			agrees := true
			for j, p := range positions {
				if row[p] != fmt.Sprint(values[j]) {
					agrees = false
					break
				}
			}
			if agrees {
				out = append(out, strings.Join(row, ","))
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// Every shape's predicate, invoked with each subset of its positions bound
// through scalar inputs and through one relation input, answers on a random
// graph what its whole evaluation answers at that binding, in every mode. A
// whole evaluation that fails is a failure.
func TestRules_BoundInvocationsOfTheNamedShapesMatchTheWholeEvaluation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, seed := range shapeSeeds {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				writeShapeGraph(t, d, rand.New(rand.NewSource(seed)))
				r := rand.New(rand.NewSource(seed * 7))
				for _, c := range shapeCases {
					label := fmt.Sprintf("seed %d %s", seed, c.name)
					rs := mustRuleSet(t, c.rules)
					head := make([]string, c.arity)
					for i := range head {
						head[i] = fmt.Sprintf("?v%d", i)
					}
					variables := strings.Join(head, " ")
					rendered, err := plainRows(d.Query(fmt.Sprintf(`[:find %s :in $ %% :where (%s %s)]`, variables, c.predicate, variables), rs))
					require.NoError(t, err, "%s: the whole evaluation answers", label)
					whole := make([][]string, len(rendered))
					for i, row := range rendered {
						whole[i] = strings.Split(row, ",")
					}
					for mask := 1; mask < 1<<c.arity; mask++ {
						var bound []string
						var positions []int
						for i := 0; i < c.arity; i++ {
							if mask&(1<<i) != 0 {
								bound = append(bound, head[i])
								positions = append(positions, i)
							}
						}
						combinations := map[string][]int64{}
						for _, row := range whole {
							var values []int64
							key := ""
							for _, p := range positions {
								var x int64
								fmt.Sscan(row[p], &x)
								values = append(values, x)
								key += row[p] + ","
							}
							combinations[key] = values
						}
						for k := 0; k < 3; k++ {
							var values []int64
							key := ""
							for range positions {
								x := int64(r.Intn(14))
								values = append(values, x)
								key += fmt.Sprint(x) + ","
							}
							combinations[key] = values
						}
						keys := make([]string, 0, len(combinations))
						for k := range combinations {
							keys = append(keys, k)
						}
						sort.Strings(keys)
						if len(keys) > shapeKeysPerMask {
							r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
							keys = keys[:shapeKeysPerMask]
						}
						scalar := fmt.Sprintf(`[:find %s :in $ %% %s :where (%s %s)]`, variables, strings.Join(bound, " "), c.predicate, variables)
						var every [][]int64
						var relation [][]interface{}
						for _, k := range keys {
							values := combinations[k]
							inputs := []interface{}{rs}
							tuple := []interface{}{}
							for _, x := range values {
								inputs = append(inputs, x)
								tuple = append(tuple, x)
							}
							every = append(every, values)
							relation = append(relation, tuple)
							got, err := plainRows(d.Query(scalar, inputs...))
							require.NoError(t, err, "%s mask %b at %v", label, mask, values)
							require.Equal(t, rowsAt(whole, positions, [][]int64{values}), got, "%s mask %b at %v: the bound invocation answers the whole evaluation at its binding", label, mask, values)
						}
						got, err := plainRows(d.Query(fmt.Sprintf(`[:find %s :in $ %% [[%s] ...] :where (%s %s)]`, variables, strings.Join(bound, " "), c.predicate, variables), rs, relation))
						require.NoError(t, err, "%s mask %b as a relation input", label, mask)
						require.Equal(t, rowsAt(whole, positions, every), got, "%s mask %b as a relation input: the invocation answers the whole evaluation at every combination", label, mask)
					}
				}
			}
		})
	}
}
