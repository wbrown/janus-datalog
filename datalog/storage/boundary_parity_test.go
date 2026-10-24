package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The parser, the builder and Prepare accept and refuse the same text with
// one message (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md goal 5 and
// §6): each case is one query given as text to ParseQuery, as a builder chain
// to Build where the builder spells it, and hand-built to Prepare, and the
// verdicts and the messages are one. An accepted text is accepted again by
// Prepare after each text boundary.

// boundaryCase is one text with its spellings at the three boundaries.
type boundaryCase struct {
	name    string
	text    string
	build   func() (*query.Query, error) // nil where the builder has no spelling of the text
	hand    *query.Query
	refused bool
}

// followsOverPages is the rule set every case invoking a predicate runs with.
const followsOverPages = `[[(follows ?a ?b) [?a :page/next ?b]]]`

func boundarySym(name string) query.Symbol { return datalog.NewSymbol(name) }

func boundaryVar(name string) query.Variable { return query.Variable{Name: boundarySym(name)} }

// handFindIn is the hand-built [:find find... :in $ in... :where where...].
func handFindIn(find []string, in []query.InputSpec, where ...query.Clause) *query.Query {
	q := &query.Query{In: append([]query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}}, in...), Where: where}
	for _, name := range find {
		q.Find = append(q.Find, query.FindVariable{Symbol: boundarySym(name)})
	}
	return q
}

// pageNextPattern is the hand-built [?x :page/next ?y].
func pageNextPattern(x, y string) *query.DataPattern {
	return &query.DataPattern{Elements: []query.PatternElement{boundaryVar(x), query.Constant{Value: datalog.NewKeyword(":page/next")}, boundaryVar(y)}}
}

// pageKindPattern is the hand-built [?e :page/kind ?k].
func pageKindPattern(e, k string) *query.DataPattern {
	return &query.DataPattern{Elements: []query.PatternElement{boundaryVar(e), query.Constant{Value: datalog.NewKeyword(":page/kind")}, boundaryVar(k)}}
}

// followsInvocation is the hand-built (follows ?x ?y).
func followsInvocation(x, y string) *query.RuleInvocation {
	return &query.RuleInvocation{Name: boundarySym("follows"), Args: []query.PatternElement{boundaryVar(x), boundaryVar(y)}}
}

// collectingY is the hand-built [(q inner inputs...) [?y ...]].
func collectingY(inner *query.Query, inputs ...query.PatternElement) *query.SubqueryPattern {
	return &query.SubqueryPattern{Query: inner, Inputs: inputs, Binding: query.CollectionBinding{Variable: boundarySym("?y")}}
}

// groundOf is the hand-built [(ground value) binding].
func groundOf(value interface{}, binding interface{}) *query.Expression {
	return &query.Expression{Function: &query.GroundFunction{Value: value}, Binding: binding}
}

func boundaryCases() []boundaryCase {
	dollar := query.Constant{Value: datalog.SymDollar}
	rules := query.Constant{Value: datalog.SymRules}
	scalarX := query.ScalarInput{Symbol: boundarySym("?x")}
	pageNext, pageKind := qb.Kw(":page/next"), qb.Kw(":page/kind")
	one, two, three := int64(1), int64(2), int64(3)
	limit := 1

	return []boundaryCase{
		{
			name: "an inner :in declaring % twice is refused",
			text: `[:find ?y :in $ % ?x :where [(q [:find ?y :in $ % % ?x :where (follows ?x ?y)] $ % % ?x) [?y ...]]]`,
			build: func() (*query.Query, error) {
				x, y := qb.NewVar("x"), qb.NewVar("y")
				inner := qb.Query().Find(y).In(qb.DB, qb.Rules, qb.Rules, qb.Scalar(x)).Where(qb.Rule("follows", x, y))
				return qb.Query().Find(y).In(qb.DB, qb.Rules, qb.Scalar(x)).Where(qb.Subquery(inner, x).BindCollection(y)).Build()
			},
			hand: handFindIn([]string{"?y"}, []query.InputSpec{query.RulesInput{}, scalarX},
				collectingY(handFindIn([]string{"?y"}, []query.InputSpec{query.RulesInput{}, query.RulesInput{}, scalarX}, followsInvocation("?x", "?y")),
					dollar, rules, rules, boundaryVar("?x"))),
			refused: true,
		},
		{
			name: "an inner invocation under an inner :in declaring no % is refused",
			text: `[:find ?y :in $ % ?x :where [(q [:find ?y :in $ ?x :where (follows ?x ?y)] $ ?x) [?y ...]]]`,
			build: func() (*query.Query, error) {
				x, y := qb.NewVar("x"), qb.NewVar("y")
				inner := qb.Query().Find(y).In(qb.DB, qb.Scalar(x)).Where(qb.Rule("follows", x, y))
				return qb.Query().Find(y).In(qb.DB, qb.Rules, qb.Scalar(x)).Where(qb.Subquery(inner, x).BindCollection(y)).Build()
			},
			hand: handFindIn([]string{"?y"}, []query.InputSpec{query.RulesInput{}, scalarX},
				collectingY(handFindIn([]string{"?y"}, []query.InputSpec{scalarX}, followsInvocation("?x", "?y")),
					dollar, boundaryVar("?x"))),
			refused: true,
		},
		{
			name: "an inner :limit is refused",
			text: `[:find ?y :in $ ?x :where [(q [:find ?y :in $ ?x :where [?x :page/next ?y] :limit 1] $ ?x) [?y ...]]]`,
			build: func() (*query.Query, error) {
				x, y := qb.NewVar("x"), qb.NewVar("y")
				inner := qb.Query().Find(y).In(qb.DB, qb.Scalar(x)).Where(qb.Pat(x, pageNext, y)).Limit(1)
				return qb.Query().Find(y).In(qb.DB, qb.Scalar(x)).Where(qb.Subquery(inner, x).BindCollection(y)).Build()
			},
			hand: func() *query.Query {
				inner := handFindIn([]string{"?y"}, []query.InputSpec{scalarX}, pageNextPattern("?x", "?y"))
				inner.Limit = &limit
				return handFindIn([]string{"?y"}, []query.InputSpec{scalarX}, collectingY(inner, dollar, boundaryVar("?x")))
			}(),
			refused: true,
		},
		{
			name: "an inner pull is refused",
			text: `[:find ?y :in $ ?x :where [(q [:find (pull ?y [:page/kind]) :in $ ?x :where [?x :page/next ?y]] $ ?x) [?y ...]]]`,
			build: func() (*query.Query, error) {
				x, y := qb.NewVar("x"), qb.NewVar("y")
				inner := qb.Query().Find(qb.Pull(y, pageKind)).In(qb.DB, qb.Scalar(x)).Where(qb.Pat(x, pageNext, y))
				return qb.Query().Find(y).In(qb.DB, qb.Scalar(x)).Where(qb.Subquery(inner, x).BindCollection(y)).Build()
			},
			hand: handFindIn([]string{"?y"}, []query.InputSpec{scalarX},
				collectingY(&query.Query{
					Find:  []query.FindElement{query.FindPull{Variable: boundarySym("?y"), Pattern: &query.PullPattern{Specs: []query.PullAttrSpec{&query.PullAttribute{Attr: datalog.NewKeyword(":page/kind")}}}}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, scalarX},
					Where: []query.Clause{pageNextPattern("?x", "?y")},
				}, dollar, boundaryVar("?x"))),
			refused: true,
		},
		{
			name: "a sort key nothing binds is refused",
			text: `[:find ?e :where [?e :page/kind ?k] :order-by [?z]]`,
			build: func() (*query.Query, error) {
				e, k, z := qb.NewVar("e"), qb.NewVar("k"), qb.NewVar("z")
				return qb.Query().Find(e).Where(qb.Pat(e, pageKind, k)).OrderBy(qb.Asc(z)).Build()
			},
			hand: func() *query.Query {
				q := handFindIn([]string{"?e"}, nil, pageKindPattern("?e", "?k"))
				q.OrderBy = []query.OrderByClause{{Variable: boundarySym("?z")}}
				return q
			}(),
			refused: true,
		},
		{
			name: "a sort key under an aggregate that is no group key is refused",
			text: `[:find (count ?e) :where [?e :page/kind ?k] :order-by [?k]]`,
			build: func() (*query.Query, error) {
				e, k := qb.NewVar("e"), qb.NewVar("k")
				return qb.Query().Find(qb.Count(e)).Where(qb.Pat(e, pageKind, k)).OrderBy(qb.Asc(k)).Build()
			},
			hand: &query.Query{
				Find:    []query.FindElement{query.FindAggregate{Function: datalog.SymCount, Arg: boundarySym("?e")}},
				In:      []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
				Where:   []query.Clause{pageKindPattern("?e", "?k")},
				OrderBy: []query.OrderByClause{{Variable: boundarySym("?k")}},
			},
			refused: true,
		},
		{
			name:    "one value under the tuple form is refused",
			text:    `[:find ?p :where [?e :page/kind ?k] [(ground 5) [?p ?q]]]`,
			hand:    handFindIn([]string{"?p"}, nil, pageKindPattern("?e", "?k"), groundOf(int64(5), query.TupleBinding{Variables: []query.Symbol{boundarySym("?p"), boundarySym("?q")}})),
			refused: true,
		},
		{
			name:    "a vector of values under a one-symbol relation form is refused",
			text:    `[:find ?p :where [?e :page/kind ?k] [(ground [1 2]) [[?p]]]]`,
			hand:    handFindIn([]string{"?p"}, nil, pageKindPattern("?e", "?k"), groundOf([]interface{}{one, two}, query.RelationBinding{Variables: []query.Symbol{boundarySym("?p")}})),
			refused: true,
		},
		{
			name:    "a vector of another count of values under the relation form is refused",
			text:    `[:find ?p :where [?e :page/kind ?k] [(ground [1 2 3]) [[?p ?q]]]]`,
			hand:    handFindIn([]string{"?p"}, nil, pageKindPattern("?e", "?k"), groundOf([]interface{}{one, two, three}, query.RelationBinding{Variables: []query.Symbol{boundarySym("?p"), boundarySym("?q")}})),
			refused: true,
		},
		{
			name:    "one value under the relation form is refused",
			text:    `[:find ?p :where [?e :page/kind ?k] [(ground 5) [[?p ?q]]]]`,
			hand:    handFindIn([]string{"?p"}, nil, pageKindPattern("?e", "?k"), groundOf(int64(5), query.RelationBinding{Variables: []query.Symbol{boundarySym("?p"), boundarySym("?q")}})),
			refused: true,
		},
		{
			name: "an empty vector under the tuple form is refused",
			text: `[:find ?p :where [?e :page/kind ?k] [(ground []) [?p]]]`,
			build: func() (*query.Query, error) {
				e, k, p := qb.NewVar("e"), qb.NewVar("k"), qb.NewVar("p")
				return qb.Query().Find(p).Where(qb.Pat(e, pageKind, k), qb.TupleGround().As(p)).Build()
			},
			hand:    handFindIn([]string{"?p"}, nil, pageKindPattern("?e", "?k"), groundOf([]interface{}{}, query.TupleBinding{Variables: []query.Symbol{boundarySym("?p")}})),
			refused: true,
		},
		{
			name:    "a relation ground with a row that is no vector is refused",
			text:    `[:find ?p :where [?e :page/kind ?k] [(ground [[1 2] 3]) [[?p ?q]]]]`,
			hand:    handFindIn([]string{"?p"}, nil, pageKindPattern("?e", "?k"), groundOf([]interface{}{[]interface{}{one, two}, three}, query.RelationBinding{Variables: []query.Symbol{boundarySym("?p"), boundarySym("?q")}})),
			refused: true,
		},
		{
			name: "one value under the tuple form inside an or-join branch is refused",
			text: `[:find ?e ?a :where [?e :page/kind ?k] (or-join [?a] [(ground 1) [?a ?b]] [(ground 2) ?a])]`,
			hand: handFindIn([]string{"?e", "?a"}, nil, pageKindPattern("?e", "?k"), &query.OrJoinClause{
				OutputVars: []query.Symbol{boundarySym("?a")},
				Branches: [][]query.Clause{
					{groundOf(one, query.TupleBinding{Variables: []query.Symbol{boundarySym("?a"), boundarySym("?b")}})},
					{groundOf(two, boundarySym("?a"))},
				},
			}),
			refused: true,
		},
		{
			name: "one value under the tuple form inside a subquery body is refused",
			text: `[:find ?e ?a :where [?e :page/kind ?k] [(q [:find ?x :in $ ?e :where [(ground 1) [?x ?y]]] $ ?e) [[?a]]]]`,
			hand: handFindIn([]string{"?e", "?a"}, nil, pageKindPattern("?e", "?k"), &query.SubqueryPattern{
				Query:   handFindIn([]string{"?x"}, []query.InputSpec{query.ScalarInput{Symbol: boundarySym("?e")}}, groundOf(one, query.TupleBinding{Variables: []query.Symbol{boundarySym("?x"), boundarySym("?y")}})),
				Inputs:  []query.PatternElement{dollar, boundaryVar("?e")},
				Binding: query.RelationBinding{Variables: []query.Symbol{boundarySym("?a")}},
			}),
			refused: true,
		},
		{
			name: "a scalar binding over a two-element inner find is refused",
			text: `[:find ?e ?h :where [?e :page/kind ?k] [(q [:find ?n ?m :in $ ?e :where [?e :page/next ?n] [?e :page/kind ?m]] $ ?e) ?h]]`,
			hand: handFindIn([]string{"?e", "?h"}, nil, pageKindPattern("?e", "?k"), &query.SubqueryPattern{
				Query:   handFindIn([]string{"?n", "?m"}, []query.InputSpec{query.ScalarInput{Symbol: boundarySym("?e")}}, pageNextPattern("?e", "?n"), pageKindPattern("?e", "?m")),
				Inputs:  []query.PatternElement{dollar, boundaryVar("?e")},
				Binding: query.ScalarBinding{Variable: boundarySym("?h")},
			}),
			refused: true,
		},
		{
			name: "a collection binding over a two-element inner find is refused",
			text: `[:find ?e ?h :where [?e :page/kind ?k] [(q [:find ?n ?m :in $ ?e :where [?e :page/next ?n] [?e :page/kind ?m]] $ ?e) [?h ...]]]`,
			build: func() (*query.Query, error) {
				e, h, k, n, m := qb.NewVar("e"), qb.NewVar("h"), qb.NewVar("k"), qb.NewVar("n"), qb.NewVar("m")
				inner := qb.Query().Find(n, m).In(qb.DB, qb.Scalar(e)).Where(qb.Pat(e, pageNext, n), qb.Pat(e, pageKind, m))
				return qb.Query().Find(e, h).Where(qb.Pat(e, pageKind, k), qb.Subquery(inner, e).BindCollection(h)).Build()
			},
			hand: handFindIn([]string{"?e", "?h"}, nil, pageKindPattern("?e", "?k"), &query.SubqueryPattern{
				Query:   handFindIn([]string{"?n", "?m"}, []query.InputSpec{query.ScalarInput{Symbol: boundarySym("?e")}}, pageNextPattern("?e", "?n"), pageKindPattern("?e", "?m")),
				Inputs:  []query.PatternElement{dollar, boundaryVar("?e")},
				Binding: query.CollectionBinding{Variable: boundarySym("?h")},
			}),
			refused: true,
		},
		{
			name: "an inner query declaring % once is accepted",
			text: `[:find ?y :in $ % ?x :where [(q [:find ?y :in $ % ?x :where (follows ?x ?y)] $ % ?x) [?y ...]]]`,
			build: func() (*query.Query, error) {
				x, y := qb.NewVar("x"), qb.NewVar("y")
				inner := qb.Query().Find(y).In(qb.DB, qb.Rules, qb.Scalar(x)).Where(qb.Rule("follows", x, y))
				return qb.Query().Find(y).In(qb.DB, qb.Rules, qb.Scalar(x)).Where(qb.Subquery(inner, x).BindCollection(y)).Build()
			},
			hand: handFindIn([]string{"?y"}, []query.InputSpec{query.RulesInput{}, scalarX},
				collectingY(handFindIn([]string{"?y"}, []query.InputSpec{query.RulesInput{}, scalarX}, followsInvocation("?x", "?y")),
					dollar, rules, boundaryVar("?x"))),
		},
		{
			name: "a sort key the :where binds is accepted",
			text: `[:find ?e :where [?e :page/kind ?k] :order-by [?k]]`,
			build: func() (*query.Query, error) {
				e, k := qb.NewVar("e"), qb.NewVar("k")
				return qb.Query().Find(e).Where(qb.Pat(e, pageKind, k)).OrderBy(qb.Asc(k)).Build()
			},
			hand: func() *query.Query {
				q := handFindIn([]string{"?e"}, nil, pageKindPattern("?e", "?k"))
				q.OrderBy = []query.OrderByClause{{Variable: boundarySym("?k")}}
				return q
			}(),
		},
		{
			name: "a vector of as many values under the tuple form is accepted",
			text: `[:find ?p :where [?e :page/kind ?k] [(ground [1 2]) [?p ?q]]]`,
			hand: handFindIn([]string{"?p"}, nil, pageKindPattern("?e", "?k"), groundOf([]interface{}{one, two}, query.TupleBinding{Variables: []query.Symbol{boundarySym("?p"), boundarySym("?q")}})),
		},
		{
			name: "a sort key that is a collection input symbol the :where does not bind is accepted",
			text: `[:find ?e :in $ [?w ...] :where [?e :page/kind ?k] :order-by [?w]]`,
			build: func() (*query.Query, error) {
				e, k, w := qb.NewVar("e"), qb.NewVar("k"), qb.NewVar("w")
				return qb.Query().Find(e).In(qb.DB, qb.Collection(w)).Where(qb.Pat(e, pageKind, k)).OrderBy(qb.Asc(w)).Build()
			},
			hand: func() *query.Query {
				q := handFindIn([]string{"?e"}, []query.InputSpec{query.CollectionInput{Symbol: boundarySym("?w")}}, pageKindPattern("?e", "?k"))
				q.OrderBy = []query.OrderByClause{{Variable: boundarySym("?w")}}
				return q
			}(),
		},
	}
}

func TestBoundaryParity_EveryRefusalIsTheSameAtEveryBoundary(t *testing.T) {
	rules, err := parser.ParseRules(followsOverPages)
	require.NoError(t, err)

	for _, tc := range boundaryCases() {
		t.Run(tc.name, func(t *testing.T) {
			parsed, parseErr := parser.ParseQuery(tc.text)
			_, prepareErr := query.Prepare(tc.hand, rules)
			var built *query.Query
			var buildErr error
			if tc.build != nil {
				require.NotPanics(t, func() { built, buildErr = tc.build() })
			}

			if tc.refused {
				require.Error(t, parseErr, "ParseQuery refuses the text")
				require.Error(t, prepareErr, "Prepare refuses the hand-built query")
				require.Equal(t, parseErr.Error(), prepareErr.Error(), "ParseQuery and Prepare refuse with one message")
				if tc.build != nil {
					require.Error(t, buildErr, "Build refuses the builder chain")
					require.Equal(t, parseErr.Error(), buildErr.Error(), "ParseQuery and Build refuse with one message")
				}
				return
			}

			require.NoError(t, parseErr, "ParseQuery accepts the text")
			require.NoError(t, prepareErr, "Prepare accepts the hand-built query")
			_, err := query.Prepare(parsed, rules)
			require.NoError(t, err, "Prepare accepts the parsed query")
			if tc.build != nil {
				require.NoError(t, buildErr, "Build accepts the builder chain")
				_, err = query.Prepare(built, rules)
				require.NoError(t, err, "Prepare accepts the built query")
			}
		})
	}
}
