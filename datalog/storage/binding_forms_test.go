package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A symbol written in several places of a query is one symbol, and every
// binding form binds a relation over its symbols that the rest of the query
// joins (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.5,
// docs/proposals/RELATION_INPUT_PROOF.md §8): two :in entries naming a symbol
// are joined on it, a name repeated in an inner query's tuple input unifies
// and one repeated in its find is answered at both positions, a binding that
// unified feeds every consumer as any relation does, and a ground's shape the
// text admits binds as its value.

// pairedFacts: e1, e2 and e3 carry :p/n 1, 2 and 3 beside :p/m 1, 3 and 3,
// so e1 alone has :p/n equal to :p/m, and the :p/m value 3 is held twice.
func pairedFacts() []fact {
	e1, e2, e3 := named("e1"), named("e2"), named("e3")
	return []fact{
		{e1, ":p/n", int64(1)}, {e1, ":p/m", int64(1)},
		{e2, ":p/n", int64(2)}, {e2, ":p/m", int64(3)},
		{e3, ":p/n", int64(3)}, {e3, ":p/m", int64(3)},
	}
}

// Two :in entries naming one symbol bind two relations the query joins on it,
// whatever forms the two entries take: the query answers the entities whose
// value both entries carry, and nothing when the two carry no value in common.
func TestInputEntries_SharingASymbolAreJoinedOnIt(t *testing.T) {
	cases := []struct {
		name, text string
		inputs     []interface{}
		want       []string
	}{
		{"two scalars carrying one value", `[:find ?e :in $ ?x ?x :where [?e :p/n ?x]]`, []interface{}{int64(1), int64(1)}, []string{"e1"}},
		{"two scalars carrying two values", `[:find ?e :in $ ?x ?x :where [?e :p/n ?x]]`, []interface{}{int64(1), int64(2)}, nil},
		{"a scalar among a collection's members", `[:find ?e :in $ ?x [?x ...] :where [?e :p/n ?x]]`, []interface{}{int64(2), []interface{}{int64(2), int64(3)}}, []string{"e2"}},
		{"a scalar outside a collection's members", `[:find ?e :in $ ?x [?x ...] :where [?e :p/n ?x]]`, []interface{}{int64(1), []interface{}{int64(2), int64(3)}}, nil},
		{"a scalar agreeing with a tuple's position", `[:find ?e :in $ ?x [?x ?y] :where [?e :p/n ?x] [?e :p/m ?y]]`, []interface{}{int64(2), []interface{}{int64(2), int64(3)}}, []string{"e2"}},
		{"a scalar differing from a tuple's position", `[:find ?e :in $ ?x [?x ?y] :where [?e :p/n ?x] [?e :p/m ?y]]`, []interface{}{int64(1), []interface{}{int64(2), int64(3)}}, nil},
		{"a scalar selecting a relation's tuple", `[:find ?e :in $ ?x [[?x ?y] ...] :where [?e :p/n ?x] [?e :p/m ?y]]`, []interface{}{int64(2), []interface{}{[]interface{}{int64(1), int64(1)}, []interface{}{int64(2), int64(3)}}}, []string{"e2"}},
		{"a collection selecting a relation's tuples", `[:find ?e :in $ [?x ...] [[?x ?y] ...] :where [?e :p/n ?x] [?e :p/m ?y]]`, []interface{}{[]interface{}{int64(2)}, []interface{}{[]interface{}{int64(1), int64(1)}, []interface{}{int64(2), int64(3)}}}, []string{"e2"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, pairedFacts())
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					rel, err := d.Query(c.text, c.inputs...)
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, renderedTuples(t, rel, names))
				})
			}
		})
	}
}

// A name written twice inside an inner query is one name there as it is at
// the top level: a tuple input naming one symbol twice binds the calls whose
// two values agree, and a find naming one variable twice answers its value at
// both positions, under a scalar input, under a collection input, and when
// the variable is the input itself.
func TestInnerQuery_ARepeatedNameUnifiesInItsInAndRepeatsInItsFind(t *testing.T) {
	cases := []struct {
		name, text string
		inputs     []interface{}
		want       []string
	}{
		{"a tuple input naming one symbol twice", `[:find ?e ?a :where [?e :p/n ?n] [?e :p/m ?m] [(q [:find ?x :in $ [?x ?x] :where [(identity ?x) ?z]] $ ?n ?m) [[?a]]]]`, nil, []string{"e1 1", "e3 3"}},
		{"a find naming one variable twice", `[:find ?e ?a ?b :where [?e :p/n 1] [(q [:find ?n ?n :in $ ?e :where [?e :p/n ?n]] $ ?e) [[?a ?b]]]]`, nil, []string{"e1 1 1"}},
		{"a find naming its input twice", `[:find ?e ?a ?b :where [?e :p/n 1] [(q [:find ?x ?x :in $ ?x :where [?x :p/n _]] $ ?e) [[?a ?b]]]]`, nil, []string{"e1 e1 e1"}},
		{"a find naming a collection's member twice", `[:find ?a ?b :in $ ?vs :where [(q [:find ?x ?x :in $ [?x ...] :where [?e :p/n ?x]] $ ?vs) [[?a ?b]]]]`, []interface{}{[]interface{}{int64(1), int64(2)}}, []string{"1 1", "2 2"}},
		{"a binding naming one symbol twice over a collection's members", `[:find ?a :in $ ?vs :where [(q [:find ?x ?y :in $ [?x ...] :where [?e :p/n ?x] [?e :p/m ?y]] $ ?vs) [[?a ?a]]]]`, []interface{}{[]interface{}{int64(1), int64(2), int64(3)}}, []string{"1", "3"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, pairedFacts())
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					rel, err := d.Query(c.text, c.inputs...)
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, renderedTuples(t, rel, names))
				})
			}
		})
	}
}

// agreeingPair is a subquery binding each entity's :p/n and :p/m under one
// name, so the entities whose two values agree enter the scope under ?h.
const agreeingPair = `[(q [:find ?n ?m :in $ ?e :where [?e :p/n ?n] [?e :p/m ?m]] $ ?e) [[?h ?h]]]`

// agreeingTuples is a relation ground whose tuples name one symbol twice, so
// the tuples whose two values agree bind ?a.
const agreeingTuples = `[(ground [[1 1] [3 3] [2 9]]) [[?a ?a]]]`

// A binding that unified enters the scope as a relation over its one name,
// and every consumer after it reads that relation: a join, a count, a not, an
// or-join, a subquery and an expression over a subquery's binding, and a
// join, a not, a count and a subquery over an expression's binding. The
// expression binding's tuples 1 and 3 are both :p/m values, so the not drops
// every tuple, and the tuple 2 9 binds nothing. A binding naming the scope's
// own ?e at both positions unifies the entity with its number, which never
// agree, where the same two under distinct names bind every entity.
func TestUnifiedBinding_FeedsEveryConsumerAsARelation(t *testing.T) {
	cases := []struct {
		name, text string
		want       []string
	}{
		{"a scope-bound symbol at both positions of a binding", `[:find ?e :where [?e :p/n _] [(q [:find ?x ?n :in $ ?x :where [?x :p/n ?n]] $ ?e) [[?e ?e]]]]`, nil},
		{"a scope-bound symbol and a number under distinct names", `[:find ?e :where [?e :p/n _] [(q [:find ?x ?n :in $ ?x :where [?x :p/n ?n]] $ ?e) [[?e ?n]]]]`, []string{"e1", "e2", "e3"}},
		{"a subquery's binding joined", `[:find ?e ?h ?x :where [?e :p/n _] ` + agreeingPair + ` [?x :p/m ?h]]`, []string{"e1 1 e1", "e3 3 e2", "e3 3 e3"}},
		{"a subquery's binding counted", `[:find (count ?h) :where [?e :p/n _] ` + agreeingPair + `]`, []string{"2"}},
		{"a subquery's binding under a not", `[:find ?e ?h :where [?e :p/n _] ` + agreeingPair + ` (not [?x :p/n ?h] [?x :p/m 1])]`, []string{"e3 3"}},
		{"a subquery's binding under an or-join", `[:find ?e ?h ?y :where [?e :p/n _] ` + agreeingPair + ` (or-join [[?h] ?y] [?y :p/n ?h] [?y :p/m ?h])]`, []string{"e1 1 e1", "e3 3 e2", "e3 3 e3"}},
		{"a subquery's binding passed to a subquery", `[:find ?e ?h ?c :where [?e :p/n _] ` + agreeingPair + ` [(q [:find (count ?y) :in $ ?v :where [?y :p/m ?v]] $ ?h) [[?c]]]]`, []string{"e1 1 1", "e3 3 2"}},
		{"a subquery's binding read by an expression", `[:find ?e ?k :where [?e :p/n _] ` + agreeingPair + ` [(+ ?h 1) ?k]]`, []string{"e1 2", "e3 4"}},
		{"an expression's binding joined", `[:find ?a ?x :where [(ground [[1 1] [3 3] [1 2]]) [[?a ?a]]] [?x :p/n ?a]]`, []string{"1 e1", "3 e3"}},
		{"an expression's binding under a not", `[:find ?a :where ` + agreeingTuples + ` (not [?x :p/m ?a])]`, nil},
		{"an expression's binding counted", `[:find (count ?a) :where ` + agreeingTuples + `]`, []string{"2"}},
		{"an expression's binding passed to a subquery", `[:find ?a ?c :where [(ground [[1 1] [2 9]]) [[?a ?a]]] [(q [:find (count ?y) :in $ ?v :where [?y :p/n ?v]] $ ?a) [[?c]]]]`, []string{"1 1"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, pairedFacts())
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					rel, err := d.Query(c.text)
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, renderedTuples(t, rel, names))
				})
			}
		})
	}
}

// A function evaluated per tuple binds its value under a repeated name as a
// ground does: the tuple whose two positions agree binds, under the tuple
// form over subvec, under the relation form over enumerate, and under the
// tuple form over a scalar input's value beside a group.
func TestPerTupleFunction_ARepeatedBindingNameUnifies(t *testing.T) {
	cases := []struct {
		name, text string
		inputs     []interface{}
		want       []string
	}{
		{"subvec under the tuple form", `[:find ?a :in $ [?v ...] :where [(subvec ?v 0 2) [?a ?a]]]`, []interface{}{[]interface{}{[]interface{}{int64(1), int64(2)}, []interface{}{int64(3), int64(3)}}}, []string{"3"}},
		{"enumerate under the relation form", `[:find ?i :in $ [?v ...] :where [(enumerate ?v) [[?i ?i]]]]`, []interface{}{[]interface{}{[]interface{}{int64(5), int64(1)}}}, []string{"1"}},
		{"a scalar input's tuple under the tuple form beside a group", `[:find ?e ?a :in $ ?v :where [?e :p/n 1] [(identity ?v) [?a ?a]]]`, []interface{}{[]interface{}{int64(1), int64(2)}}, nil},
		{"a scalar input's agreeing tuple under the tuple form beside a group", `[:find ?e ?a :in $ ?v :where [?e :p/n 1] [(identity ?v) [?a ?a]]]`, []interface{}{[]interface{}{int64(1), int64(1)}}, []string{"e1 1"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, pairedFacts())
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					rel, err := d.Query(c.text, c.inputs...)
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, renderedTuples(t, rel, names))
				})
			}
		})
	}
}

// A ground's value binds as its shape has it: a vector of vectors under the
// relation form binds its tuples, and an empty vector under the relation form
// or the collection form binds nothing and answers no tuple; a vector of one
// vector under the tuple form of one symbol binds that one value, the inner
// vector, so a not over it drops no entity whose value is a number; and a
// scalar reaching the tuple form at run time, from an input the text cannot
// see the shape of, is the query's error.
func TestGround_AValueBindsAsItsShapeHasIt(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, pairedFacts())
			bound, err := d.Query(`[:find ?a ?b :where [(ground [[1 2]]) [[?a ?b]]]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"1 2"}, renderedTuples(t, bound, names), "a vector of vectors under the relation form binds its tuples")
			none, err := d.Query(`[:find ?a ?b :where [(ground []) [[?a ?b]]]]`)
			require.NoError(t, err)
			require.Empty(t, renderedTuples(t, none, names), "an empty vector under the relation form binds nothing")
			none, err = d.Query(`[:find ?a :where [(ground []) [?a ...]]]`)
			require.NoError(t, err)
			require.Empty(t, renderedTuples(t, none, names), "an empty vector under the collection form binds nothing")
			kept, err := d.Query(`[:find ?e :where [?e :p/n ?n] (not [(ground [[1 2]]) [?n]])]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"e1", "e2", "e3"}, renderedTuples(t, kept, names), "the tuple form of one binds the inner vector, which equals no entity's number")
			_, err = executor.CollectTuples(d.Query(`[:find ?a ?b :in $ ?v :where [(identity ?v) [?a ?b]]]`, int64(1)))
			require.Error(t, err, "a scalar under the tuple form at run time is the query's error")
		})
	}
}
