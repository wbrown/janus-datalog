package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Every :in form binds a relation over its symbols that the query joins once
// (docs/proposals/RELATION_INPUT_PROOF.md §8): a collection, a tuple and a
// relation input are spellings of one relation, the find aggregates over the
// whole join grouped by its variables, a streaming result reads its own input
// tuples, and a bound invocation over a relation input rewrites once.

// countFacts: p, q and s carry :t/c 0, 1 and 1.
func countFacts() []fact {
	p, q, s := named("p"), named("q"), named("s")
	return []fact{{p, ":t/c", int64(0)}, {q, ":t/c", int64(1)}, {s, ":t/c", int64(1)}}
}

// A relation input with an aggregate alone in the find answers the aggregate
// over the whole join: the count of the entities whose :t/c is among the
// relation's values, 1 and 0, is one tuple, 3.
func TestRelationInput_APureAggregateIsOverTheWholeJoin(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, countFacts())
			rel, err := d.Query(`[:find (count ?e) :in $ [[?c] ...] :where [?e :t/c ?c]]`,
				[]interface{}{[]interface{}{int64(1)}, []interface{}{int64(0)}})
			require.NoError(t, err)
			require.Equal(t, []string{"3"}, renderedTuples(t, rel, names))
		})
	}
}

// A relation input whose symbol is among the find's variables answers the
// aggregate per group, one group per value of the input: one entity at :t/c
// 0 and two at :t/c 1.
func TestRelationInput_GroupKeysNamingTheInputAnswerPerTuple(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, countFacts())
			rel, err := d.Query(`[:find ?c (count ?e) :in $ [[?c] ...] :where [?e :t/c ?c]]`,
				[]interface{}{[]interface{}{int64(1)}, []interface{}{int64(0)}})
			require.NoError(t, err)
			require.Equal(t, []string{"0 1", "1 2"}, renderedTuples(t, rel, names))
		})
	}
}

// A collection input, a relation input of one symbol, a tuple input of one
// tuple and a scalar input bind one relation for the same values, so each
// answers the same tuples, with and without an aggregate: the values 1 and 0
// select every entity, three, and the value 1 alone selects q and s, two.
func TestInputForms_OneRuleForCollectionTupleAndRelation(t *testing.T) {
	type spelling struct {
		name, spec string
		value      interface{}
	}
	twoValues := []spelling{
		{"collection", "[?c ...]", []interface{}{int64(1), int64(0)}},
		{"relation", "[[?c] ...]", []interface{}{[]interface{}{int64(1)}, []interface{}{int64(0)}}},
	}
	oneValue := []spelling{
		{"scalar", "?c", int64(1)},
		{"tuple", "[?c]", []interface{}{int64(1)}},
		{"collection", "[?c ...]", []interface{}{int64(1)}},
		{"relation", "[[?c] ...]", []interface{}{[]interface{}{int64(1)}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, countFacts())
			for _, form := range twoValues {
				rel, err := d.Query("[:find ?e :in $ "+form.spec+" :where [?e :t/c ?c]]", form.value)
				require.NoError(t, err, form.name)
				require.Equal(t, []string{"p", "q", "s"}, renderedTuples(t, rel, names), "%s over two values", form.name)
				rel, err = d.Query("[:find (count ?e) :in $ "+form.spec+" :where [?e :t/c ?c]]", form.value)
				require.NoError(t, err, form.name)
				require.Equal(t, []string{"3"}, renderedTuples(t, rel, names), "%s over two values, counted", form.name)
			}
			for _, form := range oneValue {
				rel, err := d.Query("[:find ?e :in $ "+form.spec+" :where [?e :t/c ?c]]", form.value)
				require.NoError(t, err, form.name)
				require.Equal(t, []string{"q", "s"}, renderedTuples(t, rel, names), "%s over one value", form.name)
				rel, err = d.Query("[:find (count ?e) :in $ "+form.spec+" :where [?e :t/c ?c]]", form.value)
				require.NoError(t, err, form.name)
				require.Equal(t, []string{"2"}, renderedTuples(t, rel, names), "%s over one value, counted", form.name)
			}
		})
	}
}

// A relation-input query whose result streams answers from its own input
// tuples whatever the order it is read in: the find of the input symbols
// alone, over three entities the body scans and an :order-by on the entity
// the find does not carry, answers every one of fifty input tuples on every
// one of ten reads.
func TestRelationInput_AStreamingResultReadsItsOwnTuples(t *testing.T) {
	p, q, s := named("p"), named("q"), named("s")
	facts := []fact{{p, ":t/a", int64(1)}, {q, ":t/a", int64(2)}, {s, ":t/a", int64(3)}}
	var input []interface{}
	var want []string
	for i := int64(1); i <= 50; i++ {
		input = append(input, []interface{}{i, i + 1})
		want = append(want, fmt.Sprintf("%d %d", i, i+1))
	}
	sort.Strings(want)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			for read := 0; read < 10; read++ {
				rel, err := d.Query(`[:find ?j ?f :in $ [[?j ?f] ...] :where [?e :t/a ?v] :order-by [[?e :asc]]]`, input)
				require.NoError(t, err)
				require.Equal(t, want, renderedTuples(t, rel, names), "read %d", read)
			}
		})
	}
}

// scalarInputFacts: p, q and s carry :t/c 0, 1 and 1, the first two beside
// a :t/k.
func scalarInputFacts() []fact {
	p, q, s := named("p"), named("q"), named("s")
	return []fact{
		{p, ":t/c", int64(0)}, {p, ":t/k", int64(7)},
		{q, ":t/c", int64(1)}, {q, ":t/k", int64(8)},
		{s, ":t/c", int64(1)},
	}
}

// A scalar input is a relation of one tuple the query joins, as the same
// value written as a ground is: a scalar no clause reads, named in the find
// beside a count, is the product's one value beside the count of the whole
// body.
func TestScalarInput_ReadByNoClauseIsTheProductsValueBesideTheCount(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, scalarInputFacts())
			rel, err := d.Query(`[:find ?w (count ?e) :in $ ?w :where [?e :t/c _]]`, int64(5))
			require.NoError(t, err)
			require.Equal(t, []string{"5 3"}, renderedTuples(t, rel, names))
		})
	}
}

// A scalar input a branch reads, with the form's header omitting it, answers
// whatever the same value written as a ground answers, the two spellings
// being one relation.
func TestScalarInput_ABranchReadsItAsTheGroundSpellingDoes(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, scalarInputFacts())
			ground, err := d.Query(`[:find ?c ?k (count ?e) :where [(ground 1) ?c] [?e :t/c ?c] (or-join [?k] (and [?z :t/c ?c] [?z :t/k ?k]))]`)
			require.NoError(t, err)
			want := renderedTuples(t, ground, names)
			rel, err := d.Query(`[:find ?c ?k (count ?e) :in $ ?c :where [?e :t/c ?c] (or-join [?k] (and [?z :t/c ?c] [?z :t/k ?k]))]`, int64(1))
			require.NoError(t, err)
			require.Equal(t, want, renderedTuples(t, rel, names), "the input spelling answers as the ground spelling does")
		})
	}
}

// A bound invocation whose argument a relation input binds runs one rewrite
// and one evaluation over the whole input: the closure from n1 and from n3
// over a chain of five, with one magic-sets event.
func TestRules_ABoundInvocationOverARelationInputRunsOnce(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			counts := &ruleEventCounts{}
			opts := mode.plannerOptions()
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts, AnnotationHandler: counts.handle})
			g := writeGraph(t, d, chain(5))
			rel, err := d.Query(`[:find ?x ?y :in $ % [[?x] ...] :where (reach ?x ?y)]`, mustRuleSet(t, closureRules),
				[]interface{}{[]interface{}{nodeID("n1")}, []interface{}{nodeID("n3")}})
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"n1>n2", "n1>n3", "n1>n4", "n1>n5", "n3>n4", "n3>n5"}, g.tuples(t, rel))
			require.Equal(t, 1, counts.snapshot()[0], "the invocation rewrites once over the relation input; the events:\n%s", counts.events())
		})
	}
}

// Under an aggregating find every group at the find boundary is a factor of
// the relation the aggregate runs over, the groups the find does not read
// included: three members no clause reads beside three entities count nine,
// through a collection input and a relation input alike, as the same members
// written as a ground count.
func TestInputReadByNoClause_IsAFactorOfTheCount(t *testing.T) {
	spellings := []struct {
		name, spec string
		value      interface{}
	}{
		{"collection", "[?w ...]", []interface{}{int64(1), int64(2), int64(3)}},
		{"relation", "[[?w] ...]", []interface{}{[]interface{}{int64(1)}, []interface{}{int64(2)}, []interface{}{int64(3)}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, countFacts())
			ground, err := d.Query(`[:find (count ?e) :where [(ground [1 2 3]) [?w ...]] [?e :t/c _]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"9"}, renderedTuples(t, ground, names), "the ground spelling counts the product")
			for _, form := range spellings {
				rel, err := d.Query("[:find (count ?e) :in $ "+form.spec+" :where [?e :t/c _]]", form.value)
				require.NoError(t, err, form.name)
				require.Equal(t, []string{"9"}, renderedTuples(t, rel, names), "a %s input no clause reads is a factor of the count", form.name)
			}
		})
	}
}

// A where group the find does not read is a factor of the count: three
// entities carrying :t/c beside two carrying :t/k count six, and beside a
// pattern matching nothing count what a find over nothing counts.
func TestDisjointGroup_IsAFactorOfTheCount(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, scalarInputFacts())
			rel, err := d.Query(`[:find (count ?e) :where [?e :t/c _] [?z :t/k _]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"6"}, renderedTuples(t, rel, names), "the group the find does not read multiplies the count")
			nothing, err := d.Query(`[:find (count ?e) :where [?e :t/none _]]`)
			require.NoError(t, err)
			empty, err := d.Query(`[:find (count ?e) :where [?e :t/c _] [?z :t/none _]]`)
			require.NoError(t, err)
			require.Equal(t, renderedTuples(t, nothing, names), renderedTuples(t, empty, names), "an empty factor counts as a find over nothing")
		})
	}
}

// A relation input symbol the :where does not bind is bound by the input, so
// a sort on it orders the join: the values of the entities the input's first
// symbol keys, in the order of the tags its second symbol pairs them with.
func TestOrderBy_ARelationInputSymbolNoClauseBindsOrdersTheJoin(t *testing.T) {
	p, q, s := named("p"), named("q"), named("s")
	facts := []fact{
		{p, ":t/key", int64(1)}, {p, ":t/val", "first"},
		{q, ":t/key", int64(2)}, {q, ":t/val", "second"},
		{s, ":t/key", int64(3)}, {s, ":t/val", "third"},
	}
	input := []interface{}{
		[]interface{}{int64(1), int64(30)},
		[]interface{}{int64(2), int64(10)},
		[]interface{}{int64(3), int64(20)},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, facts)
			rel, err := d.Query(`[:find ?v :in $ [[?k ?tag] ...] :where [?e :t/key ?k] [?e :t/val ?v] :order-by [[?tag :asc]]]`, input)
			require.NoError(t, err)
			var values []interface{}
			require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
				values = append(values, tuple[0])
				return nil
			}))
			require.Equal(t, []interface{}{"second", "third", "first"}, values)
		})
	}
}

// A collection input no clause reads is a group the sort reads, so a sort on
// it is accepted and orders the product of the result with the input's
// members, and the projection onto the find keeps each value's first
// occurrence: sorted by the member ascending and then the value descending,
// the values come descending.
func TestOrderBy_ACollectionInputSymbolNoClauseReadsOrdersTheProduct(t *testing.T) {
	p, q, s := named("p"), named("q"), named("s")
	facts := []fact{{p, ":t/c", int64(1)}, {q, ":t/c", int64(2)}, {s, ":t/c", int64(3)}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, facts)
			rel, err := d.Query(`[:find ?c :in $ [?w ...] :where [?e :t/c ?c] :order-by [[?w :asc] [?c :desc]]]`, []interface{}{int64(7), int64(8)})
			require.NoError(t, err)
			var values []interface{}
			require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
				values = append(values, tuple[0])
				return nil
			}))
			require.Equal(t, []interface{}{int64(3), int64(2), int64(1)}, values)
		})
	}
}
