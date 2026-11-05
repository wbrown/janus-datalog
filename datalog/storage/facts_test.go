package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A database OpenFacts opens holds every fact it is given: a pattern answers
// each, an attribute at which one entity holds two values answers both, and a
// definition the given schema makes stands.
func TestOpenFactsHoldsEveryFactItIsGiven(t *testing.T) {
	one, two := datalog.NewIdentity("facts:one"), datalog.NewIdentity("facts:two")
	name, tag, rank := kw(":facts/name"), kw(":facts/tag"), kw(":facts/rank")
	facts := []datalog.Datom{
		{E: one, A: name, V: "one"},
		{E: one, A: tag, V: "red"},
		{E: one, A: tag, V: "blue"},
		{E: two, A: name, V: "two"},
		{E: two, A: tag, V: "red"},
		{E: one, A: rank, V: int64(1)},
		{E: one, A: rank, V: int64(2)},
	}
	given, err := schema.NewBuilder().Attribute(":facts/rank").Type(schema.TypeLong).One().Add().Build()
	require.NoError(t, err)
	d, err := OpenFacts(facts, DatabaseOptions{Schema: given})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	names, err := executor.CollectTuples(d.Query(`[:find ?e ?n :where [?e :facts/name ?n]]`))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{one, "one"}, {two, "two"}}, names, "a single-valued attribute answers its value")

	tags, err := executor.CollectTuples(d.Query(`[:find ?e ?t :where [?e :facts/tag ?t]]`))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{one, "red"}, {one, "blue"}, {two, "red"}}, tags, "an attribute one entity holds two values of answers both")

	ranks, err := executor.CollectTuples(d.Query(`[:find ?r :where [?e :facts/rank ?r]]`))
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, ranks, "the given schema's cardinality-one definition stands, so the later fact supersedes the earlier")
}

// A datom carrying an op is a log's record, which OpenFacts refuses.
func TestOpenFactsRefusesALogsRecord(t *testing.T) {
	record := datalog.Datom{E: datalog.NewIdentity("facts:one"), A: kw(":facts/tag"), V: "red", Op: datalog.OpCRDTRemove}
	_, err := OpenFacts([]datalog.Datom{record}, DatabaseOptions{})
	require.ErrorContains(t, err, "a log opens from its dump")
}

// A vector holds only at a cardinality-vector attribute, so at an attribute
// the given schema leaves undefined each entity holds its vector whole, its
// elements in their order. The same vector given twice at one entity is one
// fact.
func TestOpenFactsHoldsEachEntitysVectorWhole(t *testing.T) {
	one, two := datalog.NewIdentity("facts:one"), datalog.NewIdentity("facts:two")
	tags := kw(":facts/tags")
	facts := []datalog.Datom{
		{E: one, A: tags, V: []interface{}{int64(1), int64(2)}},
		{E: two, A: tags, V: []interface{}{int64(2), int64(1), int64(3)}},
		{E: one, A: tags, V: []interface{}{int64(1), int64(2)}},
	}
	d, err := OpenFacts(facts, DatabaseOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	vectors, err := executor.CollectTuples(d.Query(`[:find ?e ?v :where [?e :facts/tags ?v]]`))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{one, []int64{1, 2}},
		{two, []int64{2, 1, 3}},
	}, vectors, "each entity answers its one vector, its elements in their order")
}

// Two different vectors at one entity cannot both hold, and neither can a
// vector and a scalar at one attribute. OpenFacts refuses each.
func TestOpenFactsRefusesVectorFactsThatCannotBothHold(t *testing.T) {
	one, two := datalog.NewIdentity("facts:one"), datalog.NewIdentity("facts:two")
	tags := kw(":facts/tags")
	for _, tc := range []struct {
		name    string
		facts   []datalog.Datom
		refusal string
	}{
		{
			name: "two vectors at one entity",
			facts: []datalog.Datom{
				{E: one, A: tags, V: []interface{}{int64(1)}},
				{E: one, A: tags, V: []interface{}{int64(2)}},
			},
			refusal: "an entity holds one vector at an attribute",
		},
		{
			name: "a vector and then a scalar at one attribute",
			facts: []datalog.Datom{
				{E: one, A: tags, V: []interface{}{int64(1)}},
				{E: two, A: tags, V: int64(2)},
			},
			refusal: "an attribute holds vectors or scalars, not both",
		},
		{
			name: "a scalar and then a vector at one attribute",
			facts: []datalog.Datom{
				{E: one, A: tags, V: int64(2)},
				{E: two, A: tags, V: []interface{}{int64(1)}},
			},
			refusal: "an attribute holds vectors or scalars, not both",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OpenFacts(tc.facts, DatabaseOptions{})
			require.ErrorContains(t, err, tc.refusal)
		})
	}
}

// Scalar facts at an attribute the given schema declares cardinality-vector
// are the entity's vector, in the facts' order, as Transaction.Add appends
// them. At an ordered set, a value the vector already holds is written once.
func TestOpenFactsAppendsScalarFactsToADeclaredVector(t *testing.T) {
	one := datalog.NewIdentity("facts:one")
	steps, prefs := kw(":facts/steps"), kw(":facts/prefs")
	given, err := schema.NewBuilder().
		Attribute(":facts/steps").Type(schema.TypeString).Vector().Add().
		Attribute(":facts/prefs").Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)
	facts := []datalog.Datom{
		{E: one, A: steps, V: "a"},
		{E: one, A: prefs, V: "x"},
		{E: one, A: steps, V: "b"},
		{E: one, A: prefs, V: "y"},
		{E: one, A: steps, V: "a"},
		{E: one, A: prefs, V: "x"},
	}
	d, err := OpenFacts(facts, DatabaseOptions{Schema: given})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	for _, c := range []struct {
		query string
		want  []string
	}{
		{`[:find ?v :where [_ :facts/steps ?v]]`, []string{"a", "b", "a"}},
		{`[:find ?v :where [_ :facts/prefs ?v]]`, []string{"x", "y"}},
	} {
		got, err := executor.CollectTuples(d.Query(c.query))
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{c.want}}, got, c.query)
	}
}

// A fact at one of the engine's words is refused, as Transaction.Add refuses
// it: the engine alone writes them.
func TestOpenFactsRefusesTheEnginesWords(t *testing.T) {
	one := datalog.NewIdentity("facts:one")
	_, err := OpenFacts([]datalog.Datom{{E: one, A: kw(":db/ident"), V: kw(":facts/name")}}, DatabaseOptions{})
	require.ErrorContains(t, err, "the engine alone writes it")
}
