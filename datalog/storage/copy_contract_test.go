package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A query result answers whether its tuples must be copied from what its
// iterator does: a result over an iterator that fills one workspace per
// pull hands out the same slice after each pull and says so, and a result
// over an iterator that builds a tuple per pull, or reads from a slice it
// holds, hands out a different slice each time and says that. Each shape
// below is one route the matcher or the executor takes; the observation is
// the slice identity across two pulls, and the answer must match it.
func TestAQueryResultAnswersTheCopyContractFromItsIterator(t *testing.T) {
	facts := []fact{
		{named("e1"), ":t/n", int64(1)}, {named("e2"), ":t/n", int64(1)}, {named("e3"), ":t/n", int64(2)},
		{named("e1"), ":t/tag", "a"}, {named("e1"), ":t/tag", "b"}, {named("e2"), ":t/tag", "c"},
	}
	shapes := []struct {
		name, text string
		inputs     []interface{}
	}{
		{"an unbound scan", `[:find ?e ?v :where [?e :t/n ?v]]`, nil},
		{"a scan driven by a collection input", `[:find ?e ?v :in $ [?e ...] :where [?e :t/n ?v]]`, []interface{}{[]interface{}{named("e1").id, named("e2").id, named("e3").id}}},
		{"a bound value on a cardinality-one attribute", `[:find ?e :where [?e :t/n 1]]`, nil},
		{"a cardinality-many attribute with the entity unbound", `[:find ?e ?v :where [?e :t/tag ?v]]`, nil},
		{"a projection", `[:find ?v ?e :where [?e :t/n ?v]]`, nil},
		{"a sorted result", `[:find ?e ?v :where [?e :t/n ?v] :order-by [[?v :desc]]]`, nil},
		{"an aggregate", `[:find ?v (count ?e) :where [?e :t/n ?v]]`, nil},
	}
	s, err := schema.NewBuilder().
		Attribute(":t/n").Type(schema.TypeLong).Add().
		Attribute(":t/tag").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			commitFacts(t, d, facts)
			for _, shape := range shapes {
				t.Run(shape.name, func(t *testing.T) {
					rel, err := d.Query(shape.text, shape.inputs...)
					require.NoError(t, err)
					it := rel.Iterator()
					defer it.Close()
					require.True(t, it.Next(), "the shape answers at least two tuples")
					first := it.Tuple()
					require.NotEmpty(t, first)
					require.True(t, it.Next(), "the shape answers at least two tuples")
					second := it.Tuple()
					reused := &first[0] == &second[0]
					require.Equal(t, reused, rel.RequiresCopy(), "the result says its tuples must be copied exactly when its iterator hands out one slice per pull")
				})
			}
		})
	}
}
