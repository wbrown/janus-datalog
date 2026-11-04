package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A database OpenFacts opens with no schema declares nothing. Its store holds
// no definition, it runs with the cardinality the store infers from each
// attribute's ops, and its dump, which carries the ops and no definition,
// answers each attribute alike when it is reopened with no schema.
func TestOpenFactsDeclaresNothingItIsNotGiven(t *testing.T) {
	one, two, three := datalog.NewIdentity("facts:one"), datalog.NewIdentity("facts:two"), datalog.NewIdentity("facts:three")
	name, tag, path := kw(":facts/name"), kw(":facts/tag"), kw(":facts/path")
	facts := []datalog.Datom{
		{E: one, A: name, V: "one"},
		{E: one, A: tag, V: "red"},
		{E: two, A: tag, V: "red"},
		{E: one, A: tag, V: "blue"},
		{E: three, A: path, V: []interface{}{"a", "b"}},
	}
	d, err := OpenFacts(facts, DatabaseOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	definitions, err := executor.CollectTuples(d.Query(`[:find ?a :where [?a :db.attribute/declaration _]]`))
	require.NoError(t, err)
	require.Empty(t, definitions, "the store holds no definition")

	for attribute, cardinality := range map[datalog.Keyword]datalog.Keyword{
		name: schema.CardinalityOne,
		tag:  schema.CardinalityMany,
		path: schema.CardinalityVector,
	} {
		definition := d.Schema().GetAttribute(attribute)
		require.NotNil(t, definition, "the database runs with a definition of %v", attribute)
		require.Equal(t, cardinality, definition.Cardinality, "the cardinality the store infers for %v", attribute)
	}

	var dump bytes.Buffer
	require.NoError(t, d.Export(&dump))
	reopened, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Dump: bytes.NewReader(dump.Bytes())})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	for _, query := range []string{
		`[:find ?e ?v :where [?e :facts/name ?v]]`,
		`[:find ?e ?v :where [?e :facts/tag ?v]]`,
		`[:find ?e ?v :where [?e :facts/path ?v]]`,
	} {
		want, err := executor.CollectTuples(d.Query(query))
		require.NoError(t, err)
		require.NotEmpty(t, want, "the database answers %s", query)
		got, err := executor.CollectTuples(reopened.Query(query))
		require.NoError(t, err)
		require.ElementsMatch(t, want, got, "the reopened dump answers %s alike", query)
	}
}
