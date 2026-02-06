package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestBindQueryInputs_TwoCollectionsCrossProduct verifies that two collection
// inputs produce a cross-product when bound together.
func TestBindQueryInputs_TwoCollectionsCrossProduct(t *testing.T) {
	// Parse query with two collection inputs
	q, err := parser.ParseQuery(`[:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]`)
	require.NoError(t, err)

	// Create input relations manually (simulating what convertInputsToRelations does)
	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
	}
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":person/name"),
		datalog.NewKeyword(":person/age"),
	}

	// Create entity relation
	entityTuples := make([]Tuple, len(entities))
	for i, e := range entities {
		entityTuples[i] = Tuple{e}
	}
	entityRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")},
		entityTuples,
	)

	// Create attribute relation
	attrTuples := make([]Tuple, len(attrs))
	for i, a := range attrs {
		attrTuples[i] = Tuple{a}
	}
	attrRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?a")},
		attrTuples,
	)

	t.Logf("Entity relation - columns: %v, size: %d", entityRel.Columns(), entityRel.Size())
	it := entityRel.Iterator()
	for it.Next() {
		t.Logf("  Entity tuple: %v", it.Tuple())
	}
	it.Close()

	t.Logf("Attr relation - columns: %v, size: %d", attrRel.Columns(), attrRel.Size())
	it = attrRel.Iterator()
	for it.Next() {
		t.Logf("  Attr tuple: %v", it.Tuple())
	}
	it.Close()

	// Bind the inputs
	inputRelations := []Relation{entityRel, attrRel}
	bound := BindQueryInputs(q, inputRelations)

	t.Logf("Bound relation - columns: %v, size: %d", bound.Columns(), bound.Size())
	it = bound.Iterator()
	var tuples []Tuple
	for it.Next() {
		tuple := make(Tuple, len(it.Tuple()))
		copy(tuple, it.Tuple())
		tuples = append(tuples, tuple)
		t.Logf("  Bound tuple: %v", tuple)
	}
	it.Close()

	// Should have cross-product: 2 entities × 2 attrs = 4 tuples
	assert.Len(t, tuples, 4, "should produce cross-product of 2×2=4 tuples")

	// Verify columns
	assert.Equal(t, []query.Symbol{
		datalog.NewSymbol("?e"),
		datalog.NewSymbol("?a"),
	}, bound.Columns())
}

// TestBindQueryInputs_EmptyCollectionReturnsEmpty verifies that an empty
// collection input produces an empty bound relation.
func TestBindQueryInputs_EmptyCollectionReturnsEmpty(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v :in $ [?e ...] :where [?e :person/name ?v]]`)
	require.NoError(t, err)

	// Create empty entity relation
	entityRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")},
		[]Tuple{}, // Empty!
	)

	t.Logf("Empty entity relation - size: %d", entityRel.Size())

	inputRelations := []Relation{entityRel}
	bound := BindQueryInputs(q, inputRelations)

	t.Logf("Bound relation - columns: %v, size: %d", bound.Columns(), bound.Size())

	// An empty collection should result in empty bound relation (0 results)
	// because there are no values to match against
	assert.Equal(t, 0, bound.Size(), "empty collection should produce empty bound relation")
}

// TestBindQueryInputs_SingleCollection verifies single collection binding works.
func TestBindQueryInputs_SingleCollection(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v :in $ [?e ...] :where [?e :person/name ?v]]`)
	require.NoError(t, err)

	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
		datalog.NewIdentity("person-3"),
	}

	entityTuples := make([]Tuple, len(entities))
	for i, e := range entities {
		entityTuples[i] = Tuple{e}
	}
	entityRel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")},
		entityTuples,
	)

	inputRelations := []Relation{entityRel}
	bound := BindQueryInputs(q, inputRelations)

	t.Logf("Bound relation - columns: %v, size: %d", bound.Columns(), bound.Size())

	assert.Equal(t, 3, bound.Size(), "should have 3 tuples for 3 entities")
	assert.Equal(t, []query.Symbol{datalog.NewSymbol("?e")}, bound.Columns())
}
