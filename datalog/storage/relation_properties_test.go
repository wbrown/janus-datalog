package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestAETVCardinalityOneRelationProperties(t *testing.T) {
	nameAttr := datalog.NewKeyword(":person/name")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       nameAttr,
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:   t.TempDir(),
		Schema: s,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(datalog.NewIdentity("person:1"), nameAttr, "one"))
	require.NoError(t, tx.Set(datalog.NewIdentity("person:2"), nameAttr, "two"))
	require.NoError(t, tx.Set(datalog.NewIdentity("person:3"), nameAttr, "three"))
	_, err = tx.Commit()
	require.NoError(t, err)

	entity := datalog.NewSymbol("?e")
	value := datalog.NewSymbol("?v")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: nameAttr},
		query.Variable{Name: value},
	}}

	rel, err := db.Matcher().Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)
	require.Equal(t, executor.RelationProperties{
		Ordering: []query.OrderByClause{{
			Variable: entity, Descending: false,
		}},
		Keys: [][]query.Symbol{{entity}},
	}, rel.Properties())

	tuples, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)
	for i := 1; i < len(tuples); i++ {
		previous := tuples[i-1][0].(datalog.Identity)
		current := tuples[i][0].(datalog.Identity)
		require.LessOrEqual(t, previous.Compare(current), 0,
			"reported entity ordering must match emitted tuples")
	}
}

func TestUnboundScanPropertiesDeclinesUnprovenShapes(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	value := datalog.NewSymbol("?v")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":person/name")},
		query.Variable{Name: value},
	}}

	require.Equal(t, executor.RelationProperties{},
		unboundScanProperties(pattern, AEVT, schema.CardinalityOne, false),
		"an index without Tx-adjacent cardinality-one resolution must not claim AETV properties")
	require.Equal(t, executor.RelationProperties{},
		unboundScanProperties(pattern, AETV, schema.CardinalityMany, false),
		"cardinality-many resolution must not claim entity uniqueness")
	require.Equal(t, executor.RelationProperties{},
		unboundScanProperties(pattern, AETV, schema.CardinalityOne, true),
		"history mode exposes multiple versions per entity")
}

func TestValidatedVBoundProperties(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":entity/type")},
		query.Constant{Value: datalog.NewKeyword(":entity.type/person")},
	}}

	require.Equal(t, executor.RelationProperties{
		Ordering: []query.OrderByClause{{
			Variable: entity, Descending: false,
		}},
		Keys: [][]query.Symbol{{entity}},
	}, validatedVBoundProperties(pattern, ReuseStrategy{
		Index:           AVET,
		NeedsValidation: true,
	}))
	require.Equal(t, executor.RelationProperties{},
		validatedVBoundProperties(pattern, ReuseStrategy{Index: AEVT}),
		"non-validated paths must derive their own guarantees")
}

func TestAVETValidatedRelationProperties(t *testing.T) {
	typeAttr := datalog.NewKeyword(":entity/type")
	personType := datalog.NewKeyword(":entity.type/person")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       typeAttr,
		ValueType:   schema.TypeKeyword,
		Cardinality: schema.CardinalityOne,
	})
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:   t.TempDir(),
		Schema: s,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	for _, identity := range []datalog.Identity{
		datalog.NewIdentity("person:1"),
		datalog.NewIdentity("person:2"),
		datalog.NewIdentity("person:3"),
	} {
		require.NoError(t, tx.Set(identity, typeAttr, personType))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	entity := datalog.NewSymbol("?e")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: typeAttr},
		query.Constant{Value: personType},
	}}
	rel, err := db.Matcher().Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)
	require.Equal(t, executor.RelationProperties{
		Ordering: []query.OrderByClause{{
			Variable: entity, Descending: false,
		}},
		Keys: [][]query.Symbol{{entity}},
	}, rel.Properties())
}
