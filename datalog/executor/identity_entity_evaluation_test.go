package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A database function fails on a value only where its entity value is no
// identity, so over a relation whose identity symbols include the function's
// entity a filter or an evaluation of it evaluates nothing that can fail: the
// query's filter is read at the clause into a held relation, and every filter
// and evaluation carries no evaluating mark. Over a relation naming no
// identity each filter and evaluation carries the mark.
func TestADatabaseFunctionOverAnIdentitySymbolEvaluatesNothingThatCanFail(t *testing.T) {
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	entity := query.VariableTerm{Symbol: e}
	missing := &query.DatabaseFunctionPredicate{Function: &query.MissingFunction{Entity: entity, Attr: datalog.NewKeyword(":t/b")}}
	getElse := &query.GetElseFunction{Entity: entity, Attr: datalog.NewKeyword(":t/a"), Default: query.ConstantTerm{Value: int64(-1)}}
	symbols := []query.Symbol{e}
	held := func(properties RelationProperties) *MaterializedRelation {
		return newMaterializedRelationFromSet(symbols, nil, ExecutorOptions{}, properties)
	}
	stream := func(properties RelationProperties) *StreamingRelation {
		return NewStreamingRelationWithProperties(symbols, &sliceIterator{pos: -1}, ExecutorOptions{}, properties)
	}
	identities := RelationProperties{Identities: symbols}

	readers := []struct {
		name string
		read func(RelationProperties) (Relation, error)
	}{
		{"the query's filter", func(p RelationProperties) (Relation, error) {
			return filterWithPredicateAndLookup(held(p), missing, nil, nil)
		}},
		{"a stream's filter", func(p RelationProperties) (Relation, error) {
			return stream(p).FilterWithPredicate(missing), nil
		}},
		{"a stream's evaluation", func(p RelationProperties) (Relation, error) {
			return stream(p).EvaluateFunction(getElse, v), nil
		}},
		{"a lazy sequence's filter", func(p RelationProperties) (Relation, error) {
			return WrapStreamingAsLazy(stream(p)).FilterWithPredicate(missing), nil
		}},
		{"a lazy sequence's evaluation", func(p RelationProperties) (Relation, error) {
			return WrapStreamingAsLazy(stream(p)).EvaluateFunction(getElse, v), nil
		}},
		{"the query's evaluation", func(p RelationProperties) (Relation, error) {
			return evaluateExpressionWithLookup(held(p), &query.Expression{Function: getElse, Binding: v}, nil, nil), nil
		}},
	}
	overs := []struct {
		name       string
		properties RelationProperties
		evaluates  bool
	}{
		{"a relation whose identity symbols include the entity", identities, false},
		{"a relation naming no identity", RelationProperties{}, true},
	}
	for _, reader := range readers {
		for _, over := range overs {
			t.Run(reader.name+" over "+over.name, func(t *testing.T) {
				result, err := reader.read(over.properties)
				require.NoError(t, err)
				require.Equal(t, over.evaluates, result.Properties().Evaluates)
			})
		}
	}

	t.Run("the query's filter that cannot fail is read at the clause", func(t *testing.T) {
		filtered, err := filterWithPredicateAndLookup(held(identities), missing, nil, nil)
		require.NoError(t, err)
		require.IsType(t, &MaterializedRelation{}, filtered)
	})
}

// An evaluation that destructures a database function's value under the
// tuple, collection or relation form reads the value's shape, which can fail
// on a value whatever the function's entity, so over a relation whose identity
// symbols include the entity it carries the evaluating mark.
func TestAnEvaluationDestructuringADatabaseFunctionsValueCarriesTheMarkOverAnIdentitySymbol(t *testing.T) {
	e, v, w := datalog.NewSymbol("?e"), datalog.NewSymbol("?v"), datalog.NewSymbol("?w")
	getElse := &query.GetElseFunction{Entity: query.VariableTerm{Symbol: e}, Attr: datalog.NewKeyword(":t/a"), Default: query.ConstantTerm{Value: int64(-1)}}
	over := newMaterializedRelationFromSet([]query.Symbol{e}, nil, ExecutorOptions{}, RelationProperties{Identities: []query.Symbol{e}})
	for _, form := range []struct {
		name    string
		binding interface{}
	}{
		{"the tuple form", query.TupleBinding{Variables: []query.Symbol{v, w}}},
		{"the collection form", query.CollectionBinding{Variable: v}},
		{"the relation form", query.RelationBinding{Variables: []query.Symbol{v, w}}},
	} {
		t.Run(form.name, func(t *testing.T) {
			evaluated := evaluateExpressionWithLookup(over, &query.Expression{Function: getElse, Binding: form.binding}, nil, nil)
			require.True(t, evaluated.Properties().Evaluates, "the form reads the value's shape, which can fail whatever the entity")
		})
	}
}
