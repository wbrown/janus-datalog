package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A ground whose constant the binding form cannot read is refused at every
// boundary naming the repair, computed from the constant's shape over the
// author's own symbols (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md
// goal 6 and §7): one value under the tuple or relation form is bound by the
// scalar form over the first symbol; a vector of values under the relation
// form is bound by the collection form when the binding names one symbol, by
// the tuple form when it names as many symbols as the vector holds values,
// and otherwise the refusal states both counts.

// groundBound is the expression [(ground value) binding] under any binding form.
func groundBound(value interface{}, binding interface{}) *Expression {
	return &Expression{Function: &GroundFunction{Value: value}, Binding: binding}
}

func TestExpressionValidate_RefusesAScalarGroundUnderTheTupleForm(t *testing.T) {
	pq := TupleBinding{Variables: []Symbol{sym("?p"), sym("?q")}}

	t.Run("one value under a two-symbol tuple form names the scalar form over the first symbol", func(t *testing.T) {
		err := groundBound(int64(5), pq).Validate()
		require.ErrorContains(t, err, "is one value; bind it with the scalar form ?p")
	})

	t.Run("one value under a one-symbol tuple form names the scalar form over that symbol", func(t *testing.T) {
		err := groundBound(int64(5), TupleBinding{Variables: []Symbol{sym("?p")}}).Validate()
		require.ErrorContains(t, err, "is one value; bind it with the scalar form ?p")
	})

	t.Run("a vector of as many values under the tuple form is accepted", func(t *testing.T) {
		require.NoError(t, groundBound([]interface{}{int64(1), int64(2)}, pq).Validate())
	})
}

func TestExpressionValidate_NamesTheRepairFromTheGroundsShape(t *testing.T) {
	p := RelationBinding{Variables: []Symbol{sym("?p")}}
	pq := RelationBinding{Variables: []Symbol{sym("?p"), sym("?q")}}

	t.Run("a vector of values under a one-symbol relation form names the collection form", func(t *testing.T) {
		err := groundBound([]interface{}{int64(1), int64(2)}, p).Validate()
		require.ErrorContains(t, err, "bind it with the collection form [?p ...]")
	})

	t.Run("a vector of as many values under the relation form names the tuple form", func(t *testing.T) {
		err := groundBound([]interface{}{int64(1), int64(2)}, pq).Validate()
		require.ErrorContains(t, err, "bind it with the tuple form [?p ?q]")
	})

	t.Run("a vector of another count of values under the relation form states both counts", func(t *testing.T) {
		err := groundBound([]interface{}{int64(1), int64(2), int64(3)}, pq).Validate()
		require.ErrorContains(t, err, "is one tuple of 3 values")
		require.ErrorContains(t, err, "names 2 symbols")
	})

	t.Run("one value under the relation form names the scalar form over the first symbol", func(t *testing.T) {
		err := groundBound(int64(5), pq).Validate()
		require.ErrorContains(t, err, "is one value; bind it with the scalar form ?p")
	})

	t.Run("one value under the collection form names the scalar form over its symbol", func(t *testing.T) {
		err := groundBound(int64(5), CollectionBinding{Variable: sym("?p")}).Validate()
		require.ErrorContains(t, err, "is one value; bind it with the scalar form ?p")
	})

	t.Run("a vector of vectors of the binding's arity under the relation form is accepted", func(t *testing.T) {
		rows := []interface{}{[]interface{}{int64(1), int64(2)}, []interface{}{int64(3), int64(4)}}
		require.NoError(t, groundBound(rows, pq).Validate())
	})
}

// A tuple or relation binding names at least one symbol, as the parser
// requires of the text, and a hand-built binding naming none is refused with
// the parser's words.
func TestExpressionValidate_RefusesABindingNamingNoSymbol(t *testing.T) {
	t.Run("the tuple form", func(t *testing.T) {
		err := groundBound(int64(5), TupleBinding{}).Validate()
		require.ErrorContains(t, err, "tuple binding cannot be empty")
	})

	t.Run("the relation form", func(t *testing.T) {
		err := groundBound([]interface{}{[]interface{}{int64(1)}}, RelationBinding{}).Validate()
		require.ErrorContains(t, err, "relation binding cannot be empty")
	})
}
