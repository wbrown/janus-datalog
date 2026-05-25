package executor

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestCreateInputRelations_ArityMismatchReturnsError is a regression test for
// BUG_SUBQUERY_INPUT_ARITY_MISMATCH_SILENT_UNCORRELATED.
//
// createInputRelationsFromValuesWithOptions previously returned a nil relation
// slice on an input arity mismatch — the comment said "this is an error" but the
// function had no error return. Callers then proceeded with nil inputs, silently
// turning a correlated subquery into an uncorrelated/global execution or an empty
// result. A mismatch must now surface as an error.
func TestCreateInputRelations_ArityMismatchReturnsError(t *testing.T) {
	source := datalog.NewSymbol("$")

	t.Run("too few values for :in $ ?x ?y", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find ?z :in $ ?x ?y :where [?z :a/b ?x] [?z :a/c ?y]]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		// :in declares 3 inputs ($, ?x, ?y); supply only 2.
		_, err = createInputRelationsFromValuesWithOptions(q, []interface{}{source, "x"}, ExecutorOptions{})
		if err == nil {
			t.Fatal("expected an arity-mismatch error, got nil (silent uncorrelated execution)")
		}
		if !strings.Contains(err.Error(), "arity mismatch") {
			t.Fatalf("error should describe the arity mismatch, got: %v", err)
		}
	})

	t.Run("missing database source marker for :in $ ?x", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find ?z :in $ ?x :where [?z :a/b ?x]]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		// Correct count (2), but position 0 is not the $ source marker.
		_, err = createInputRelationsFromValuesWithOptions(q, []interface{}{"not-a-source", "x"}, ExecutorOptions{})
		if err == nil {
			t.Fatal("expected a missing-database-source error, got nil")
		}
		if !strings.Contains(err.Error(), "database source") {
			t.Fatalf("error should describe the missing database source, got: %v", err)
		}
	})

	t.Run("correct shape succeeds", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find ?z :in $ ?x ?y :where [?z :a/b ?x] [?z :a/c ?y]]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		rels, err := createInputRelationsFromValuesWithOptions(q, []interface{}{source, "x", "y"}, ExecutorOptions{})
		if err != nil {
			t.Fatalf("unexpected error for correct input shape: %v", err)
		}
		// Two scalar inputs (?x, ?y) → two single-value relations.
		if len(rels) != 2 {
			t.Fatalf("expected 2 input relations (?x, ?y), got %d", len(rels))
		}
	})
}
