package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Pull + deduplication composition (docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md).
// Pulled values are result presentation, not datalog values: pulls render at
// the result boundary after sort/strip/limit, so relational operations
// (dedup, union, limit) only ever see Identity in the entity binding.

// Pull + :limit N≥2: LimitRelation.ensure deduplicates Identity tuples, then
// the boundary pull renders only the N surviving tuples.
func TestPullWithLimitTwoTuples(t *testing.T) {
	matcher := NewMemoryPatternMatcher(nonProjectedSortDatoms())

	q, err := parser.ParseQuery(`[:find (pull ?e [:user/name])
	                              :where [?e :user/age ?age]
	                              :limit 3]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			result, err := executor.Execute(q)
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}
			if result.Size() != 3 {
				t.Fatalf("expected 3 pulled tuples, got %d", result.Size())
			}
		})
	}
}

// Pull + relation input: the iteration path unions per-tuple results and
// deduplicates them on Identity tuples (by-entity set semantics); the
// boundary pull renders each surviving tuple.
func TestPullWithRelationInputUnion(t *testing.T) {
	keyAttr := datalog.NewKeyword(":item/key")
	valAttr := datalog.NewKeyword(":item/val")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	var datoms []datalog.Datom
	add := func(seed, key string, val int64) {
		e := datalog.NewIdentity(seed)
		datoms = append(datoms,
			datalog.Datom{E: e, A: keyAttr, V: key, Tx: tx},
			datalog.Datom{E: e, A: valAttr, V: val, Tx: tx},
		)
	}
	add("a1", "A", 1)
	add("a2", "A", 2)
	add("b1", "B", 3)

	matcher := NewMemoryPatternMatcher(datoms)

	q, err := parser.ParseQuery(`[:find (pull ?e [:item/val])
	                              :in $ [[?k] ...]
	                              :where [?e :item/key ?k]]`)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			inputRel := NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?k")}, []Tuple{{"A"}, {"B"}})

			result, err := executor.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}
			if result.Size() != 3 {
				t.Fatalf("expected 3 pulled tuples across the union, got %d", result.Size())
			}
		})
	}
}
