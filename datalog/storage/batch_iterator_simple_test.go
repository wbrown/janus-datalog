package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBatchIteratorSimple(t *testing.T) {
	// Create a minimal test case
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create exactly 3 entities with known IDs
	tx := db.NewTransaction()

	e1 := datalog.NewIdentity("entity1")
	e2 := datalog.NewIdentity("entity2")
	e3 := datalog.NewIdentity("entity3")

	attr := datalog.NewKeyword(":test/value")

	tx.Add(e1, attr, "value1")
	tx.Add(e2, attr, "value2")
	tx.Add(e3, attr, "value3")

	tx.Commit()

	// Pattern: [?e :test/value ?v]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?e")},
			query.Constant{Value: attr},
			query.Variable{Name: query.Symbol("?v")},
		},
	}

	matcher := NewBadgerMatcher(db.store)

	// First, get all entities directly
	direct, _ := matcher.Match(pattern, nil)

	t.Logf("Direct match results:")
	it := direct.Iterator()
	var entities []datalog.Identity
	for it.Next() {
		tuple := it.Tuple()
		if e, ok := tuple[0].(datalog.Identity); ok {
			entities = append(entities, e)
			hash := e.Hash()
			t.Logf("  Entity: L85=%s, hash=%x", e.String(), hash[:8])
		}
	}

	// Now create a binding relation with just e1 and e3
	bindingTuples := []executor.Tuple{
		{e1},
		{e3},
	}
	bindingRel := executor.NewMaterializedRelation([]query.Symbol{"?e"}, bindingTuples)

	t.Logf("\nBinding relation:")
	for _, tuple := range bindingTuples {
		if e, ok := tuple[0].(datalog.Identity); ok {
			hash := e.Hash()
			t.Logf("  Binding: L85=%s, hash=%x", e.String(), hash[:8])
		}
	}

	// Match with bindings (should return only e1 and e3)
	result, err := matcher.Match(pattern, executor.Relations{bindingRel})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("\nMatch with bindings:")
	count := 0
	it2 := result.Iterator()
	for it2.Next() {
		tuple := it2.Tuple()
		count++
		t.Logf("  Result %d: %v", count, tuple)
	}

	if count != 2 {
		t.Errorf("Expected 2 results (e1 and e3), got %d", count)
	}
}
