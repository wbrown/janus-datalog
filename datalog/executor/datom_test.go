package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestDatomIterator(t *testing.T) {
	// Create some test datoms
	e1 := datalog.NewIdentity("user:alice")
	e2 := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := uint64(1)

	datoms := []datalog.Datom{
		{E: e1, A: nameAttr, V: "Alice", Tx: tx},
		{E: e1, A: ageAttr, V: int64(30), Tx: tx},
		{E: e2, A: nameAttr, V: "Bob", Tx: tx},
		{E: e2, A: ageAttr, V: int64(25), Tx: tx},
	}

	// Test extracting just entity and value
	userSym := query.Symbol("?user")
	valueSym := query.Symbol("?value")
	binding := PatternBinding{
		EntitySym: &userSym,
		ValueSym:  &valueSym,
	}

	it := NewDatomIterator(datoms, binding)

	// Check columns
	if len(it.columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(it.columns))
	}
	if it.columns[0] != userSym || it.columns[1] != valueSym {
		t.Errorf("unexpected columns: %v", it.columns)
	}

	// Iterate and check tuples
	count := 0
	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) != 2 {
			t.Errorf("expected tuple length 2, got %d", len(tuple))
		}

		// First element should be an Identity
		if _, ok := tuple[0].(datalog.Identity); !ok {
			t.Errorf("expected Identity in position 0, got %T", tuple[0])
		}

		// Second element should be a value (string or int64)
		switch tuple[1].(type) {
		case string, int64:
			// OK
		default:
			t.Errorf("expected string or int64 in position 1, got %T", tuple[1])
		}

		count++
	}

	if count != 4 {
		t.Errorf("expected 4 tuples, got %d", count)
	}

	it.Close()
}

func TestDatomRelation(t *testing.T) {
	// Create test datoms
	e1 := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := uint64(1)

	datoms := []datalog.Datom{
		{E: e1, A: nameAttr, V: "Alice", Tx: tx},
		{E: e1, A: ageAttr, V: int64(30), Tx: tx},
	}

	// Create relation with all fields bound
	eSym := query.Symbol("?e")
	aSym := query.Symbol("?a")
	vSym := query.Symbol("?v")
	txSym := query.Symbol("?tx")
	binding := PatternBinding{
		EntitySym:    &eSym,
		AttributeSym: &aSym,
		ValueSym:     &vSym,
		TxSym:        &txSym,
	}

	rel := NewDatomRelation(datoms, binding)

	// Check columns
	cols := rel.Columns()
	if len(cols) != 4 {
		t.Errorf("expected 4 columns, got %d", len(cols))
	}

	// Check data
	it := rel.Iterator()
	defer it.Close()

	count := 0
	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) != 4 {
			t.Errorf("expected tuple length 4, got %d", len(tuple))
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 tuples, got %d", count)
	}
}

func TestDatomJoinScenario(t *testing.T) {
	// Simulate a real query scenario:
	// Find all users and their ages

	e1 := datalog.NewIdentity("user:alice")
	e2 := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := uint64(1)

	// First pattern: [?user :user/name ?name]
	namePattern := []datalog.Datom{
		{E: e1, A: nameAttr, V: "Alice", Tx: tx},
		{E: e2, A: nameAttr, V: "Bob", Tx: tx},
	}

	// Second pattern: [?user :user/age ?age]
	agePattern := []datalog.Datom{
		{E: e1, A: ageAttr, V: int64(30), Tx: tx},
		{E: e2, A: ageAttr, V: int64(25), Tx: tx},
	}

	// Create relations
	userSym := query.Symbol("?user")
	nameSym := query.Symbol("?name")
	ageSym := query.Symbol("?age")

	nameRel := NewDatomRelation(namePattern, PatternBinding{
		EntitySym: &userSym,
		ValueSym:  &nameSym,
	})

	ageRel := NewDatomRelation(agePattern, PatternBinding{
		EntitySym: &userSym,
		ValueSym:  &ageSym,
	})

	// Join on ?user
	joined := nameRel.HashJoin(ageRel, []query.Symbol{"?user"})

	// Debug: Check what we have before join
	t.Logf("nameRel size: %d", nameRel.Size())
	t.Logf("ageRel size: %d", ageRel.Size())

	// Check results
	if joined.Size() != 2 {
		t.Errorf("expected 2 joined results, got %d", joined.Size())

		// Debug: Print what's in the relations
		it := nameRel.Iterator()
		t.Log("Name relation:")
		for it.Next() {
			t.Logf("  %v", it.Tuple())
		}
		it.Close()

		it = ageRel.Iterator()
		t.Log("Age relation:")
		for it.Next() {
			t.Logf("  %v", it.Tuple())
		}
		it.Close()
	}

	// Verify columns
	cols := joined.Columns()
	expectedCols := []query.Symbol{"?user", "?name", "?age"}
	if len(cols) != len(expectedCols) {
		t.Errorf("expected %d columns, got %d", len(expectedCols), len(cols))
	}

	// Check actual data
	it := joined.Iterator()
	defer it.Close()

	results := make(map[string]int64) // name -> age
	for it.Next() {
		tuple := it.Tuple()
		if name, ok := tuple[1].(string); ok {
			if age, ok := tuple[2].(int64); ok {
				results[name] = age
			}
		}
	}

	// Verify Alice is 30 and Bob is 25
	if results["Alice"] != 30 {
		t.Errorf("expected Alice to be 30, got %d", results["Alice"])
	}
	if results["Bob"] != 25 {
		t.Errorf("expected Bob to be 25, got %d", results["Bob"])
	}
}
