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
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	datoms := []datalog.Datom{
		{E: e1, A: nameAttr, V: "Alice", Tx: tx},
		{E: e1, A: ageAttr, V: int64(30), Tx: tx},
		{E: e2, A: nameAttr, V: "Bob", Tx: tx},
		{E: e2, A: ageAttr, V: int64(25), Tx: tx},
	}

	// Test extracting just entity and value
	userSym := datalog.NewSymbol("?user")
	valueSym := datalog.NewSymbol("?value")
	binding := PatternBinding{
		EntitySym: &userSym,
		ValueSym:  &valueSym,
	}

	it := NewDatomIterator(datoms, binding)

	// Check symbols
	if len(it.symbols) != 2 {
		t.Errorf("expected 2 symbols, got %d", len(it.symbols))
	}
	if it.symbols[0] != userSym || it.symbols[1] != valueSym {
		t.Errorf("unexpected symbols: %v", it.symbols)
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
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	datoms := []datalog.Datom{
		{E: e1, A: nameAttr, V: "Alice", Tx: tx},
		{E: e1, A: ageAttr, V: int64(30), Tx: tx},
	}

	// Create relation with all fields bound
	eSym := datalog.NewSymbol("?e")
	aSym := datalog.NewSymbol("?a")
	vSym := datalog.NewSymbol("?v")
	txSym := datalog.NewSymbol("?tx")
	binding := PatternBinding{
		EntitySym:    &eSym,
		AttributeSym: &aSym,
		ValueSym:     &vSym,
		TxSym:        &txSym,
	}

	rel := NewDatomRelation(datoms, binding)

	// Check symbols
	symbols := rel.Symbols()
	if len(symbols) != 4 {
		t.Errorf("expected 4 symbols, got %d", len(symbols))
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
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

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
	userSym := datalog.NewSymbol("?user")
	nameSym := datalog.NewSymbol("?name")
	ageSym := datalog.NewSymbol("?age")

	nameRel := NewDatomRelation(namePattern, PatternBinding{
		EntitySym: &userSym,
		ValueSym:  &nameSym,
	})

	ageRel := NewDatomRelation(agePattern, PatternBinding{
		EntitySym: &userSym,
		ValueSym:  &ageSym,
	})

	// Join on ?user
	joined := nameRel.HashJoin(ageRel, []query.Symbol{datalog.NewSymbol("?user")})

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

	// Verify symbols
	symbols := joined.Symbols()
	expectedSymbols := []query.Symbol{datalog.NewSymbol("?user"), datalog.NewSymbol("?name"), datalog.NewSymbol("?age")}
	if len(symbols) != len(expectedSymbols) {
		t.Errorf("expected %d symbols, got %d", len(expectedSymbols), len(symbols))
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
