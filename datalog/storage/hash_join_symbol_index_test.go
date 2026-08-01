package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestHashJoinSymbolIndexBug tests the bug where HashJoinScan confused
// datom position with symbol index in the binding relation.
//
// Bug scenario:
// - Pattern: [?e :attr ?ref] where ?ref comes from a binding relation
// - ?ref is at datom position 2 (V position)
// - But ?ref is at symbol index 0 in the binding relation (first/only symbol)
// - buildHashSet was using position=2 instead of symbolIndex=0
// - Tried to access tuple[2] when tuple only had length 1
// - Result: Empty hash set → no matches
//
// This bug was hidden because:
// - With threshold ≤2, IndexNestedLoop was used for small binding sets
// - Only appeared when we changed threshold to 0, making HashJoinScan the default
// - Most benchmarks had patterns where datom position == symbol index
func TestHashJoinSymbolIndexBug(t *testing.T) {
	// Query that triggers the bug:
	// 1. [?s :symbol/ticker "AAPL"] returns binding with symbols=[?s]
	// 2. [?e :price/symbol ?s] joins on ?s
	//    - ?s is at datom position 2 (V position in the pattern)
	//    - But ?s is at symbol index 0 in the binding relation
	//    - Bug: used position=2 to access tuple[2], but tuple only has length 1
	queryStr := `[:find ?e ?value
	              :where [?s :symbol/ticker "AAPL"]
	                     [?e :price/symbol ?s]
	                     [?e :price/value ?value]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create schema-like data: symbol entity referenced by price entities
			symbolKw := datalog.NewKeyword(":symbol/ticker")
			priceSymbol := datalog.NewKeyword(":price/symbol")
			priceValue := datalog.NewKeyword(":price/value")

			tx := db.NewTransaction()

			// Create symbol entity
			symbolEntity := datalog.NewIdentity("AAPL")
			err := tx.Add(symbolEntity, symbolKw, "AAPL")
			assert.NoError(t, err)

			// Create price entities that reference the symbol
			for i := 0; i < 5; i++ {
				priceEntity := datalog.NewIdentity("price-" + string(rune('A'+i)))
				err = tx.Add(priceEntity, priceSymbol, symbolEntity)
				assert.NoError(t, err)
				err = tx.Add(priceEntity, priceValue, float64(100+i))
				assert.NoError(t, err)
			}

			_, err = tx.Commit()
			assert.NoError(t, err)

			matcher := NewPatternMatcherWithOptions(db.Store(), executor.ExecutorOptions{})
			exec := executor.NewExecutorWithOptions(matcher, db,
				planner.PlannerOptions{EnableAlgebraOptimizer: mode.algebra})

			result, err := exec.Execute(q)
			assert.NoError(t, err)

			// Should find all 5 price entities
			assert.Equal(t, 5, result.Size(), "Should find 5 price entities")

			// Verify we actually got the right entities
			it := result.Iterator()
			defer it.Close()
			count := 0
			for it.Next() {
				tuple := it.Tuple()
				assert.Len(t, tuple, 2, "Should have 2 symbols: ?e and ?value")
				// Verify entity is an Identity
				switch v := tuple[0].(type) {
				case datalog.Identity:
					// Valid Identity type
				default:
					t.Errorf("First tuple position should be Identity, got %T: %v", v, v)
				}
				// Verify value is a float64
				value, ok := tuple[1].(float64)
				assert.True(t, ok, "Second symbol should be float64, got %T", tuple[1])
				assert.GreaterOrEqual(t, value, 100.0)
				assert.LessOrEqual(t, value, 104.0)
				count++
			}
			assert.Equal(t, 5, count, "Iterator should return 5 tuples")
		})
	}
}

// TestHashJoinSymbolIndexMultiSymbol tests the fix works with multiple symbols
// where the join variable is not the first symbol.
func TestHashJoinSymbolIndexMultiSymbol(t *testing.T) {
	// Query where join variable is the second symbol:
	// 1. [?e1 :attr1 ?x] [?e1 :attr2 ?y] returns symbols=[?e1, ?x, ?y]
	//    (assuming phases separate these, second might be symbols=[?y])
	// 2. [?e2 :attr3 ?y] joins on ?y
	//    - ?y is at datom position 2 (V)
	//    - ?y's symbol index depends on result of first patterns
	queryStr := `[:find ?e1 ?e2
	              :where [?e1 :attr1 "X"]
	                     [?e1 :attr2 ?y]
	                     [?e2 :attr3 ?y]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create data
			attr1 := datalog.NewKeyword(":attr1")
			attr2 := datalog.NewKeyword(":attr2")
			attr3 := datalog.NewKeyword(":attr3")

			tx := db.NewTransaction()

			// Entity A with attr1=X and attr2=Y
			entityA := datalog.NewIdentity("A")
			tx.Add(entityA, attr1, "X")
			tx.Add(entityA, attr2, "Y")

			// Entity B references Y via attr3
			entityB := datalog.NewIdentity("B")
			tx.Add(entityB, attr3, "Y")

			_, err := tx.Commit()
			assert.NoError(t, err)

			matcher := NewPatternMatcherWithOptions(db.Store(), executor.ExecutorOptions{})
			exec := executor.NewExecutorWithOptions(matcher, db,
				planner.PlannerOptions{EnableAlgebraOptimizer: mode.algebra})

			result, err := exec.Execute(q)
			assert.NoError(t, err)
			assert.Equal(t, 1, result.Size(), "Should find one pair")

			it := result.Iterator()
			defer it.Close()
			assert.True(t, it.Next())
			tuple := it.Tuple()

			// Verify we got Identity types (the actual values are hashes, not the original strings)
			switch v := tuple[0].(type) {
			case datalog.Identity:
				// Valid Identity type
			default:
				t.Fatalf("Expected Identity in first tuple position, got %T", v)
			}
			switch v := tuple[1].(type) {
			case datalog.Identity:
				// Valid Identity type
			default:
				t.Fatalf("Expected Identity for second symbol, got %T", v)
			}

			// Both entities should be present and different
			// (we can't easily compare against "A" and "B" since Identity uses hashes)
		})
	}
}

func TestCompiledBindingMatchUsesPrecomputedSymbolSlots(t *testing.T) {
	parsed, err := parser.ParseQuery(
		`[:find ?e
		  :in $ ?noise ?bound
		  :where [?e :thing/ref ?bound]]`,
	)
	assert.NoError(t, err)
	pattern, err := parsed.SingleDataPattern()
	assert.NoError(t, err)

	noise := datalog.NewSymbol("?noise")
	bound := datalog.NewSymbol("?bound")
	target := datalog.NewIdentity("target")
	plan := compileBindingMatchPlan(pattern, []datalog.Symbol{noise, bound})
	datom := &datalog.Datom{
		E:  datalog.NewIdentity("source"),
		A:  datalog.NewKeyword(":thing/ref"),
		V:  target,
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}

	assert.True(t, plan.matches(&PatternMatcher{}, datom, executor.Tuple{"unused", target}))
	assert.False(t, plan.matches(
		&PatternMatcher{},
		datom,
		executor.Tuple{"unused", datalog.NewIdentity("different")},
	))
}

func TestStorageHashJoinMatchesSignedZero(t *testing.T) {
	parsed, err := parser.ParseQuery(
		`[:find ?left ?right
		  :where [?left :number/left ?value]
		         [?right :number/right ?value]]`,
	)
	assert.NoError(t, err)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			leftEntity := datalog.NewIdentity("signed-zero-left")
			rightEntity := datalog.NewIdentity("signed-zero-right")
			leftAttr := datalog.NewKeyword(":number/left")
			rightAttr := datalog.NewKeyword(":number/right")
			tx := db.NewTransaction()
			assert.NoError(t, tx.Add(leftEntity, leftAttr, float64(0)))
			assert.NoError(t, tx.Add(rightEntity, rightAttr, math.Copysign(0, -1)))
			_, err := tx.Commit()
			assert.NoError(t, err)

			matcher := NewPatternMatcherWithOptions(db.Store(), executor.ExecutorOptions{})
			exec := executor.NewExecutorWithOptions(matcher, db,
				planner.PlannerOptions{EnableAlgebraOptimizer: mode.algebra})

			result, err := exec.Execute(parsed)
			assert.NoError(t, err)
			assert.Equal(t, 1, result.Size())
		})
	}
}

func TestCompiledBindingMatchPlanAllDatomPositions(t *testing.T) {
	e := datalog.NewSymbol("?e")
	a := datalog.NewSymbol("?a")
	v := datalog.NewSymbol("?v")
	tx := datalog.NewSymbol("?tx")
	bindingSymbols := []datalog.Symbol{
		datalog.NewSymbol("?noise"),
		tx,
		v,
		e,
		a,
	}
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Variable{Name: a},
		query.Variable{Name: v},
		query.Variable{Name: tx},
	}}
	entity := datalog.NewIdentity("all-slots")
	attribute := datalog.NewKeyword(":all/slots")
	elementID := datalog.ElementID{Lamport: 7, ReplicaID: 3}
	datom := &datalog.Datom{E: entity, A: attribute, V: "", Tx: elementID}
	tuple := executor.Tuple{"noise", elementID, "", entity, attribute}
	plan := compileBindingMatchPlan(pattern, bindingSymbols)

	assert.True(t, plan.matches(&PatternMatcher{}, datom, tuple))
	for bindingIndex := 1; bindingIndex < len(tuple); bindingIndex++ {
		changed := append(executor.Tuple(nil), tuple...)
		switch bindingIndex {
		case 1:
			changed[bindingIndex] = datalog.ElementID{Lamport: 8, ReplicaID: 3}
		case 2:
			changed[bindingIndex] = "different"
		case 3:
			changed[bindingIndex] = datalog.NewIdentity("different")
		case 4:
			changed[bindingIndex] = datalog.NewKeyword(":different")
		}
		assert.False(t, plan.matches(&PatternMatcher{}, datom, changed),
			"binding index %d must participate in matching", bindingIndex)
	}
	assert.Panics(t, func() {
		plan.matches(&PatternMatcher{}, datom, executor.Tuple{"short"})
	}, "a binding tuple that violates its declared schema must fail loudly")
}

func TestCompiledBindingMatchPlanBytesByContent(t *testing.T) {
	value := datalog.NewSymbol("?value")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: datalog.NewKeyword(":blob/value")},
		query.Variable{Name: value},
	}}
	plan := compileBindingMatchPlan(pattern, []datalog.Symbol{value})
	datom := &datalog.Datom{
		E: datalog.NewIdentity("bytes"),
		A: datalog.NewKeyword(":blob/value"),
		V: []byte{1, 2, 3},
	}

	assert.True(t, plan.matches(&PatternMatcher{}, datom, executor.Tuple{[]byte{1, 2, 3}}))
	assert.False(t, plan.matches(&PatternMatcher{}, datom, executor.Tuple{[]byte{1, 2, 4}}))
}
