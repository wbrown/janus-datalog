package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestMultiRowRelationBinding(t *testing.T) {
	// Create test data: multiple entities with ages
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	ageAttr := datalog.NewKeyword(":user/age")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(25), Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(30), Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: charlie, A: ageAttr, V: int64(35), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Test 1: Use a multi-row relation to bind multiple entities at once
	t.Run("MultipleEntityBinding", func(t *testing.T) {
		// Create a relation with multiple entities
		bindingRel := NewMaterializedRelation(
			[]query.Symbol{"?user"},
			[]Tuple{
				{alice},
				{bob},
				// Note: NOT including charlie
			},
		)

		// Pattern: [?user :user/age ?age]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?user"},
				query.Constant{Value: ageAttr},
				query.Variable{Name: "?age"},
			},
		}

		// Match with the multi-row binding relation
		results, err := matcher.Match(pattern, Relations{bindingRel})
		assert.NoError(t, err)

		// Check columns
		cols := results.Columns()
		assert.Equal(t, []query.Symbol{"?user", "?age"}, cols)

		// Check we got the right data
		foundUsers := make(map[string]int64)
		count := 0
		it := results.Iterator()
		for it.Next() {
			count++
			tuple := it.Tuple()
			user := tuple[0].(datalog.Identity)
			age := tuple[1].(int64)

			if user.Equal(alice) {
				foundUsers["Alice"] = age
			} else if user.Equal(bob) {
				foundUsers["Bob"] = age
			} else {
				t.Errorf("unexpected entity: %v", user)
			}
		}
		it.Close()

		// Should only get ages for Alice and Bob, not Charlie
		assert.Equal(t, 2, count)
		assert.Equal(t, int64(25), foundUsers["Alice"])
		assert.Equal(t, int64(30), foundUsers["Bob"])
	})

	// Test 2: Binding with multiple values per symbol
	t.Run("MultipleValueBinding", func(t *testing.T) {
		// Create a relation with ages to look for
		bindingRel := NewMaterializedRelation(
			[]query.Symbol{"?age"},
			[]Tuple{
				{int64(25)},
				{int64(35)},
			},
		)

		// Pattern: [?user :user/age ?age]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?user"},
				query.Constant{Value: ageAttr},
				query.Variable{Name: "?age"},
			},
		}

		// Match with age bindings
		results, err := matcher.Match(pattern, Relations{bindingRel})
		assert.NoError(t, err)

		foundUsers := make(map[int64]string)
		count := 0
		it := results.Iterator()
		for it.Next() {
			count++
			tuple := it.Tuple()
			user := tuple[0].(datalog.Identity)
			age := tuple[1].(int64)

			if user.Equal(alice) {
				foundUsers[age] = "Alice"
			} else if user.Equal(charlie) {
				foundUsers[age] = "Charlie"
			} else {
				t.Errorf("unexpected user for age %d", age)
			}
		}
		it.Close()

		// Should get Alice (25) and Charlie (35), not Bob (30)
		assert.Equal(t, 2, count)
		assert.Equal(t, "Alice", foundUsers[int64(25)])
		assert.Equal(t, "Charlie", foundUsers[int64(35)])
	})

	// Test 3: Complex multi-column binding
	t.Run("MultiColumnBinding", func(t *testing.T) {
		// Create a more complex pattern with price data
		priceAttr := datalog.NewKeyword(":product/price")
		categoryAttr := datalog.NewKeyword(":product/category")

		p1 := datalog.NewIdentity("product:1")
		p2 := datalog.NewIdentity("product:2")
		p3 := datalog.NewIdentity("product:3")

		productDatoms := []datalog.Datom{
			{E: p1, A: categoryAttr, V: "Electronics", Tx: 1},
			{E: p1, A: priceAttr, V: 100.0, Tx: 1},
			{E: p2, A: categoryAttr, V: "Books", Tx: 2},
			{E: p2, A: priceAttr, V: 20.0, Tx: 2},
			{E: p3, A: categoryAttr, V: "Electronics", Tx: 3},
			{E: p3, A: priceAttr, V: 200.0, Tx: 3},
		}

		productMatcher := NewMemoryPatternMatcher(productDatoms)

		// Create a binding relation with category and price range
		bindingRel := NewMaterializedRelation(
			[]query.Symbol{"?category", "?minPrice"},
			[]Tuple{
				{"Electronics", 150.0}, // Only expensive electronics
				{"Books", 10.0},        // All books over $10
			},
		)

		// First pattern: [?p :product/category ?category]
		catPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?p"},
				query.Constant{Value: categoryAttr},
				query.Variable{Name: "?category"},
			},
		}

		// Match categories with binding
		catResults, err := productMatcher.Match(catPattern, Relations{bindingRel})
		assert.NoError(t, err)

		// Count category results
		catCount := 0
		catIt := catResults.Iterator()
		for catIt.Next() {
			catCount++
		}
		catIt.Close()

		// Should get all three products (both Electronics and one Book)
		assert.Equal(t, 3, catCount)

		// Now match prices
		pricePattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?p"},
				query.Constant{Value: priceAttr},
				query.Variable{Name: "?price"},
			},
		}

		priceResults, err := productMatcher.Match(pricePattern, nil)
		assert.NoError(t, err)

		// Count price results
		priceCount := 0
		priceIt := priceResults.Iterator()
		for priceIt.Next() {
			priceCount++
		}
		priceIt.Close()

		assert.Equal(t, 3, priceCount)

		// In a real query, these would be joined and filtered
		// The executor would handle the price > minPrice predicate
	})
}

func TestRelationBasedPatternMatching(t *testing.T) {
	// Test that the new interface properly handles empty bindings
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":foo"), V: "bar", Tx: 1},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword(":foo"), V: "baz", Tx: 2},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":foo")},
			query.Variable{Name: "?v"},
		},
	}

	// Test with nil bindings
	results, err := matcher.Match(pattern, nil)
	assert.NoError(t, err)

	count1 := 0
	it1 := results.Iterator()
	for it1.Next() {
		count1++
	}
	it1.Close()
	assert.Equal(t, 2, count1)

	// Test with empty Relations
	results2, err := matcher.Match(pattern, Relations{})
	assert.NoError(t, err)

	count2 := 0
	it2 := results2.Iterator()
	for it2.Next() {
		count2++
	}
	it2.Close()
	assert.Equal(t, 2, count2)

	// Results should be the same
	assert.Equal(t, count1, count2)
}
