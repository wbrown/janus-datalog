package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// BUG: ExecuteQueryWithInputs does not apply CRDT resolution
// =============================================================================
//
// SYMPTOMS:
// - Queries return ALL historical values instead of CRDT-resolved current values
// - For CardinalityOne: returns N values (all unique historical) instead of 1
// - For CardinalityMany: returns all ever-added values instead of current set
//
// ROOT CAUSE:
// - The matcher has CRDT resolution code in the cache path
// - But when A is a Variable (even if bound via inputs), the cache path is skipped
// - Join strategies scan raw storage without CRDT resolution
//
// EXPECTED BEHAVIOR:
// - ExecuteQueryWithInputs should return CRDT-resolved values
// - CardinalityOne: single value (LWW winner)
// - CardinalityMany: current set members (tombstoned values excluded)
// =============================================================================

// TestExecuteQueryWithInputs_CardinalityOne_ReturnsMultipleValues reproduces the bug
// where a CardinalityOne attribute query returns all historical values instead of
// the LWW-resolved single value.
func TestExecuteQueryWithInputs_CardinalityOne_ReturnsMultipleValues(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, nil)

			// Create schema with CardinalityOne
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			// Write multiple values to the same CardinalityOne attribute
			// Under LWW semantics, only the last value should be "current"
			names := []string{"Alice", "Bob", "Charlie", "Diana"}
			for _, name := range names {
				tx := db.NewTransaction()
				err := tx.Set(personID, nameAttr, name)
				require.NoError(t, err)
				_, err = tx.Commit()
				require.NoError(t, err)
			}

			expectedName := "Diana" // Last value = LWW winner

			// Verify PullInto works correctly (control case)
			type Person struct {
				ID   datalog.Identity `datalog:"-,id"`
				Name string           `datalog:"person/name"`
			}
			var person Person
			err := db.PullInto(personID, &person)
			require.NoError(t, err)
			assert.Equal(t, expectedName, person.Name, "PullInto should return LWW-resolved value")

			// THE BUG: ExecuteQueryWithInputs returns ALL historical values
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr))
			require.NoError(t, err)

			t.Logf("ExecuteQueryWithInputs returned %d results: %v", len(results), results)

			// This assertion will FAIL until the bug is fixed
			// Currently returns: [[Alice] [Bob] [Charlie] [Diana]]
			// Should return: [[Diana]]
			if len(results) != 1 {
				t.Errorf("BUG CONFIRMED: CardinalityOne query returned %d values instead of 1", len(results))
				t.Log("Expected: 1 result (LWW-resolved value)")
				t.Logf("Got: %d results (all historical values)", len(results))
				for i, r := range results {
					t.Logf("  [%d]: %v", i, r)
				}
			}

			assert.Len(t, results, 1, "CardinalityOne should return exactly 1 value")
			if len(results) == 1 {
				assert.Equal(t, expectedName, results[0][0], "Should return LWW-resolved value")
			}
		})
	}
}

// TestExecuteQueryWithInputs_CardinalityMany_ReturnsAllHistoricalValues reproduces
// the bug where a CardinalityMany attribute query returns all ever-added values
// instead of the current set (with tombstoned values excluded).
func TestExecuteQueryWithInputs_CardinalityMany_ReturnsAllHistoricalValues(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, nil)

			// Create schema with CardinalityMany
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/tags"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityMany,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			tagsAttr := datalog.NewKeyword(":person/tags")

			// Set tags multiple times - each Set replaces the entire set
			tagSets := [][]string{
				{"red", "green", "blue"},
				{"alpha", "beta"},
				{"one", "two", "three"},
			}
			for _, tags := range tagSets {
				tx := db.NewTransaction()
				// Convert to []interface{} for Set
				tagValues := make([]interface{}, len(tags))
				for i, tag := range tags {
					tagValues[i] = tag
				}
				err := tx.Set(personID, tagsAttr, tagValues)
				require.NoError(t, err)
				_, err = tx.Commit()
				require.NoError(t, err)
			}

			expectedTags := []string{"one", "two", "three"} // Last set

			// Verify PullInto works correctly (control case)
			type Person struct {
				ID   datalog.Identity `datalog:"-,id"`
				Tags []string         `datalog:"person/tags"`
			}
			var person Person
			err := db.PullInto(personID, &person)
			require.NoError(t, err)
			assert.Len(t, person.Tags, len(expectedTags), "PullInto should return current set only")
			t.Logf("PullInto returned tags: %v", person.Tags)

			// THE BUG: ExecuteQueryWithInputs returns ALL ever-added tags
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, tagsAttr))
			require.NoError(t, err)

			t.Logf("ExecuteQueryWithInputs returned %d results", len(results))
			for i, r := range results {
				t.Logf("  [%d]: %v", i, r)
			}

			// This assertion will FAIL until the bug is fixed
			// Currently returns all 8 unique tags ever added
			// Should return only the current 3 tags
			if len(results) != len(expectedTags) {
				t.Errorf("BUG CONFIRMED: CardinalityMany query returned %d values instead of %d",
					len(results), len(expectedTags))
				t.Logf("Expected: %d results (current set members)", len(expectedTags))
				t.Logf("Got: %d results (all historical values)", len(results))
			}

			assert.Len(t, results, len(expectedTags),
				"CardinalityMany should return only current set members")
		})
	}
}

// TestDirectMatch_VsExecuteQuery_CRDTResolution compares direct Match with ExecuteQuery
// to isolate whether the bug is in the matcher or the query executor.
func TestDirectMatch_VsExecuteQuery_CRDTResolution(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, nil)

			// Create schema with CardinalityOne
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			// Write multiple values
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				err := tx.Set(personID, nameAttr, name)
				require.NoError(t, err)
				_, err = tx.Commit()
				require.NoError(t, err)
			}

			// Test direct Match (via db.Matcher())
			matcher := NewPatternMatcher(db.Store())
			matcher.SetSchema(s)

			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: personID},
					query.Constant{Value: nameAttr},
					query.Variable{Name: datalog.NewSymbol("?name")},
					query.Blank{},
				},
			}

			results, err := matcher.Match(query.PatternQuery(pattern), nil)
			require.NoError(t, err)

			// Count results from Match
			matchCount := 0
			iter := results.Iterator()
			for iter.Next() {
				matchCount++
				t.Logf("Direct Match result: %v", iter.Tuple())
			}

			t.Logf("Direct Match returned %d results", matchCount)

			// The matcher SHOULD return only 1 result for CardinalityOne
			// If it returns multiple, the bug is in the matcher
			// If it returns 1 but ExecuteQuery returns multiple, the bug is in the executor
			if matchCount == 1 {
				t.Log("Direct Match correctly returns 1 result - bug is in query executor")
			} else {
				t.Logf("Direct Match returns %d results - bug may be in matcher CRDT logic", matchCount)
			}

			// Also test ExecuteQueryWithInputs for comparison
			queryResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr))
			require.NoError(t, err)

			t.Logf("ExecuteQueryWithInputs returned %d results", len(queryResults))

			// Compare the two approaches
			if matchCount == 1 && len(queryResults) > 1 {
				t.Log("DIAGNOSIS: Direct Match works, ExecuteQueryWithInputs doesn't")
				t.Log("The bug is in how the query executor uses the matcher")
			} else if matchCount > 1 && len(queryResults) > 1 {
				t.Log("DIAGNOSIS: Both return multiple - bug is in matcher CRDT resolution")
			}
		})
	}
}

// TestSchemaAwareness_InQueryExecution verifies the schema is accessible
// during query execution.
func TestSchemaAwareness_InQueryExecution(t *testing.T) {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})

	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:   dir,
		Schema: s,
	})
	require.NoError(t, err)
	defer db.Close()

	// Verify schema is set
	require.NotNil(t, db.Schema(), "Database should have schema set")

	attrDef := db.Schema().GetAttribute(datalog.NewKeyword(":person/name"))
	require.NotNil(t, attrDef, "Schema should contain :person/name")
	assert.Equal(t, schema.CardinalityOne, attrDef.Cardinality,
		":person/name should be CardinalityOne")

	// Verify Matcher() has schema
	matcher := db.Matcher()
	require.NotNil(t, matcher, "Matcher should not be nil")

	// The matcher should be using the schema for CRDT resolution
	// This test documents that the schema IS available - the bug is in
	// how/whether the executor applies CRDT resolution during query execution
	t.Log("Schema is correctly configured and accessible")
	t.Log("The bug is NOT in schema configuration - it's in query execution CRDT resolution")
}

// TestAllQueryMethods_CRDTResolution tests all query methods to determine which
// correctly apply CRDT resolution. This provides a comprehensive matrix of
// working vs broken methods.
func TestAllQueryMethods_CRDTResolution(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testAllQueryMethodsCRDTResolution(t, mode)
		})
	}
}

func testAllQueryMethodsCRDTResolution(t *testing.T, mode optimizerMode) {
	db := createOptimizerModeDB(t, mode, nil)

	// Create schema with CardinalityOne
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	personID := datalog.NewIdentity("person-1")
	nameAttr := datalog.NewKeyword(":person/name")

	// Write multiple values - only "Charlie" should be visible (LWW)
	for _, name := range []string{"Alice", "Bob", "Charlie"} {
		tx := db.NewTransaction()
		err := tx.Set(personID, nameAttr, name)
		require.NoError(t, err)
		_, err = tx.Commit()
		require.NoError(t, err)
	}

	expectedName := "Charlie"

	// Track results for summary
	type methodResult struct {
		name    string
		count   int
		value   interface{}
		correct bool
	}
	var results []methodResult

	// 1. Direct Matcher.Match (control - known working)
	t.Run("DirectMatch", func(t *testing.T) {
		matcher := NewPatternMatcher(db.Store())
		matcher.SetSchema(s)

		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: personID},
				query.Constant{Value: nameAttr},
				query.Variable{Name: datalog.NewSymbol("?name")},
				query.Blank{},
			},
		}

		matchResults, err := matcher.Match(query.PatternQuery(pattern), nil)
		require.NoError(t, err)

		count := 0
		var lastValue interface{}
		iter := matchResults.Iterator()
		for iter.Next() {
			count++
			tuple := iter.Tuple()
			// The tuple contains only the bound variables from the pattern
			// Since we have one Variable (?name), it should be at index 0
			if len(tuple) > 0 {
				lastValue = tuple[0]
			}
			t.Logf("  tuple[%d]: %v (len=%d)", count-1, tuple, len(tuple))
		}

		correct := count == 1
		results = append(results, methodResult{"DirectMatch", count, lastValue, correct})
		t.Logf("DirectMatch: %d results, value=%v, correct=%v", count, lastValue, correct)
	})

	// 2. PullInto (control - known working)
	t.Run("PullInto", func(t *testing.T) {
		type Person struct {
			ID   datalog.Identity `datalog:"-,id"`
			Name string           `datalog:"person/name"`
		}
		var person Person
		err := db.PullInto(personID, &person)
		require.NoError(t, err)

		correct := person.Name == expectedName
		results = append(results, methodResult{"PullInto", 1, person.Name, correct})
		t.Logf("PullInto: value=%q, correct=%v", person.Name, correct)
	})

	// 3. Pull (to map)
	t.Run("Pull", func(t *testing.T) {
		pulled, err := db.Pull(personID, "[*]")
		require.NoError(t, err)

		name := pulled["person/name"]
		// Pull might return single value or slice depending on implementation
		var count int
		var value interface{}
		switch v := name.(type) {
		case []interface{}:
			count = len(v)
			if count > 0 {
				value = v[len(v)-1]
			}
		case string:
			count = 1
			value = v
		default:
			count = 1
			value = v
		}

		correct := count == 1 && value == expectedName
		results = append(results, methodResult{"Pull", count, value, correct})
		t.Logf("Pull: %d results, value=%v, correct=%v", count, value, correct)
	})

	// 4. ExecuteQuery (basic, no inputs)
	t.Run("ExecuteQuery", func(t *testing.T) {
		// Use a query that doesn't need inputs by hardcoding the entity lookup
		queryResults, err := executor.CollectTuples(db.Query(
			`[:find ?v :where [?e :person/name ?v]]`))
		require.NoError(t, err)

		count := len(queryResults)
		var value interface{}
		if count > 0 {
			value = queryResults[count-1][0]
		}

		correct := count == 1
		results = append(results, methodResult{"ExecuteQuery", count, value, correct})
		t.Logf("ExecuteQuery: %d results, correct=%v", count, correct)
	})

	// 5. ExecuteQueryWithInputs (known broken)
	t.Run("ExecuteQueryWithInputs", func(t *testing.T) {
		queryResults, err := executor.CollectTuples(db.Query(
			`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
			personID, nameAttr))
		require.NoError(t, err)

		count := len(queryResults)
		var value interface{}
		if count > 0 {
			value = queryResults[count-1][0]
		}

		correct := count == 1
		results = append(results, methodResult{"ExecuteQueryWithInputs", count, value, correct})
		t.Logf("ExecuteQueryWithInputs: %d results, correct=%v", count, correct)
	})

	// 6. Query (streaming Relation)
	t.Run("Query", func(t *testing.T) {
		rel, err := db.Query(
			`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
			personID, nameAttr)
		require.NoError(t, err)

		count := 0
		var value interface{}
		iter := rel.Iterator()
		for iter.Next() {
			count++
			value = iter.Tuple()[0]
		}

		correct := count == 1
		results = append(results, methodResult{"Query", count, value, correct})
		t.Logf("Query (streaming): %d results, correct=%v", count, correct)
	})

	// 7. QueryInto
	t.Run("QueryInto", func(t *testing.T) {
		var names []string
		err := db.QueryInto(&names,
			`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
			personID, nameAttr)
		require.NoError(t, err)

		count := len(names)
		var value interface{}
		if count > 0 {
			value = names[count-1]
		}

		correct := count == 1
		results = append(results, methodResult{"QueryInto", count, value, correct})
		t.Logf("QueryInto: %d results, correct=%v", count, correct)
	})

	// 8. QueryOneInto
	t.Run("QueryOneInto", func(t *testing.T) {
		var name string
		found, err := db.QueryOneInto(&name,
			`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
			personID, nameAttr)

		// QueryOneInto correctly errors when multiple results are returned
		// This error is actually a symptom of the CRDT bug - there should only be one result
		if err != nil {
			t.Logf("QueryOneInto: error=%v (expected due to CRDT bug returning multiple results)", err)
			results = append(results, methodResult{"QueryOneInto", -1, "ERROR: multiple results", false})
			return
		}

		count := 0
		if found {
			count = 1
		}

		correct := found && name == expectedName
		results = append(results, methodResult{"QueryOneInto", count, name, correct})
		t.Logf("QueryOneInto: found=%v, value=%q, correct=%v", found, name, correct)
	})

	// Print summary table
	t.Log("\n========== CRDT RESOLUTION MATRIX ==========")
	t.Log("Method                    | Count | Correct | Value")
	t.Log("--------------------------|-------|---------|------")
	for _, r := range results {
		status := "✗ BUG"
		if r.correct {
			status = "✓ OK "
		}
		t.Logf("%-25s | %5d | %s   | %v", r.name, r.count, status, r.value)
	}
	t.Log("=============================================")
	t.Log("Expected: 1 result with value 'Charlie' for all methods")
}
