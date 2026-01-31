package storage

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestPullIntegration_StandaloneAPI(t *testing.T) {
	// Create temporary database
	tmpDir, err := os.MkdirTemp("", "pull-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := NewDatabase(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create test entities
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	region := datalog.NewIdentity("region:us-west")

	// Add data
	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":user/age"), int64(30))
	tx.Add(alice, datalog.NewKeyword(":user/email"), "alice@example.com")
	tx.Add(alice, datalog.NewKeyword(":user/region"), region)

	tx.Add(bob, datalog.NewKeyword(":user/name"), "Bob")
	tx.Add(bob, datalog.NewKeyword(":user/age"), int64(25))

	tx.Add(region, datalog.NewKeyword(":region/code"), "US-W")
	tx.Add(region, datalog.NewKeyword(":region/name"), "US West")

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Test simple pull
	t.Run("SimpleAttributes", func(t *testing.T) {
		result, err := db.Pull(alice, `[:user/name :user/age]`)
		if err != nil {
			t.Fatalf("pull failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result["user/name"] != "Alice" {
			t.Errorf("expected name=Alice, got %v", result["user/name"])
		}
		if result["user/age"] != int64(30) {
			t.Errorf("expected age=30, got %v", result["user/age"])
		}
		// Email should NOT be present (not in pattern)
		if _, ok := result["user/email"]; ok {
			t.Error("email should not be in result")
		}
	})

	// Test wildcard pull
	t.Run("Wildcard", func(t *testing.T) {
		result, err := db.Pull(alice, `[*]`)
		if err != nil {
			t.Fatalf("pull failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		// Should have all 4 attributes (name, age, email, region)
		if len(result) != 4 {
			t.Errorf("expected 4 attributes, got %d: %v", len(result), result)
		}
	})

	// Test nested reference pull
	t.Run("NestedReference", func(t *testing.T) {
		result, err := db.Pull(alice, `[:user/name {:user/region [:region/code :region/name]}]`)
		if err != nil {
			t.Fatalf("pull failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result["user/name"] != "Alice" {
			t.Errorf("expected name=Alice, got %v", result["user/name"])
		}

		// Check nested region
		nestedRegion, ok := result["user/region"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected nested map for user/region, got %T", result["user/region"])
		}
		if nestedRegion["region/code"] != "US-W" {
			t.Errorf("expected region/code=US-W, got %v", nestedRegion["region/code"])
		}
		if nestedRegion["region/name"] != "US West" {
			t.Errorf("expected region/name=US West, got %v", nestedRegion["region/name"])
		}
	})

	// Test default value
	t.Run("DefaultValue", func(t *testing.T) {
		result, err := db.Pull(alice, `[:user/name (default :user/status "active")]`)
		if err != nil {
			t.Fatalf("pull failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		// Status should have default value since it doesn't exist
		if result["user/status"] != "active" {
			t.Errorf("expected status=active (default), got %v", result["user/status"])
		}
	})

	// Test limit on cardinality-many attribute
	t.Run("LimitCardinalityMany", func(t *testing.T) {
		// Define schema with cardinality-many for :user/tag
		// (without schema, attributes default to cardinality-one with CRDT semantics)
		s := schema.NewSchema()
		s.Add(&schema.AttributeDefinition{
			Ident:       datalog.NewKeyword(":user/tag"),
			ValueType:   schema.TypeString,
			Cardinality: schema.CardinalityMany,
		})
		db.SetSchema(s)

		// Create entity with multiple tags
		taggedEntity := datalog.NewIdentity("user:tagged")
		tx2 := db.NewTransaction()
		tx2.Add(taggedEntity, datalog.NewKeyword(":user/name"), "Tagged User")
		tx2.Add(taggedEntity, datalog.NewKeyword(":user/tag"), "tag1")
		tx2.Add(taggedEntity, datalog.NewKeyword(":user/tag"), "tag2")
		tx2.Add(taggedEntity, datalog.NewKeyword(":user/tag"), "tag3")
		tx2.Add(taggedEntity, datalog.NewKeyword(":user/tag"), "tag4")
		tx2.Add(taggedEntity, datalog.NewKeyword(":user/tag"), "tag5")
		if _, err := tx2.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		// Pull with limit
		result, err := db.Pull(taggedEntity, `[:user/name (limit :user/tag 2)]`)
		if err != nil {
			t.Fatalf("pull failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}

		// Check name
		if result["user/name"] != "Tagged User" {
			t.Errorf("expected name='Tagged User', got %v", result["user/name"])
		}

		// Check tags are limited to 2
		tags, ok := result["user/tag"].([]interface{})
		if !ok {
			t.Fatalf("expected tags to be []interface{}, got %T", result["user/tag"])
		}
		if len(tags) != 2 {
			t.Errorf("expected 2 tags (limited), got %d: %v", len(tags), tags)
		}
	})

	// Test non-existent entity
	t.Run("NonExistentEntity", func(t *testing.T) {
		nonExistent := datalog.NewIdentity("user:nonexistent")
		result, err := db.Pull(nonExistent, `[:user/name]`)
		if err != nil {
			t.Fatalf("pull failed: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil for non-existent entity, got %v", result)
		}
	})

	// Test PullMany
	t.Run("PullMany", func(t *testing.T) {
		results, err := db.PullMany([]datalog.Identity{alice, bob}, `[:user/name :user/age]`)
		if err != nil {
			t.Fatalf("pullMany failed: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		if results[0]["user/name"] != "Alice" {
			t.Errorf("expected first name=Alice, got %v", results[0]["user/name"])
		}
		if results[1]["user/name"] != "Bob" {
			t.Errorf("expected second name=Bob, got %v", results[1]["user/name"])
		}
	})
}

// TestPullWildcardPatternMatching tests that the underlying pattern matching works
// for entity-only patterns used by wildcard pull
func TestPullWildcardPatternMatching(t *testing.T) {
	// Create temporary database
	tmpDir, err := os.MkdirTemp("", "pull-pattern-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := NewDatabase(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create test entity
	alice := datalog.NewIdentity("user:alice")

	// Add data
	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":user/age"), int64(30))

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Test that we can query for all attributes of alice via standard query
	t.Run("QueryAllAttributes", func(t *testing.T) {
		results, err := db.ExecuteQueryWithInputs(`
			[:find ?a ?v
			 :in $ ?e
			 :where [?e ?a ?v]]
		`, alice)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d: %v", len(results), results)
		}
	})

	// Test direct pattern matching using the matcher
	t.Run("DirectPatternMatch", func(t *testing.T) {
		matcher := db.Matcher()

		// Create pattern: [alice ?a ?v]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: alice},
				query.Variable{Name: datalog.NewSymbol("?a")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		rel, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}
		if rel == nil {
			t.Fatal("expected non-nil relation")
		}

		// Iterate and count results
		var count int
		it := rel.Iterator()
		for it.Next() {
			count++
			tuple := it.Tuple()
			t.Logf("Found tuple: %v", tuple)
		}
		it.Close()

		if count != 2 {
			t.Errorf("expected 2 tuples, got %d", count)
		}
	})

	// Test the same pattern matching but with column extraction as pull does
	t.Run("PatternMatchWithColumnExtraction", func(t *testing.T) {
		matcher := db.Matcher()

		// Create pattern: [alice ?a ?v] - same as what getAllAttributes creates
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: alice},
				query.Variable{Name: datalog.NewSymbol("?a")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		rel, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Match failed: %v", err)
		}
		if rel == nil {
			t.Fatal("expected non-nil relation")
		}

		// Check columns
		cols := rel.Columns()
		t.Logf("Columns: %v", cols)

		// Find column indices like pull.go does
		aIdx := -1
		vIdx := -1
		for i, col := range cols {
			if col == datalog.NewSymbol("?a") {
				aIdx = i
			} else if col == datalog.NewSymbol("?v") {
				vIdx = i
			}
		}
		t.Logf("aIdx=%d, vIdx=%d", aIdx, vIdx)

		if aIdx < 0 || vIdx < 0 {
			t.Fatalf("missing expected columns: aIdx=%d, vIdx=%d", aIdx, vIdx)
		}

		// Iterate and build datoms like pull.go does
		var datoms []datalog.Datom
		it := rel.Iterator()
		defer it.Close()

		for it.Next() {
			tuple := it.Tuple()
			t.Logf("Tuple: %v (len=%d)", tuple, len(tuple))
			if aIdx < len(tuple) && vIdx < len(tuple) {
				// Handle *Keyword only - value-type Keywords are a bug
				var attr datalog.Keyword
				switch a := tuple[aIdx].(type) {
				case datalog.Keyword:
					if a == nil {
						continue
					}
					attr = a
				default:
					t.Logf("tuple[%d] is not Keyword or *Keyword, got %T: %v", aIdx, tuple[aIdx], tuple[aIdx])
					continue
				}
				datoms = append(datoms, datalog.Datom{
					E: alice,
					A: attr,
					V: tuple[vIdx],
				})
				t.Logf("Created datom: E=%v, A=%v, V=%v", alice, attr, tuple[vIdx])
			} else {
				t.Logf("indices out of range: aIdx=%d, vIdx=%d, len(tuple)=%d", aIdx, vIdx, len(tuple))
			}
		}

		if len(datoms) != 2 {
			t.Errorf("expected 2 datoms, got %d", len(datoms))
		}
	})
}

func TestPullIntegration_InQuery(t *testing.T) {
	// Create temporary database
	tmpDir, err := os.MkdirTemp("", "pull-query-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := NewDatabase(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create test entities
	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	personType := datalog.NewKeyword(":type/person")

	// Add data
	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":person/age"), int64(30))
	tx.Add(alice, datalog.NewKeyword(":entity/type"), personType)

	tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")
	tx.Add(bob, datalog.NewKeyword(":person/age"), int64(25))
	tx.Add(bob, datalog.NewKeyword(":entity/type"), personType)

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Test pull in query
	t.Run("PullInFindClause", func(t *testing.T) {
		results, err := db.ExecuteQuery(`
			[:find (pull ?e [:person/name :person/age])
			 :where [?e :entity/type :type/person]]
		`)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// Results should be pulled maps
		foundAlice := false
		foundBob := false
		for _, row := range results {
			if len(row) != 1 {
				t.Errorf("expected 1 column per row, got %d", len(row))
				continue
			}
			pulled, ok := row[0].(map[string]interface{})
			if !ok {
				t.Errorf("expected map[string]interface{}, got %T", row[0])
				continue
			}
			name := pulled["person/name"]
			if name == "Alice" {
				foundAlice = true
				if pulled["person/age"] != int64(30) {
					t.Errorf("Alice's age should be 30, got %v", pulled["person/age"])
				}
			} else if name == "Bob" {
				foundBob = true
				if pulled["person/age"] != int64(25) {
					t.Errorf("Bob's age should be 25, got %v", pulled["person/age"])
				}
			}
		}
		if !foundAlice {
			t.Error("Alice not found in results")
		}
		if !foundBob {
			t.Error("Bob not found in results")
		}
	})

	// Test mixed find clause (pull + regular variable)
	t.Run("MixedFindClause", func(t *testing.T) {
		results, err := db.ExecuteQuery(`
			[:find ?type (pull ?e [:person/name])
			 :where [?e :entity/type ?type]]
		`)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// Each row should have [type, pulled_map]
		for _, row := range results {
			if len(row) != 2 {
				t.Errorf("expected 2 columns, got %d", len(row))
				continue
			}

			// First column should be the type keyword
			typeKw, ok := row[0].(datalog.Keyword)
			if !ok {
				t.Errorf("expected Keyword for type, got %T", row[0])
			} else if typeKw.String() != ":type/person" {
				t.Errorf("expected type=:type/person, got %s", typeKw.String())
			}

			// Second column should be pulled map
			pulled, ok := row[1].(map[string]interface{})
			if !ok {
				t.Errorf("expected map for pull, got %T", row[1])
			} else {
				name := pulled["person/name"]
				if name != "Alice" && name != "Bob" {
					t.Errorf("unexpected name: %v", name)
				}
			}
		}
	})
}
