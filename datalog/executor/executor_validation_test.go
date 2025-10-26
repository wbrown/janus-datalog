package executor_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// setupTestDatabase creates a test database with comprehensive data for validation
func setupTestDatabase(t *testing.T) (*storage.Database, func()) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "executor-validation-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")

	// Open database
	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create transaction
	tx := db.NewTransaction()

	// Add test data with various patterns
	baseTime := time.Date(2025, 6, 1, 9, 30, 0, 0, time.UTC)

	// People with various attributes
	people := []struct {
		id    string
		name  string
		age   int64
		city  string
		score int64
	}{
		{"person1", "Alice", 30, "NYC", 100},
		{"person2", "Bob", 25, "SF", 85},
		{"person3", "Carol", 35, "NYC", 92},
		{"person4", "Dave", 28, "LA", 78},
		{"person5", "Eve", 32, "SF", 95},
	}

	for _, p := range people {
		personID := datalog.NewIdentity(p.id)
		tx.Add(personID, datalog.NewKeyword(":person/name"), p.name)
		tx.Add(personID, datalog.NewKeyword(":person/age"), p.age)
		tx.Add(personID, datalog.NewKeyword(":person/city"), p.city)
		tx.Add(personID, datalog.NewKeyword(":person/score"), p.score)
	}

	// Events with timestamps
	events := []struct {
		id     string
		person string
		value  int64
		time   time.Time
	}{
		{"event1", "person1", 50, baseTime},
		{"event2", "person1", 75, baseTime.Add(1 * time.Hour)},
		{"event3", "person2", 60, baseTime.Add(2 * time.Hour)},
		{"event4", "person3", 80, baseTime.Add(3 * time.Hour)},
		{"event5", "person3", 90, baseTime.Add(4 * time.Hour)},
	}

	for _, e := range events {
		eventID := datalog.NewIdentity(e.id)
		personID := datalog.NewIdentity(e.person)
		tx.Add(eventID, datalog.NewKeyword(":event/person"), personID)
		tx.Add(eventID, datalog.NewKeyword(":event/value"), e.value)
		tx.Add(eventID, datalog.NewKeyword(":event/time"), e.time)
	}

	// Commit transaction
	_, err = tx.Commit()
	if err != nil {
		db.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

// compareResults compares two relations for equivalence
func compareResults(t *testing.T, testName string, legacy, new executor.Relation) {
	t.Helper()

	// Compare columns first
	legacyCols := legacy.Columns()
	newCols := new.Columns()
	if !equalSymbols(legacyCols, newCols) {
		t.Errorf("%s: Column mismatch - Legacy: %v, New: %v",
			testName, legacyCols, newCols)
		return
	}

	// Get sorted tuples for deterministic comparison (this iterates the relations ONCE)
	legacyTuples := legacy.Sorted()
	newTuples := new.Sorted()

	// Compare sizes
	if len(legacyTuples) != len(newTuples) {
		t.Errorf("%s: Size mismatch - Legacy: %d, New: %d",
			testName, len(legacyTuples), len(newTuples))
		// Format tuples we already have - don't iterate again!
		t.Logf("Legacy result:\n%s", formatTuples(legacyCols, legacyTuples))
		t.Logf("New result:\n%s", formatTuples(newCols, newTuples))
		return
	}

	// Compare tuple by tuple
	for i := 0; i < len(legacyTuples); i++ {
		if !equalTuples(legacyTuples[i], newTuples[i]) {
			t.Errorf("%s: Tuple %d mismatch:\n  Legacy: %v\n  New: %v",
				testName, i, legacyTuples[i], newTuples[i])
		}
	}
}

// formatTuples formats tuples as a markdown table without iterating
func formatTuples(columns []query.Symbol, tuples []executor.Tuple) string {
	if len(tuples) == 0 {
		return "(empty)"
	}

	var buf strings.Builder

	// Header
	buf.WriteString("|")
	for _, col := range columns {
		buf.WriteString(fmt.Sprintf(" %s |", col))
	}
	buf.WriteString("\n|")
	for range columns {
		buf.WriteString("---|")
	}
	buf.WriteString("\n")

	// Rows
	for _, tuple := range tuples {
		buf.WriteString("|")
		for i := range columns {
			if i < len(tuple) {
				buf.WriteString(fmt.Sprintf(" %v |", tuple[i]))
			} else {
				buf.WriteString(" NULL |")
			}
		}
		buf.WriteString("\n")
	}

	return buf.String()
}

func equalSymbols(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalTuples(a, b executor.Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		// Use string comparison for simplicity (fmt.Sprintf handles all types)
		aStr := fmt.Sprintf("%v", a[i])
		bStr := fmt.Sprintf("%v", b[i])
		if aStr != bStr {
			return false
		}
	}
	return true
}

// TestComprehensiveExecutorValidation runs comprehensive validation tests
func TestComprehensiveExecutorValidation(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Create planner with default options
	plannerOpts := planner.PlannerOptions{
		EnableDynamicReordering:     true,
		EnablePredicatePushdown:     true,
		EnableSubqueryDecorrelation: false, // Disable for simpler validation
		EnableCSE:                   false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
		EnableIteratorComposition:   true, // Enable streaming
		EnableTrueStreaming:          true, // Avoid premature materialization
	}

	// Create matcher with options so StreamingRelations have correct settings
	execOpts := executor.ExecutorOptions{
		EnableIteratorComposition: plannerOpts.EnableIteratorComposition,
		EnableTrueStreaming:       plannerOpts.EnableTrueStreaming,
	}
	matcher := storage.NewBadgerMatcherWithOptions(db.Store(), execOpts)

	tests := []struct {
		name        string
		queryString string
		skip        bool
		skipReason  string
	}{
		// Basic pattern queries
		{
			name: "single pattern",
			queryString: `[:find ?person ?name
			               :where [?person :person/name ?name]]`,
		},
		{
			name: "two patterns join",
			queryString: `[:find ?name ?age
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]]`,
		},
		{
			name: "three patterns join",
			queryString: `[:find ?name ?age ?city
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]
			                      [?person :person/city ?city]]`,
		},

		// Predicate queries
		{
			name: "simple predicate",
			queryString: `[:find ?name ?age
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]
			                      [(> ?age 28)]]`,
		},
		{
			name: "multiple predicates",
			queryString: `[:find ?name ?age
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]
			                      [(> ?age 25)]
			                      [(< ?age 35)]]`,
		},
		{
			name: "equality predicate",
			queryString: `[:find ?name ?city
			               :where [?person :person/name ?name]
			                      [?person :person/city ?city]
			                      [(= ?city "NYC")]]`,
		},

		// Expression queries
		{
			name: "arithmetic expression",
			queryString: `[:find ?name ?doubled
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]
			                      [(* ?age 2) ?doubled]]`,
		},
		{
			name: "multiple expressions",
			queryString: `[:find ?name ?total
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]
			                      [?person :person/score ?score]
			                      [(+ ?age ?score) ?total]]`,
		},
		{
			name: "expression with predicate",
			queryString: `[:find ?name ?doubled
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]
			                      [(* ?age 2) ?doubled]
			                      [(> ?doubled 55)]]`,
		},

		// Aggregation queries
		{
			name: "simple count",
			queryString: `[:find (count ?person)
			               :where [?person :person/name ?name]]`,
		},
		{
			name: "grouped sum",
			queryString: `[:find ?city (sum ?age)
			               :where [?person :person/city ?city]
			                      [?person :person/age ?age]]`,
		},
		{
			name: "grouped count",
			queryString: `[:find ?city (count ?person)
			               :where [?person :person/city ?city]]`,
		},
		{
			name: "max aggregation",
			queryString: `[:find ?city (max ?age)
			               :where [?person :person/city ?city]
			                      [?person :person/age ?age]]`,
		},
		{
			name: "min aggregation",
			queryString: `[:find ?city (min ?score)
			               :where [?person :person/city ?city]
			                      [?person :person/score ?score]]`,
		},
		{
			name: "avg aggregation",
			queryString: `[:find ?city (avg ?score)
			               :where [?person :person/city ?city]
			                      [?person :person/score ?score]]`,
		},

		// Multi-phase queries (should trigger phase separation)
		{
			name: "join across entities",
			queryString: `[:find ?name ?value
			               :where [?event :event/person ?person]
			                      [?person :person/name ?name]
			                      [?event :event/value ?value]]`,
		},
		{
			name: "multi-phase with predicate",
			queryString: `[:find ?name ?value
			               :where [?event :event/person ?person]
			                      [?person :person/name ?name]
			                      [?event :event/value ?value]
			                      [(> ?value 60)]]`,
		},
		{
			name: "multi-phase with expression",
			queryString: `[:find ?name ?total
			               :where [?event :event/person ?person]
			                      [?person :person/name ?name]
			                      [?person :person/score ?score]
			                      [?event :event/value ?value]
			                      [(+ ?score ?value) ?total]]`,
		},

		// Time-based queries
		{
			name: "time extraction - day",
			queryString: `[:find ?value ?day
			               :where [?event :event/value ?value]
			                      [?event :event/time ?time]
			                      [(day ?time) ?day]]`,
		},
		{
			name: "time extraction - hour",
			queryString: `[:find ?value ?hour
			               :where [?event :event/value ?value]
			                      [?event :event/time ?time]
			                      [(hour ?time) ?hour]]`,
		},
		{
			name: "time with predicate",
			queryString: `[:find ?value ?hour
			               :where [?event :event/value ?value]
			                      [?event :event/time ?time]
			                      [(hour ?time) ?hour]
			                      [(>= ?hour 10)]]`,
		},

		// Complex combinations
		{
			name: "complex join + filter + aggregation",
			queryString: `[:find ?city (sum ?value)
			               :where [?event :event/person ?person]
			                      [?person :person/city ?city]
			                      [?event :event/value ?value]
			                      [(> ?value 60)]]`,
		},
		{
			name: "all features combined",
			queryString: `[:find ?city (sum ?total)
			               :where [?event :event/person ?person]
			                      [?person :person/city ?city]
			                      [?person :person/score ?score]
			                      [?event :event/value ?value]
			                      [(+ ?score ?value) ?total]
			                      [(> ?total 100)]]`,
		},

		// Edge cases
		{
			name: "pattern with no results",
			queryString: `[:find ?name
			               :where [?person :person/name ?name]
			                      [?person :person/age 999]]`,
		},
		{
			name: "filter eliminates all results",
			queryString: `[:find ?name ?age
			               :where [?person :person/name ?name]
			                      [?person :person/age ?age]
			                      [(> ?age 100)]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.skipReason)
			}

			// Parse query
			parsedQuery, err := parser.ParseQuery(tt.queryString)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Execute with LEGACY executor (separate context to avoid state sharing)
			legacyCtx := executor.NewContext(nil)
			legacyOpts := plannerOpts
			legacyOpts.EnableIteratorComposition = true
			legacyExec := executor.NewExecutorWithOptions(matcher, legacyOpts)

			legacyResult, legacyErr := legacyExec.ExecuteWithContext(legacyCtx, parsedQuery)

			// Execute with NEW QueryExecutor (separate context to avoid state sharing)
			newCtx := executor.NewContext(nil)
			newOpts := plannerOpts
			newOpts.EnableIteratorComposition = true
			// Need to explicitly set UseQueryExecutor in executor options
			newExec := executor.NewExecutorWithOptions(matcher, newOpts)
			newExec.SetUseQueryExecutor(true)
			newResult, newErr := newExec.ExecuteWithContext(newCtx, parsedQuery)

			// Compare errors
			if (legacyErr != nil) != (newErr != nil) {
				t.Fatalf("Error mismatch:\n  Legacy error: %v\n  New error: %v",
					legacyErr, newErr)
			}

			if legacyErr != nil {
				// Both errored - that's expected for some queries
				t.Logf("Both executors errored (expected): %v", legacyErr)
				return
			}

			// Compare results
			compareResults(t, tt.name, legacyResult, newResult)
		})
	}
}

// TestExecutorValidationWithVariousOptimizations tests with different optimization flags
func TestExecutorValidationWithVariousOptimizations(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Create matcher with streaming options
	execOpts := executor.ExecutorOptions{
		EnableIteratorComposition: true,
		EnableTrueStreaming:       true,
	}
	matcher := storage.NewBadgerMatcherWithOptions(db.Store(), execOpts)

	// Test query that exercises multiple features
	queryString := `[:find ?name (sum ?value)
	                 :where [?event :event/person ?person]
	                        [?person :person/name ?name]
	                        [?event :event/value ?value]
	                        [(> ?value 55)]]`

	parsedQuery, err := parser.ParseQuery(queryString)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	optimizationConfigs := []struct {
		name string
		opts planner.PlannerOptions
	}{
		{
			name: "all optimizations disabled",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:     false,
				EnablePredicatePushdown:     false,
				EnableSubqueryDecorrelation: false,
				EnableCSE:                   false,
				EnableFineGrainedPhases:     false,
			},
		},
		{
			name: "only dynamic reordering",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:     true,
				EnablePredicatePushdown:     false,
				EnableSubqueryDecorrelation: false,
				EnableCSE:                   false,
				EnableFineGrainedPhases:     false,
			},
		},
		{
			name: "reordering + predicate pushdown",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:     true,
				EnablePredicatePushdown:     true,
				EnableSubqueryDecorrelation: false,
				EnableCSE:                   false,
				EnableFineGrainedPhases:     false,
			},
		},
		{
			name: "all basic optimizations",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:     true,
				EnablePredicatePushdown:     true,
				EnableSubqueryDecorrelation: false,
				EnableCSE:                   false,
				EnableFineGrainedPhases:     true,
			},
		},
	}

	for _, config := range optimizationConfigs {
		t.Run(config.name, func(t *testing.T) {
			ctx := executor.NewContext(nil)

			// Legacy executor
			legacyExec := executor.NewExecutorWithOptions(matcher, config.opts)
			legacyResult, legacyErr := legacyExec.ExecuteWithContext(ctx, parsedQuery)

			// New executor
			newExec := executor.NewExecutorWithOptions(matcher, config.opts)
			newExec.SetUseQueryExecutor(true)
			newResult, newErr := newExec.ExecuteWithContext(ctx, parsedQuery)

			// Compare
			if (legacyErr != nil) != (newErr != nil) {
				t.Fatalf("Error mismatch:\n  Legacy: %v\n  New: %v", legacyErr, newErr)
			}

			if legacyErr == nil {
				compareResults(t, config.name, legacyResult, newResult)
			}
		})
	}
}

// TestExecutorValidationEdgeCases tests various edge cases
func TestExecutorValidationEdgeCases(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Create matcher with streaming options
	execOpts := executor.ExecutorOptions{
		EnableIteratorComposition: true,
		EnableTrueStreaming:       true,
	}
	matcher := storage.NewBadgerMatcherWithOptions(db.Store(), execOpts)
	opts := planner.PlannerOptions{
		EnableDynamicReordering: true,
		EnablePredicatePushdown: true,
		EnableFineGrainedPhases: true,
	}

	tests := []struct {
		name        string
		queryString string
	}{
		{
			name:        "empty result set",
			queryString: `[:find ?x :where [?x :nonexistent/attr ?y]]`,
		},
		{
			name: "single result",
			queryString: `[:find ?name
			               :where [?person :person/name ?name]
			                      [(= ?name "Alice")]]`,
		},
		{
			name: "cross product (should error in new executor)",
			queryString: `[:find ?name ?value
			               :where [?person :person/name ?name]
			                      [?event :event/value ?value]]`,
			// This will create disjoint groups - legacy might handle it,
			// but new executor should error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedQuery, err := parser.ParseQuery(tt.queryString)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			ctx := executor.NewContext(nil)

			legacyExec := executor.NewExecutorWithOptions(matcher, opts)
			legacyResult, legacyErr := legacyExec.ExecuteWithContext(ctx, parsedQuery)

			newExec := executor.NewExecutorWithOptions(matcher, opts)
			newExec.SetUseQueryExecutor(true)
			newResult, newErr := newExec.ExecuteWithContext(ctx, parsedQuery)

			// For some edge cases, we expect different behavior
			// Log differences for analysis
			t.Logf("Legacy error: %v", legacyErr)
			t.Logf("New error: %v", newErr)

			if legacyErr == nil && newErr == nil {
				compareResults(t, tt.name, legacyResult, newResult)
			} else if legacyErr == nil && newErr != nil {
				t.Logf("New executor rejected query (may be correct): %v", newErr)
				t.Logf("Legacy result size: %d", legacyResult.Size())
			}
		})
	}
}
