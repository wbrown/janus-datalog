package storage

import (
	"os"
	"sort"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// bridgeTestDB holds a test database and pre-created identities for test cases.
type bridgeTestDB struct {
	DB                              *Database
	Dir                             string
	Alice, Bob, Charlie, Diana, Eve datalog.Identity
}

// setupBridgeTestDB creates a database with 5 people for testing bridging predicates.
//
// People:
//
//	alice:   "Alice",   age 30, team "alpha"
//	bob:     "Bob",     age 25, team "alpha"
//	charlie: "Charlie", age 35, team "beta"
//	diana:   "Diana",   age 28, team "beta"
//	eve:     "Eve",     age 40, team "gamma"
func setupBridgeTestDB(t *testing.T) *bridgeTestDB {
	t.Helper()
	dir, err := os.MkdirTemp("", "predicate-bridge-test-*")
	if err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create database: %v", err)
	}

	tdb := &bridgeTestDB{
		DB:      db,
		Dir:     dir,
		Alice:   datalog.NewIdentity("person:alice"),
		Bob:     datalog.NewIdentity("person:bob"),
		Charlie: datalog.NewIdentity("person:charlie"),
		Diana:   datalog.NewIdentity("person:diana"),
		Eve:     datalog.NewIdentity("person:eve"),
	}

	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	teamAttr := datalog.NewKeyword(":person/team")

	tx := db.NewTransaction()
	for _, p := range []struct {
		id   datalog.Identity
		name string
		age  int64
		team string
	}{
		{tdb.Alice, "Alice", 30, "alpha"},
		{tdb.Bob, "Bob", 25, "alpha"},
		{tdb.Charlie, "Charlie", 35, "beta"},
		{tdb.Diana, "Diana", 28, "beta"},
		{tdb.Eve, "Eve", 40, "gamma"},
	} {
		tx.Add(p.id, nameAttr, p.name)
		tx.Add(p.id, ageAttr, p.age)
		tx.Add(p.id, teamAttr, p.team)
	}
	_, err = tx.Commit()
	if err != nil {
		db.Close()
		os.RemoveAll(dir)
		t.Fatalf("Failed to commit: %v", err)
	}
	return tdb
}

func (tdb *bridgeTestDB) cleanup() {
	tdb.DB.Close()
	os.RemoveAll(tdb.Dir)
}

// collectStrings extracts the first column as strings from query results.
func collectStrings(results [][]interface{}) []string {
	var out []string
	for _, row := range results {
		if len(row) > 0 {
			if s, ok := row[0].(string); ok {
				out = append(out, s)
			}
		}
	}
	sort.Strings(out)
	return out
}

// --- Group 1: Scalar input constant-bindable (planner path) ---

// TestScalarInput_InequalityPredicate tests a scalar input bridging via not=.
// The scalar ?self only appears in the predicate, not in any data pattern,
// so the planner should mark it as constant-bindable.
func TestScalarInput_InequalityPredicate(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $ ?self
		  :where
		  [?e :person/name ?name]
		  [(not= ?e ?self)]]`,
		tdb.Alice,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	got := collectStrings(results)
	expect := []string{"Bob", "Charlie", "Diana", "Eve"}
	if len(got) != len(expect) {
		t.Fatalf("Expected %d results %v, got %d: %v", len(expect), expect, len(got), got)
	}
	for i, name := range expect {
		if got[i] != name {
			t.Errorf("Result[%d]: expected %q, got %q", i, name, got[i])
		}
	}
}

// TestScalarInput_ComparisonPredicate tests a scalar threshold bridging via >.
// ?min-age only appears in the predicate, not in any data pattern.
func TestScalarInput_ComparisonPredicate(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $ ?min-age
		  :where
		  [?e :person/name ?name]
		  [?e :person/age ?age]
		  [(> ?age ?min-age)]]`,
		int64(30),
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	got := collectStrings(results)
	expect := []string{"Charlie", "Eve"} // ages 35, 40 > 30
	if len(got) != len(expect) {
		t.Fatalf("Expected %d results %v, got %d: %v", len(expect), expect, len(got), got)
	}
	for i, name := range expect {
		if got[i] != name {
			t.Errorf("Result[%d]: expected %q, got %q", i, name, got[i])
		}
	}
}

// TestScalarInput_EqualityPredicate tests a scalar that appears IN a data pattern.
// ?target-team is used in [?e :person/team ?target-team], so it is NOT
// constant-bindable — it joins normally via the pattern.
func TestScalarInput_EqualityPredicate(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $ ?target-team
		  :where
		  [?e :person/name ?name]
		  [?e :person/team ?target-team]]`,
		"alpha",
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	got := collectStrings(results)
	expect := []string{"Alice", "Bob"}
	if len(got) != len(expect) {
		t.Fatalf("Expected %d results %v, got %d: %v", len(expect), expect, len(got), got)
	}
	for i, name := range expect {
		if got[i] != name {
			t.Errorf("Result[%d]: expected %q, got %q", i, name, got[i])
		}
	}
}

// TestScalarInput_MultipleBridgingPredicates tests two scalar inputs, each
// bridging a separate predicate. Both ?self and ?min-age are constant-bindable.
func TestScalarInput_MultipleBridgingPredicates(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $ ?self ?min-age
		  :where
		  [?e :person/name ?name]
		  [?e :person/age ?age]
		  [(not= ?e ?self)]
		  [(> ?age ?min-age)]]`,
		tdb.Alice, int64(28),
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	got := collectStrings(results)
	// Not alice, age > 28: charlie(35), eve(40). Diana(28) excluded by strict >.
	expect := []string{"Charlie", "Eve"}
	if len(got) != len(expect) {
		t.Fatalf("Expected %d results %v, got %d: %v", len(expect), expect, len(got), got)
	}
	for i, name := range expect {
		if got[i] != name {
			t.Errorf("Result[%d]: expected %q, got %q", i, name, got[i])
		}
	}
}

// TestScalarInput_ExpressionBridge tests a scalar input bridging via an expression.
// ?bonus only appears in the expression [(+ ?age ?bonus) ?adjusted], not in data patterns.
func TestScalarInput_ExpressionBridge(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name ?adjusted
		  :in $ ?bonus
		  :where
		  [?e :person/name ?name]
		  [?e :person/age ?age]
		  [(+ ?age ?bonus) ?adjusted]]`,
		int64(10),
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(results))
	}

	// Collect name -> adjusted age
	adjusted := make(map[string]int64)
	for _, row := range results {
		name, _ := row[0].(string)
		age, _ := row[1].(int64)
		adjusted[name] = age
	}

	expect := map[string]int64{
		"Alice": 40, "Bob": 35, "Charlie": 45, "Diana": 38, "Eve": 50,
	}
	for name, expectedAge := range expect {
		if adjusted[name] != expectedAge {
			t.Errorf("%s: expected adjusted age %d, got %d", name, expectedAge, adjusted[name])
		}
	}
}

// --- Group 2: Scalar appears in data pattern — NOT constant-bindable ---

// TestScalarInput_InPatternNotConstantBindable tests that a scalar appearing in
// a data pattern is NOT treated as constant-bindable. ?target-name is used in
// [?e :person/name ?target-name] to find the person, then we find teammates.
func TestScalarInput_InPatternNotConstantBindable(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?other-name
		  :in $ ?target-name
		  :where
		  [?e :person/name ?target-name]
		  [?e :person/team ?team]
		  [?other :person/team ?team]
		  [?other :person/name ?other-name]
		  [(not= ?other ?e)]]`,
		"Alice",
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	got := collectStrings(results)
	// Alice is on team "alpha". Bob is the only other alpha member.
	expect := []string{"Bob"}
	if len(got) != len(expect) {
		t.Fatalf("Expected %d results %v, got %d: %v", len(expect), expect, len(got), got)
	}
	if got[0] != "Bob" {
		t.Errorf("Expected 'Bob', got %q", got[0])
	}
}

// --- Group 3: Non-scalar bridging — theta-join fallback ---

// TestCollectionInput_BridgingPredicate tests a collection input where each
// excluded name must be checked against all people. This is NOT constant-bindable
// because it's a collection (multi-row), not a scalar.
//
// IMPORTANT semantic note: The predicate [(not= ?name ?excluded-name)] performs
// PAIRWISE comparison, not set membership testing. So:
//   - Alice paired with "Bob" → passes (Alice ≠ Bob) → Alice IS in results
//   - Bob paired with "Alice" → passes (Bob ≠ Alice) → Bob IS in results
//
// This is NOT "find names not in the exclusion set". For that semantics,
// you'd need a NOT clause with a subquery pattern.
func TestCollectionInput_BridgingPredicate(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $ [?excluded-name ...]
		  :where
		  [?e :person/name ?name]
		  [(not= ?name ?excluded-name)]]`,
		[]string{"Alice", "Bob"},
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	got := collectStrings(results)
	// Pairwise not= produces all (name, excluded) pairs where name ≠ excluded.
	// Alice×Bob passes, Bob×Alice passes, etc. After projection to ?name: all 5.
	expect := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	if len(got) != len(expect) {
		t.Fatalf("Expected %d results %v, got %d: %v", len(expect), expect, len(got), got)
	}
	for i, name := range expect {
		if got[i] != name {
			t.Errorf("Result[%d]: expected %q, got %q", i, name, got[i])
		}
	}
}

// TestCollectionInput_ComparisonBridge tests a collection of thresholds compared
// against ages. The theta-join produces all valid (person, threshold) combinations,
// then projection to :find columns deduplicates.
func TestCollectionInput_ComparisonBridge(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name ?age
		  :in $ [?threshold ...]
		  :where
		  [?e :person/name ?name]
		  [?e :person/age ?age]
		  [(> ?age ?threshold)]]`,
		[]interface{}{int64(25), int64(35)},
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Theta-join produces 5 passing (person, threshold) combinations:
	//   alice(30) > 25 yes, > 35 no  → 1 combination
	//   bob(25)   > 25 no,  > 35 no  → 0 combinations
	//   charlie(35) > 25 yes, > 35 no → 1 combination
	//   diana(28) > 25 yes, > 35 no   → 1 combination
	//   eve(40)   > 25 yes, > 35 yes  → 2 combinations
	// Total: 5 combinations.
	//
	// But :find ?name ?age projects away ?threshold, and Eve(40) appears twice
	// (once for threshold 25, once for 35). After deduplication: 4 unique rows.
	if len(results) != 4 {
		t.Fatalf("Expected 4 results (Eve deduplicated), got %d: %v", len(results), results)
	}

	// Verify the specific (name, age) pairs we expect
	type row struct {
		name string
		age  int64
	}
	got := make(map[row]bool)
	for _, r := range results {
		name, _ := r[0].(string)
		age, _ := r[1].(int64)
		got[row{name, age}] = true
	}

	for _, expected := range []row{
		{"Alice", 30},   // > 25
		{"Charlie", 35}, // > 25
		{"Diana", 28},   // > 25
		{"Eve", 40},     // > 25 and > 35, but deduplicated
	} {
		if !got[expected] {
			t.Errorf("Missing expected row: (%s, %d)", expected.name, expected.age)
		}
	}
}

// TestDisjointPatterns_BridgedByPredicate tests two completely disjoint pattern
// groups (no shared variables, no inputs) bridged only by a predicate.
// This is the pure theta-join case — no constant optimization possible.
func TestDisjointPatterns_BridgedByPredicate(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?n1 ?n2
		  :where
		  [?e1 :person/name ?n1] [?e1 :person/team "alpha"]
		  [?e2 :person/name ?n2] [?e2 :person/team "beta"]
		  [(not= ?e1 ?e2)]]`,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// alpha = {Alice, Bob}, beta = {Charlie, Diana}
	// not= always true since different entities
	// Expect 2×2 = 4 rows
	if len(results) != 4 {
		t.Fatalf("Expected 4 results, got %d: %v", len(results), results)
	}

	type pair struct{ n1, n2 string }
	got := make(map[pair]bool)
	for _, row := range results {
		n1, _ := row[0].(string)
		n2, _ := row[1].(string)
		got[pair{n1, n2}] = true
	}

	for _, expected := range []pair{
		{"Alice", "Charlie"}, {"Alice", "Diana"},
		{"Bob", "Charlie"}, {"Bob", "Diana"},
	} {
		if !got[expected] {
			t.Errorf("Missing expected pair: (%s, %s)", expected.n1, expected.n2)
		}
	}
}

// --- Group 4: Expression bridging disjoint groups (cross-join) ---

// TestDisjointGroups_ExpressionBridge tests an expression that requires symbols
// from two disjoint pattern groups, forcing a cross-join.
func TestDisjointGroups_ExpressionBridge(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?n1 ?n2 ?combined
		  :where
		  [?e1 :person/name ?n1] [?e1 :person/team "alpha"]
		  [?e2 :person/name ?n2] [?e2 :person/team "beta"]
		  [(str ?n1 " & " ?n2) ?combined]]`,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// alpha = {Alice, Bob}, beta = {Charlie, Diana} → 4 combinations
	if len(results) != 4 {
		t.Fatalf("Expected 4 results, got %d: %v", len(results), results)
	}

	type row struct{ n1, n2, combined string }
	got := make(map[row]bool)
	for _, r := range results {
		n1, _ := r[0].(string)
		n2, _ := r[1].(string)
		combined, _ := r[2].(string)
		got[row{n1, n2, combined}] = true
	}

	for _, expected := range []row{
		{"Alice", "Charlie", "Alice & Charlie"},
		{"Alice", "Diana", "Alice & Diana"},
		{"Bob", "Charlie", "Bob & Charlie"},
		{"Bob", "Diana", "Bob & Diana"},
	} {
		if !got[expected] {
			t.Errorf("Missing expected row: %v", expected)
		}
	}
}

// --- Group 5: Edge cases ---

// TestScalarInput_AllExcluded tests that a scalar threshold that excludes
// all entities returns 0 results.
func TestScalarInput_AllExcluded(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	results, err := tdb.DB.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $ ?min-age
		  :where
		  [?e :person/name ?name]
		  [?e :person/age ?age]
		  [(> ?age ?min-age)]]`,
		int64(100),
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results (nobody > 100), got %d: %v", len(results), results)
	}
}

// TestScalarInput_SingleEntitySelfExclusion tests self-exclusion when the
// database contains only one entity.
func TestScalarInput_SingleEntitySelfExclusion(t *testing.T) {
	dir, err := os.MkdirTemp("", "predicate-single-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("person:alice")
	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	results, err := db.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $ ?self
		  :where
		  [?e :person/name ?name]
		  [(not= ?e ?self)]]`,
		alice,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results (only entity excluded), got %d: %v", len(results), results)
	}
}

// --- Group 6: Planner metadata verification ---

// TestPlanner_ConstantBindableDetection verifies the planner marks scalar inputs
// that only appear in predicates (not data patterns) as constant-bindable.
func TestPlanner_ConstantBindableDetection(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	plan, err := tdb.DB.Explain(
		`[:find ?e
		  :in $ ?parent
		  :where
		  [?e :person/name ?name]
		  [(not= ?e ?parent)]]`,
		tdb.Alice,
	)
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}

	// The planner should mark ?parent as constant-bindable because it only
	// appears in the predicate [(not= ?e ?parent)], not in any data pattern.
	found := false
	parentSym := datalog.NewSymbol("?parent")
	for _, phase := range plan.Phases {
		if cbInputs, ok := phase.Metadata["constant_bindable_inputs"]; ok {
			if syms, ok := cbInputs.([]query.Symbol); ok {
				for _, sym := range syms {
					if sym == parentSym {
						found = true
					}
				}
			}
		}
	}
	if !found {
		t.Errorf("Expected planner to mark ?parent as constant-bindable, but metadata did not contain it")
		for i, phase := range plan.Phases {
			t.Logf("Phase %d metadata: %v", i, phase.Metadata)
		}
	}
}

// TestPlanner_ScalarInPatternNotConstantBindable verifies the planner does NOT
// mark a scalar input as constant-bindable when it appears in a data pattern.
func TestPlanner_ScalarInPatternNotConstantBindable(t *testing.T) {
	tdb := setupBridgeTestDB(t)
	defer tdb.cleanup()

	plan, err := tdb.DB.Explain(
		`[:find ?name
		  :in $ ?target
		  :where
		  [?e :person/name ?target]
		  [?e :person/age ?age]]`,
		"Alice",
	)
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}

	// ?target appears in data pattern [?e :person/name ?target], so it should
	// NOT be marked as constant-bindable.
	targetSym := datalog.NewSymbol("?target")
	for _, phase := range plan.Phases {
		if cbInputs, ok := phase.Metadata["constant_bindable_inputs"]; ok {
			if syms, ok := cbInputs.([]query.Symbol); ok {
				for _, sym := range syms {
					if sym == targetSym {
						t.Errorf("?target should NOT be constant-bindable (it appears in a data pattern)")
					}
				}
			}
		}
	}
}
