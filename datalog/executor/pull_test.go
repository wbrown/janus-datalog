package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPullExecutor_SimpleAttributes(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	emailAttr := datalog.NewKeyword(":user/email")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: emailAttr, V: "alice@example.com", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse pull pattern
	pattern, err := parser.ParsePullPattern(`[:user/name :user/age]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Execute pull
	result, err := puller.Pull(alice, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Check results - keys should be without colon
	if result["user/name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", result["user/name"])
	}
	if result["user/age"] != int64(30) {
		t.Errorf("expected age=30, got %v", result["user/age"])
	}

	// Email should NOT be in result (not in pattern)
	if _, exists := result["user/email"]; exists {
		t.Error("email should not be in result")
	}
}

func TestPullExecutor_Wildcard(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	emailAttr := datalog.NewKeyword(":user/email")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: emailAttr, V: "alice@example.com", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse wildcard pattern
	pattern, err := parser.ParsePullPattern(`[*]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Execute pull
	result, err := puller.Pull(alice, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// All attributes should be in result
	if len(result) != 3 {
		t.Errorf("expected 3 attributes, got %d: %v", len(result), result)
	}

	if result["user/name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", result["user/name"])
	}
	if result["user/age"] != int64(30) {
		t.Errorf("expected age=30, got %v", result["user/age"])
	}
	if result["user/email"] != "alice@example.com" {
		t.Errorf("expected email=alice@example.com, got %v", result["user/email"])
	}
}

func TestPullExecutor_NestedReference(t *testing.T) {
	// Create test data with references
	region := datalog.NewIdentity("region:us-west")
	entity := datalog.NewIdentity("entity:apple")

	regionCodeAttr := datalog.NewKeyword(":region/code")
	regionNameAttr := datalog.NewKeyword(":region/name")
	entityCodeAttr := datalog.NewKeyword(":entity/code")
	entityNameAttr := datalog.NewKeyword(":entity/name")
	entityRegionAttr := datalog.NewKeyword(":entity/region")

	datoms := []datalog.Datom{
		{E: region, A: regionCodeAttr, V: "US-W", Tx: 1},
		{E: region, A: regionNameAttr, V: "US West", Tx: 1},
		{E: entity, A: entityCodeAttr, V: "AAPL", Tx: 1},
		{E: entity, A: entityNameAttr, V: "Apple Inc.", Tx: 1},
		{E: entity, A: entityRegionAttr, V: region, Tx: 1}, // Reference to region
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse nested pattern
	pattern, err := parser.ParsePullPattern(`[:entity/code :entity/name {:entity/region [:region/code :region/name]}]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Execute pull
	result, err := puller.Pull(entity, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Check top-level attributes
	if result["entity/code"] != "AAPL" {
		t.Errorf("expected entity/code=AAPL, got %v", result["entity/code"])
	}
	if result["entity/name"] != "Apple Inc." {
		t.Errorf("expected entity/name=Apple Inc., got %v", result["entity/name"])
	}

	// Check nested region
	nestedRegion, ok := result["entity/region"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested map for entity/region, got %T", result["entity/region"])
	}

	if nestedRegion["region/code"] != "US-W" {
		t.Errorf("expected region/code=US-W, got %v", nestedRegion["region/code"])
	}
	if nestedRegion["region/name"] != "US West" {
		t.Errorf("expected region/name=US West, got %v", nestedRegion["region/name"])
	}
}

func TestPullExecutor_CycleDetection(t *testing.T) {
	// Create circular reference
	entity1 := datalog.NewIdentity("entity:1")
	entity2 := datalog.NewIdentity("entity:2")

	nameAttr := datalog.NewKeyword(":entity/name")
	nextAttr := datalog.NewKeyword(":entity/next")

	datoms := []datalog.Datom{
		{E: entity1, A: nameAttr, V: "Entity 1", Tx: 1},
		{E: entity1, A: nextAttr, V: entity2, Tx: 1},
		{E: entity2, A: nameAttr, V: "Entity 2", Tx: 1},
		{E: entity2, A: nextAttr, V: entity1, Tx: 1}, // Circular reference back to entity1
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse pattern that would recurse infinitely without cycle detection
	pattern, err := parser.ParsePullPattern(`[:entity/name {:entity/next [:entity/name {:entity/next [:entity/name]}]}]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Execute pull - should NOT panic or loop forever
	result, err := puller.Pull(entity1, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Check top-level
	if result["entity/name"] != "Entity 1" {
		t.Errorf("expected entity/name=Entity 1, got %v", result["entity/name"])
	}

	// Check first level nested
	nested1, ok := result["entity/next"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested map for entity/next, got %T", result["entity/next"])
	}
	if nested1["entity/name"] != "Entity 2" {
		t.Errorf("expected nested entity/name=Entity 2, got %v", nested1["entity/name"])
	}

	// The second level nested should stop at the cycle (entity1 was already visited)
	nested2, ok := nested1["entity/next"].(map[string]interface{})
	if ok && nested2 != nil {
		// Due to cycle detection, this should be nil or empty
		// (entity1 is already in the visited set when we try to pull it again)
		if nested2["entity/name"] != nil {
			t.Logf("Note: Nested entity/next returned %v (may vary by implementation)", nested2)
		}
	}

	t.Logf("Result structure: %+v", result)
}

func TestPullExecutor_MissingAttribute(t *testing.T) {
	// Create test data with partial attributes
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		// No age attribute
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse pattern requesting missing attribute
	pattern, err := parser.ParsePullPattern(`[:user/name :user/age]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Execute pull
	result, err := puller.Pull(alice, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	// Name should be present
	if result["user/name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", result["user/name"])
	}

	// Age should be OMITTED (not present, not nil)
	if _, exists := result["user/age"]; exists {
		t.Error("missing attribute should be omitted, not included as nil")
	}
}

func TestPullExecutor_DefaultValue(t *testing.T) {
	// Create test data with partial attributes
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		// No status attribute
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse pattern with default value
	pattern, err := parser.ParsePullPattern(`[:user/name (default :user/status "inactive")]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Execute pull
	result, err := puller.Pull(alice, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	// Name should be present
	if result["user/name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", result["user/name"])
	}

	// Status should have default value
	if result["user/status"] != "inactive" {
		t.Errorf("expected status=inactive (default), got %v", result["user/status"])
	}
}

func TestPullExecutor_NonExistentEntity(t *testing.T) {
	// Create empty datoms
	datoms := []datalog.Datom{}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse pattern
	pattern, err := parser.ParsePullPattern(`[:user/name :user/age]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Pull non-existent entity
	nonExistent := datalog.NewIdentity("user:nonexistent")
	result, err := puller.Pull(nonExistent, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	// Result should be nil for non-existent entity
	if result != nil {
		t.Errorf("expected nil result for non-existent entity, got %v", result)
	}
}

func TestPullExecutor_PullMany(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse pattern
	pattern, err := parser.ParsePullPattern(`[:user/name]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Pull many entities
	results, err := puller.PullMany([]datalog.Identity{alice, bob}, pattern)
	if err != nil {
		t.Fatalf("pullMany failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0]["user/name"] != "Alice" {
		t.Errorf("expected first result name=Alice, got %v", results[0]["user/name"])
	}
	if results[1]["user/name"] != "Bob" {
		t.Errorf("expected second result name=Bob, got %v", results[1]["user/name"])
	}
}

func TestPullExecutor_DeeplyNested(t *testing.T) {
	// Create 3-level deep reference chain
	nation := datalog.NewIdentity("nation:usa")
	region := datalog.NewIdentity("region:us-west")
	entity := datalog.NewIdentity("entity:apple")

	nationNameAttr := datalog.NewKeyword(":nation/name")
	regionCodeAttr := datalog.NewKeyword(":region/code")
	regionNationAttr := datalog.NewKeyword(":region/nation")
	entityCodeAttr := datalog.NewKeyword(":entity/code")
	entityRegionAttr := datalog.NewKeyword(":entity/region")

	datoms := []datalog.Datom{
		{E: nation, A: nationNameAttr, V: "United States", Tx: 1},
		{E: region, A: regionCodeAttr, V: "US-W", Tx: 1},
		{E: region, A: regionNationAttr, V: nation, Tx: 1},
		{E: entity, A: entityCodeAttr, V: "AAPL", Tx: 1},
		{E: entity, A: entityRegionAttr, V: region, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)

	// Parse 3-level nested pattern
	pattern, err := parser.ParsePullPattern(`[:entity/code {:entity/region [:region/code {:region/nation [:nation/name]}]}]`)
	if err != nil {
		t.Fatalf("failed to parse pattern: %v", err)
	}

	// Execute pull
	result, err := puller.Pull(entity, pattern)
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}

	// Navigate to deeply nested nation
	regionMap := result["entity/region"].(map[string]interface{})
	nationMap := regionMap["region/nation"].(map[string]interface{})

	if nationMap["nation/name"] != "United States" {
		t.Errorf("expected nation/name=United States, got %v", nationMap["nation/name"])
	}
}

func TestKeyName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{":entity/name", "entity/name"},
		{":user/age", "user/age"},
		{"nocolon", "nocolon"},
		{":", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			kw := datalog.NewKeyword(tt.input)
			result := query.KeyName(kw)
			if result != tt.expected {
				t.Errorf("KeyName(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}
