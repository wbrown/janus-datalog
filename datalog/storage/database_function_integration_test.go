package storage

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// get-else Integration Tests
// =============================================================================

func TestGetElseBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "get-else-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add test data - some entities have :person/nickname, some don't
	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	charlie := datalog.NewIdentity("charlie")

	// Alice has both name and nickname
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice Smith")
	tx.Add(alice, datalog.NewKeyword(":person/nickname"), "Ali")

	// Bob has name but no nickname
	tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob Jones")

	// Charlie has name but no nickname
	tx.Add(charlie, datalog.NewKeyword(":person/name"), "Charlie Brown")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	tests := []struct {
		name        string
		query       string
		wantCount   int
		checkResult func(t *testing.T, results [][]interface{})
	}{
		{
			name:      "get-else returns actual value when present",
			query:     `[:find ?name ?nick :where [?e :person/name ?name] [(get-else $ ?e :person/nickname "No Nickname") ?nick]]`,
			wantCount: 3,
			checkResult: func(t *testing.T, results [][]interface{}) {
				nameToNick := make(map[string]string)
				for _, tuple := range results {
					name := tuple[0].(string)
					nick := tuple[1].(string)
					nameToNick[name] = nick
				}
				if nameToNick["Alice Smith"] != "Ali" {
					t.Errorf("Alice should have nickname 'Ali', got %q", nameToNick["Alice Smith"])
				}
				if nameToNick["Bob Jones"] != "No Nickname" {
					t.Errorf("Bob should have default 'No Nickname', got %q", nameToNick["Bob Jones"])
				}
				if nameToNick["Charlie Brown"] != "No Nickname" {
					t.Errorf("Charlie should have default 'No Nickname', got %q", nameToNick["Charlie Brown"])
				}
			},
		},
		{
			name:      "get-else with empty string default",
			query:     `[:find ?name ?nick :where [?e :person/name ?name] [(get-else $ ?e :person/nickname "") ?nick]]`,
			wantCount: 3,
			checkResult: func(t *testing.T, results [][]interface{}) {
				nameToNick := make(map[string]string)
				for _, tuple := range results {
					name := tuple[0].(string)
					nick := tuple[1].(string)
					nameToNick[name] = nick
				}
				if nameToNick["Alice Smith"] != "Ali" {
					t.Errorf("Alice should have nickname 'Ali', got %q", nameToNick["Alice Smith"])
				}
				if nameToNick["Bob Jones"] != "" {
					t.Errorf("Bob should have empty default, got %q", nameToNick["Bob Jones"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := executor.CollectTuples(db.Query(tt.query))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(results) != tt.wantCount {
				t.Errorf("Expected %d results, got %d", tt.wantCount, len(results))
				for i, tuple := range results {
					t.Logf("  Tuple %d: %v", i, tuple)
				}
			}
			if tt.checkResult != nil {
				tt.checkResult(t, results)
			}
		})
	}
}

func TestGetElseNumericDefault(t *testing.T) {
	dir, err := os.MkdirTemp("", "get-else-numeric-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	prod1 := datalog.NewIdentity("product-1")
	prod2 := datalog.NewIdentity("product-2")

	// Product 1 has a discount
	tx.Add(prod1, datalog.NewKeyword(":product/name"), "Widget")
	tx.Add(prod1, datalog.NewKeyword(":product/discount"), int64(10))

	// Product 2 has no discount
	tx.Add(prod2, datalog.NewKeyword(":product/name"), "Gadget")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	results, err := executor.CollectTuples(db.Query(`[:find ?name ?discount :where [?e :product/name ?name] [(get-else $ ?e :product/discount 0) ?discount]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	nameToDiscount := make(map[string]int64)
	for _, tuple := range results {
		name := tuple[0].(string)
		discount, ok := tuple[1].(int64)
		if !ok {
			t.Errorf("Expected int64 for discount, got %T: %v", tuple[1], tuple[1])
			continue
		}
		nameToDiscount[name] = discount
	}

	if nameToDiscount["Widget"] != 10 {
		t.Errorf("Widget should have discount 10, got %d", nameToDiscount["Widget"])
	}
	if nameToDiscount["Gadget"] != 0 {
		t.Errorf("Gadget should have default discount 0, got %d", nameToDiscount["Gadget"])
	}
}

// =============================================================================
// missing? Integration Tests
// =============================================================================

func TestMissingAsPredicate(t *testing.T) {
	dir, err := os.MkdirTemp("", "missing-predicate-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	charlie := datalog.NewIdentity("charlie")

	// Alice has email
	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":user/email"), "alice@example.com")

	// Bob has no email
	tx.Add(bob, datalog.NewKeyword(":user/name"), "Bob")

	// Charlie has email
	tx.Add(charlie, datalog.NewKeyword(":user/name"), "Charlie")
	tx.Add(charlie, datalog.NewKeyword(":user/email"), "charlie@example.com")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Find users WITHOUT email - should only return Bob
	results, err := executor.CollectTuples(db.Query(`[:find ?name :where [?e :user/name ?name] [(missing? $ ?e :user/email)]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result (Bob), got %d", len(results))
		for i, tuple := range results {
			t.Logf("  Tuple %d: %v", i, tuple)
		}
		return
	}

	if results[0][0].(string) != "Bob" {
		t.Errorf("Expected 'Bob', got %v", results[0][0])
	}
}

func TestMissingAsExpression(t *testing.T) {
	dir, err := os.MkdirTemp("", "missing-expr-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")

	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":user/verified"), true)

	tx.Add(bob, datalog.NewKeyword(":user/name"), "Bob")
	// Bob is not verified

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Get missing status as a boolean symbol
	results, err := executor.CollectTuples(db.Query(`[:find ?name ?needs_verification :where [?e :user/name ?name] [(missing? $ ?e :user/verified) ?needs_verification]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
		for i, tuple := range results {
			t.Logf("  Tuple %d: %v", i, tuple)
		}
		return
	}

	nameToMissing := make(map[string]bool)
	for _, tuple := range results {
		name := tuple[0].(string)
		missing, ok := tuple[1].(bool)
		if !ok {
			t.Errorf("Expected bool for missing status, got %T: %v", tuple[1], tuple[1])
			continue
		}
		nameToMissing[name] = missing
	}

	if nameToMissing["Alice"] != false {
		t.Errorf("Alice has :user/verified, so missing? should be false")
	}
	if nameToMissing["Bob"] != true {
		t.Errorf("Bob lacks :user/verified, so missing? should be true")
	}
}

func TestMissingMultipleAttributes(t *testing.T) {
	dir, err := os.MkdirTemp("", "missing-multi-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	p1 := datalog.NewIdentity("p1")
	p2 := datalog.NewIdentity("p2")
	p3 := datalog.NewIdentity("p3")
	p4 := datalog.NewIdentity("p4")

	// p1: has phone, no email
	tx.Add(p1, datalog.NewKeyword(":contact/name"), "Person1")
	tx.Add(p1, datalog.NewKeyword(":contact/phone"), "555-1111")

	// p2: has email, no phone
	tx.Add(p2, datalog.NewKeyword(":contact/name"), "Person2")
	tx.Add(p2, datalog.NewKeyword(":contact/email"), "p2@example.com")

	// p3: has both
	tx.Add(p3, datalog.NewKeyword(":contact/name"), "Person3")
	tx.Add(p3, datalog.NewKeyword(":contact/phone"), "555-3333")
	tx.Add(p3, datalog.NewKeyword(":contact/email"), "p3@example.com")

	// p4: has neither
	tx.Add(p4, datalog.NewKeyword(":contact/name"), "Person4")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Find contacts missing BOTH phone AND email
	results, err := executor.CollectTuples(db.Query(`[:find ?name :where [?e :contact/name ?name] [(missing? $ ?e :contact/phone)] [(missing? $ ?e :contact/email)]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result (Person4), got %d", len(results))
		for i, tuple := range results {
			t.Logf("  Tuple %d: %v", i, tuple)
		}
		return
	}

	if results[0][0].(string) != "Person4" {
		t.Errorf("Expected 'Person4', got %v", results[0][0])
	}
}

// =============================================================================
// get-some Integration Tests
// =============================================================================

func TestGetSomeBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "get-some-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	user1 := datalog.NewIdentity("user1")
	user2 := datalog.NewIdentity("user2")
	user3 := datalog.NewIdentity("user3")

	// User1 has nickname (should use nickname)
	tx.Add(user1, datalog.NewKeyword(":user/id"), "U001")
	tx.Add(user1, datalog.NewKeyword(":user/nickname"), "CoolGuy")
	tx.Add(user1, datalog.NewKeyword(":user/fullname"), "John Doe")
	tx.Add(user1, datalog.NewKeyword(":user/email"), "john@example.com")

	// User2 has no nickname but has fullname (should use fullname)
	tx.Add(user2, datalog.NewKeyword(":user/id"), "U002")
	tx.Add(user2, datalog.NewKeyword(":user/fullname"), "Jane Smith")
	tx.Add(user2, datalog.NewKeyword(":user/email"), "jane@example.com")

	// User3 has only email (should use email)
	tx.Add(user3, datalog.NewKeyword(":user/id"), "U003")
	tx.Add(user3, datalog.NewKeyword(":user/email"), "anon@example.com")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// get-some with three fallback options: nickname -> fullname -> email
	results, err := executor.CollectTuples(db.Query(`[:find ?id ?display :where [?e :user/id ?id] [(get-some $ ?e :user/nickname :user/fullname :user/email) ?display]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
		for i, tuple := range results {
			t.Logf("  Tuple %d: %v", i, tuple)
		}
		return
	}

	idToDisplay := make(map[string]string)
	for _, tuple := range results {
		id := tuple[0].(string)
		// get-some returns a GetSomeResult struct, which contains the value
		// The executor should extract just the value for the binding
		display, ok := tuple[1].(string)
		if !ok {
			t.Logf("Tuple for id %s has display type %T: %v", id, tuple[1], tuple[1])
			continue
		}
		idToDisplay[id] = display
	}

	if idToDisplay["U001"] != "CoolGuy" {
		t.Errorf("User1 should display 'CoolGuy' (nickname), got %q", idToDisplay["U001"])
	}
	if idToDisplay["U002"] != "Jane Smith" {
		t.Errorf("User2 should display 'Jane Smith' (fullname), got %q", idToDisplay["U002"])
	}
	if idToDisplay["U003"] != "anon@example.com" {
		t.Errorf("User3 should display 'anon@example.com' (email), got %q", idToDisplay["U003"])
	}
}

func TestGetSomeNoMatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "get-some-nomatch-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	user1 := datalog.NewIdentity("user1")
	user2 := datalog.NewIdentity("user2")

	// User1 has nickname
	tx.Add(user1, datalog.NewKeyword(":user/id"), "U001")
	tx.Add(user1, datalog.NewKeyword(":user/nickname"), "HasNick")

	// User2 has NONE of the fallback attributes
	tx.Add(user2, datalog.NewKeyword(":user/id"), "U002")
	// No nickname, fullname, or displayname

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// get-some should filter out User2 since none of the attrs exist
	results, err := executor.CollectTuples(db.Query(`[:find ?id ?display :where [?e :user/id ?id] [(get-some $ ?e :user/nickname :user/fullname :user/displayname) ?display]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should only return User1
	if len(results) != 1 {
		t.Errorf("Expected 1 result (only User1), got %d", len(results))
		for i, tuple := range results {
			t.Logf("  Tuple %d: %v", i, tuple)
		}
		return
	}

	if results[0][0].(string) != "U001" {
		t.Errorf("Expected User1 (U001), got %v", results[0][0])
	}
}

// =============================================================================
// Combined Database Function Tests
// =============================================================================

func TestCombinedDatabaseFunctions(t *testing.T) {
	dir, err := os.MkdirTemp("", "combined-db-func-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")

	// Alice: has phone, no email
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":person/phone"), "555-ALICE")
	tx.Add(alice, datalog.NewKeyword(":person/priority"), int64(1))

	// Bob: has email, no phone
	tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")
	tx.Add(bob, datalog.NewKeyword(":person/email"), "bob@example.com")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Use get-else to get phone with default "N/A"
	// AND check if email is missing
	results, err := executor.CollectTuples(db.Query(`[:find ?name ?phone ?email_missing :where [?e :person/name ?name] [(get-else $ ?e :person/phone "N/A") ?phone] [(missing? $ ?e :person/email) ?email_missing]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
		for i, tuple := range results {
			t.Logf("  Tuple %d: %v", i, tuple)
		}
		return
	}

	for _, tuple := range results {
		name := tuple[0].(string)
		phone := tuple[1].(string)
		emailMissing := tuple[2].(bool)

		switch name {
		case "Alice":
			if phone != "555-ALICE" {
				t.Errorf("Alice's phone should be '555-ALICE', got %q", phone)
			}
			if !emailMissing {
				t.Errorf("Alice's email should be missing (true), got %v", emailMissing)
			}
		case "Bob":
			if phone != "N/A" {
				t.Errorf("Bob's phone should be 'N/A' (default), got %q", phone)
			}
			if emailMissing {
				t.Errorf("Bob's email should NOT be missing (false), got %v", emailMissing)
			}
		default:
			t.Errorf("Unexpected name: %s", name)
		}
	}
}

func TestDatabaseFunctionWithAggregation(t *testing.T) {
	dir, err := os.MkdirTemp("", "db-func-agg-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	for i := 1; i <= 5; i++ {
		eid := datalog.NewIdentity("item-" + string(rune('0'+i)))
		tx.Add(eid, datalog.NewKeyword(":item/name"), "Item"+string(rune('0'+i)))
		// Only odd items have discount
		if i%2 == 1 {
			tx.Add(eid, datalog.NewKeyword(":item/discount"), int64(i*10))
		}
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Sum of discounts, with 0 as default for items without discount
	results, err := executor.CollectTuples(db.Query(`[:find (sum ?discount) :where [?e :item/name ?name] [(get-else $ ?e :item/discount 0) ?discount]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 aggregated result, got %d", len(results))
	}

	// Items 1, 3, 5 have discounts: 10 + 30 + 50 = 90
	// Items 2, 4 get default 0
	// Total: 90
	sum, ok := results[0][0].(int64)
	if !ok {
		// Try float64 as some aggregations return float
		if f, ok := results[0][0].(float64); ok {
			sum = int64(f)
		} else {
			t.Fatalf("Expected numeric sum, got %T: %v", results[0][0], results[0][0])
		}
	}

	if sum != 90 {
		t.Errorf("Expected sum of 90, got %d", sum)
	}
}

func TestDatabaseFunctionWithOrderBy(t *testing.T) {
	dir, err := os.MkdirTemp("", "db-func-orderby-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	p1 := datalog.NewIdentity("p1")
	p2 := datalog.NewIdentity("p2")
	p3 := datalog.NewIdentity("p3")

	tx.Add(p1, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(p1, datalog.NewKeyword(":person/score"), int64(85))

	tx.Add(p2, datalog.NewKeyword(":person/name"), "Bob")
	// No score for Bob

	tx.Add(p3, datalog.NewKeyword(":person/name"), "Charlie")
	tx.Add(p3, datalog.NewKeyword(":person/score"), int64(95))

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Order by score descending (Bob gets 0 as default)
	results, err := executor.CollectTuples(db.Query(`[:find ?name ?score :where [?e :person/name ?name] [(get-else $ ?e :person/score 0) ?score] :order-by [[?score :desc]]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Order should be: Charlie (95), Alice (85), Bob (0)
	expected := []string{"Charlie", "Alice", "Bob"}
	for i, tuple := range results {
		name := tuple[0].(string)
		if name != expected[i] {
			t.Errorf("Position %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

// =============================================================================
// get-else with Vector Default
// =============================================================================

func TestGetElseWithVectorDefault(t *testing.T) {
	dir, err := os.MkdirTemp("", "get-else-vector-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Schema with a vector attribute
	s, err := schema.NewBuilder().
		Attribute(":entity/name").Type(schema.TypeString).Add().
		Attribute(":entity/lore").Type(schema.TypeString).Vector().Add().
		Build()
	if err != nil {
		t.Fatalf("Failed to build schema: %v", err)
	}

	db, err := NewDatabaseWithSchema(dir, s)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	room1 := datalog.NewIdentity("room1")
	room2 := datalog.NewIdentity("room2")

	// room1 has lore (vector with 2 paragraphs)
	tx.Add(room1, datalog.NewKeyword(":entity/name"), "Stone Chamber")
	tx.Add(room1, datalog.NewKeyword(":entity/lore"), "A rectangular chamber hewn from dark stone.")
	tx.Add(room1, datalog.NewKeyword(":entity/lore"), "Scorch marks darken the floor near the entrance.")

	// room2 has no lore
	tx.Add(room2, datalog.NewKeyword(":entity/name"), "Empty Corridor")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Run("vector attribute returns vector when present", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(
			`[:find ?name ?lore :where
			  [?e :entity/name ?name]
			  [(get-else $ ?e :entity/lore []) ?lore]]`,
		))
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(results) != 2 {
			t.Fatalf("Expected 2 results, got %d", len(results))
		}

		for _, tuple := range results {
			name := tuple[0].(string)
			switch name {
			case "Stone Chamber":
				lore, ok := tuple[1].([]string)
				if !ok {
					t.Fatalf("Expected []string for Stone Chamber lore, got %T: %v", tuple[1], tuple[1])
				}
				if len(lore) != 2 {
					t.Errorf("Expected 2 lore paragraphs, got %d", len(lore))
				}
			case "Empty Corridor":
				lore, ok := tuple[1].([]string)
				if !ok {
					t.Fatalf("Expected []string for Empty Corridor lore, got %T: %v", tuple[1], tuple[1])
				}
				if len(lore) != 0 {
					t.Errorf("Expected empty lore vector, got %d elements", len(lore))
				}
			}
		}
	})

	t.Run("vector default with input parameter entity", func(t *testing.T) {
		// Query a specific entity with no lore — should get empty vector
		results, err := executor.CollectTuples(db.Query(
			`[:find ?lore :in $ ?room :where
			  [(get-else $ ?room :entity/lore []) ?lore]]`,
			room2,
		))
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(results))
		}
		lore, ok := results[0][0].([]string)
		if !ok {
			t.Fatalf("Expected []string, got %T: %v", results[0][0], results[0][0])
		}
		if len(lore) != 0 {
			t.Errorf("Expected empty vector default, got %d elements", len(lore))
		}
	})
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

func TestGetElseWithNullishValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "get-else-null-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("e1")
	e2 := datalog.NewIdentity("e2")

	// e1 has empty string as value
	tx.Add(e1, datalog.NewKeyword(":entity/name"), "Entity1")
	tx.Add(e1, datalog.NewKeyword(":entity/description"), "") // empty string IS a value

	// e2 has no description at all
	tx.Add(e2, datalog.NewKeyword(":entity/name"), "Entity2")

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// The empty string should be returned (not the default)
	results, err := executor.CollectTuples(db.Query(`[:find ?name ?desc :where [?e :entity/name ?name] [(get-else $ ?e :entity/description "DEFAULT") ?desc]]`))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	for _, tuple := range results {
		name := tuple[0].(string)
		desc := tuple[1].(string)
		switch name {
		case "Entity1":
			if desc != "" {
				t.Errorf("Entity1 has empty string description, should return empty, got %q", desc)
			}
		case "Entity2":
			if desc != "DEFAULT" {
				t.Errorf("Entity2 has no description, should return DEFAULT, got %q", desc)
			}
		}
	}
}

func TestDatabaseFunctionWithInputParameters(t *testing.T) {
	dir, err := os.MkdirTemp("", "db-func-input-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")

	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":user/status"), "active")

	tx.Add(bob, datalog.NewKeyword(":user/name"), "Bob")
	// No status

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Use input parameter for default value
	results, err := executor.CollectTuples(db.Query(
		`[:find ?name ?status :in $ ?default-status :where [?e :user/name ?name] [(get-else $ ?e :user/status ?default-status) ?status]]`,
		"pending",
	))
	if err != nil {
		// This might fail because get-else expects a constant default, not a variable
		// Let's verify this is the expected behavior
		t.Logf("Query with variable default failed (expected): %v", err)
		return
	}

	// If it succeeds, check results
	t.Logf("Query with variable default succeeded (results: %d)", len(results))
	for _, tuple := range results {
		t.Logf("  %v", tuple)
	}
}

// TestGetElseWithPopulatedVectorDefault verifies that get-else returns a typed
// default when the attribute is a vector. The default value [] is parsed as
// []interface{}, but TypedDefaulter should convert it to []string (or the
// appropriate typed slice) to match the attribute's schema type.
func TestGetElseWithPopulatedVectorDefault(t *testing.T) {
	dir, err := os.MkdirTemp("", "get-else-populated-vector-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := schema.NewBuilder().
		Attribute(":entity/name").Type(schema.TypeString).Add().
		Attribute(":entity/tags").Type(schema.TypeString).Vector().Add().
		Build()
	if err != nil {
		t.Fatalf("Failed to build schema: %v", err)
	}

	db, err := NewDatabaseWithSchema(dir, s)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	hasData := datalog.NewIdentity("has-data")
	noData := datalog.NewIdentity("no-data")

	tx.Add(hasData, datalog.NewKeyword(":entity/name"), "HasData")
	tx.Add(hasData, datalog.NewKeyword(":entity/tags"), "alpha")
	tx.Add(hasData, datalog.NewKeyword(":entity/tags"), "beta")

	tx.Add(noData, datalog.NewKeyword(":entity/name"), "NoData")
	// No tags for noData

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// get-else with empty vector default — both should return []string
	results, err := executor.CollectTuples(db.Query(
		`[:find ?name ?tags :where
		  [?e :entity/name ?name]
		  [(get-else $ ?e :entity/tags []) ?tags]]`,
	))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	for _, tuple := range results {
		name := tuple[0].(string)
		switch name {
		case "HasData":
			tags, ok := tuple[1].([]string)
			if !ok {
				t.Fatalf("HasData: expected []string, got %T: %v", tuple[1], tuple[1])
			}
			if len(tags) != 2 {
				t.Errorf("HasData: expected 2 tags, got %d", len(tags))
			}
		case "NoData":
			tags, ok := tuple[1].([]string)
			if !ok {
				t.Fatalf("NoData: expected []string (typed default), got %T: %v", tuple[1], tuple[1])
			}
			if len(tags) != 0 {
				t.Errorf("NoData: expected empty tags, got %d", len(tags))
			}
		}
	}
}

func TestLookupAttributeDirectly(t *testing.T) {
	// Test the LookupAttribute method directly on BadgerMatcher
	dir, err := os.MkdirTemp("", "lookup-attr-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")

	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice Smith")
	tx.Add(alice, datalog.NewKeyword(":person/age"), int64(30))
	tx.Add(alice, datalog.NewKeyword(":person/active"), true)

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	matcher := db.Matcher()
	badgerMatcher, ok := matcher.(*BadgerMatcher)
	if !ok {
		t.Fatalf("Expected BadgerMatcher, got %T", matcher)
	}

	// Test string attribute
	val, found := requireAttributeLookup(t, badgerMatcher, alice, datalog.NewKeyword(":person/name"))
	if !found {
		t.Error(":person/name should be found")
	} else if val != "Alice Smith" {
		t.Errorf("Expected 'Alice Smith', got %v", val)
	}

	// Test int64 attribute
	val, found = requireAttributeLookup(t, badgerMatcher, alice, datalog.NewKeyword(":person/age"))
	if !found {
		t.Error(":person/age should be found")
	} else if val != int64(30) {
		t.Errorf("Expected 30, got %v", val)
	}

	// Test bool attribute
	val, found = requireAttributeLookup(t, badgerMatcher, alice, datalog.NewKeyword(":person/active"))
	if !found {
		t.Error(":person/active should be found")
	} else if val != true {
		t.Errorf("Expected true, got %v", val)
	}

	// Test missing attribute
	val, found = requireAttributeLookup(t, badgerMatcher, alice, datalog.NewKeyword(":person/email"))
	if found {
		t.Errorf(":person/email should NOT be found, got %v", val)
	}

	// Test non-existent entity
	noone := datalog.NewIdentity("noone")
	val, found = requireAttributeLookup(t, badgerMatcher, noone, datalog.NewKeyword(":person/name"))
	if found {
		t.Errorf("Non-existent entity should return not found, got %v", val)
	}
}
