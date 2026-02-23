package storage

import (
	"sort"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestMultiSource_MemorySourceWithDatabase(t *testing.T) {
	db := createTestDB(t)

	// Add entities to the database
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")

	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":user/id"), "uid-1")
	tx.Add(bob, datalog.NewKeyword(":user/name"), "Bob")
	tx.Add(bob, datalog.NewKeyword(":user/id"), "uid-2")
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Create in-memory cache source with supplementary data
	cacheDatoms := []datalog.Datom{
		{E: datalog.NewIdentity("cache:1"), A: datalog.NewKeyword(":cache/user-id"), V: "uid-1"},
		{E: datalog.NewIdentity("cache:1"), A: datalog.NewKeyword(":cache/score"), V: int64(95)},
		{E: datalog.NewIdentity("cache:2"), A: datalog.NewKeyword(":cache/user-id"), V: "uid-2"},
		{E: datalog.NewIdentity("cache:2"), A: datalog.NewKeyword(":cache/score"), V: int64(82)},
	}
	cache := executor.NewMemoryPatternMatcher(cacheDatoms)

	// Query joining database and memory source
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?name ?score
		  :in $ $cache
		  :where [?e :user/name ?name]
		         [?e :user/id ?uid]
		         [$cache ?c :cache/user-id ?uid]
		         [$cache ?c :cache/score ?score]]`,
		WithSources(map[query.Symbol]executor.PatternMatcher{
			datalog.NewSymbol("$cache"): cache,
		}),
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Sort by name for deterministic comparison
	sort.Slice(results, func(i, j int) bool {
		return results[i][0].(string) < results[j][0].(string)
	})

	if results[0][0] != "Alice" || results[0][1] != int64(95) {
		t.Errorf("expected [Alice, 95], got %v", results[0])
	}
	if results[1][0] != "Bob" || results[1][1] != int64(82) {
		t.Errorf("expected [Bob, 82], got %v", results[1])
	}
}

func TestMultiSource_MemorySourceOnly(t *testing.T) {
	db := createTestDB(t)

	// Query only against a memory source, not using the database at all
	facts := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/name"), V: "Sword"},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword(":item/name"), V: "Shield"},
		{E: datalog.NewIdentity("e3"), A: datalog.NewKeyword(":item/name"), V: "Potion"},
	}
	items := executor.NewMemoryPatternMatcher(facts)

	results, err := db.ExecuteQueryWithInputs(
		`[:find ?name
		  :in $items
		  :where [$items ?e :item/name ?name]]`,
		WithSources(map[query.Symbol]executor.PatternMatcher{
			datalog.NewSymbol("$items"): items,
		}),
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	names := make(map[string]bool)
	for _, tuple := range results {
		names[tuple[0].(string)] = true
	}
	for _, expected := range []string{"Sword", "Shield", "Potion"} {
		if !names[expected] {
			t.Errorf("expected %s in results", expected)
		}
	}
}

func TestMultiSource_SliceSourceWithQueryBuilder(t *testing.T) {
	db := createTestDB(t)

	type Rule struct {
		Key       string
		DependsOn []string
	}

	rules := []Rule{
		{Key: "region-lore", DependsOn: []string{"world-lore", "region"}},
		{Key: "character-eval", DependsOn: []string{"character"}},
	}

	ruleSource := executor.NewSliceSource(rules, executor.AttributeSchema[Rule]{
		datalog.NewKeyword(":rule/key"):        func(r Rule) any { return r.Key },
		datalog.NewKeyword(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
	})

	// QueryBuilder-constructed query
	rs := qb.Source("$rules")
	r := qb.NewVar("r")
	key := qb.NewVar("key")

	q := qb.Query().
		Find(key).
		In(rs).
		Where(qb.PatFrom(rs, r, qb.Kw(":rule/key"), key)).
		MustBuild()

	results, err := db.ExecuteQueryWithInputs(q,
		WithSources(map[query.Symbol]executor.PatternMatcher{
			datalog.NewSymbol("$rules"): ruleSource,
		}),
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	keys := make(map[string]bool)
	for _, tuple := range results {
		keys[tuple[0].(string)] = true
	}
	if !keys["region-lore"] {
		t.Error("expected region-lore in results")
	}
	if !keys["character-eval"] {
		t.Error("expected character-eval in results")
	}
}

func TestMultiSource_SliceSourceDependencyQuery(t *testing.T) {
	db := createTestDB(t)

	type Rule struct {
		Key       string
		DependsOn []string
	}

	rules := []Rule{
		{Key: "region-lore", DependsOn: []string{"world-lore", "region"}},
		{Key: "character-eval", DependsOn: []string{"character"}},
	}

	ruleSource := executor.NewSliceSource(rules, executor.AttributeSchema[Rule]{
		datalog.NewKeyword(":rule/key"):        func(r Rule) any { return r.Key },
		datalog.NewKeyword(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
	})

	// Query dependencies for a specific rule
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?dep
		  :in $rules ?key
		  :where [$rules ?r :rule/key ?key]
		         [$rules ?r :rule/depends-on ?dep]]`,
		WithSources(map[query.Symbol]executor.PatternMatcher{
			datalog.NewSymbol("$rules"): ruleSource,
		}),
		"region-lore",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	deps := make(map[string]bool)
	for _, tuple := range results {
		deps[tuple[0].(string)] = true
	}
	if !deps["world-lore"] {
		t.Error("expected world-lore dependency")
	}
	if !deps["region"] {
		t.Error("expected region dependency")
	}
}

func TestMultiSource_SourceValidation(t *testing.T) {
	db := createTestDB(t)

	// Query declares $users in :in but doesn't provide it
	_, err := db.ExecuteQueryWithInputs(
		`[:find ?e :in $users :where [$users ?e :attr ?v]]`,
	)
	if err == nil {
		t.Fatal("expected error for missing source, got nil")
	}
}

func TestMultiSource_DefaultSourceAlwaysAvailable(t *testing.T) {
	db := createTestDB(t)

	// Add data to the default database
	entity := datalog.NewIdentity("thing:1")
	tx := db.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":thing/name"), "Widget")
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Standard query without any :in clause - default $ source
	results, err := db.ExecuteQuery(`[:find ?name :where [?e :thing/name ?name]]`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0][0] != "Widget" {
		t.Errorf("expected Widget, got %v", results[0][0])
	}
}

func TestMultiSource_CrossDatabaseJoin(t *testing.T) {
	usersDB := createTestDB(t)
	permsDB := createTestDB(t)

	// Populate users database
	alice := datalog.NewIdentity("user:alice")
	tx := usersDB.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":user/id"), "uid-1")
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit users: %v", err)
	}

	// Populate permissions database
	perm := datalog.NewIdentity("perm:1")
	tx2 := permsDB.NewTransaction()
	tx2.Add(perm, datalog.NewKeyword(":perm/user-id"), "uid-1")
	tx2.Add(perm, datalog.NewKeyword(":perm/role"), "admin")
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit perms: %v", err)
	}

	// Cross-database join: find user names and their roles
	results, err := usersDB.ExecuteQueryWithInputs(
		`[:find ?name ?role
		  :in $users $perms
		  :where [$users ?u :user/name ?name]
		         [$users ?u :user/id ?uid]
		         [$perms ?p :perm/user-id ?uid]
		         [$perms ?p :perm/role ?role]]`,
		WithSources(map[query.Symbol]executor.PatternMatcher{
			datalog.NewSymbol("$users"): usersDB,
			datalog.NewSymbol("$perms"): permsDB,
		}),
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0][0] != "Alice" {
		t.Errorf("expected Alice, got %v", results[0][0])
	}
	if results[0][1] != "admin" {
		t.Errorf("expected admin, got %v", results[0][1])
	}
}
