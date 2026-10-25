package planner

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPlanCache(t *testing.T) {
	cache := NewPlanCache(10, 1*time.Minute)
	opts := PlannerOptions{}

	// Create a sample query
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?e")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Constant{Value: "Alice"},
				},
			},
		},
	}

	// Create a sample plan
	plan := &RealizedPlan{
		Query: q,
		Phases: []RealizedPhase{
			{
				Query:    q,
				Provides: []query.Symbol{datalog.NewSymbol("?e")},
			},
		},
	}

	// Test miss
	cached, ok := cache.GetWithOptions(q, query.Bindings{}, opts)
	if ok {
		t.Error("Expected cache miss, got hit")
	}
	if cached != nil {
		t.Error("Expected nil plan on cache miss")
	}

	// Store the plan
	cache.SetWithOptions(q, query.Bindings{}, plan, opts)

	// Test hit
	cached, ok = cache.GetWithOptions(q, query.Bindings{}, opts)
	if !ok {
		t.Error("Expected cache hit, got miss")
	}
	if cached != plan {
		t.Error("Expected to get the same plan back")
	}

	// Test stats
	hits, misses, size := cache.Stats()
	if hits != 1 {
		t.Errorf("Expected 1 hit, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}
	if size != 1 {
		t.Errorf("Expected cache size 1, got %d", size)
	}

	// Test clear
	cache.Clear()

	// Stats should be reset after clear
	hits, misses, size = cache.Stats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Errorf("Expected stats to be reset after clear, got hits=%d, misses=%d, size=%d", hits, misses, size)
	}

	// Getting after clear should miss
	_, ok = cache.GetWithOptions(q, query.Bindings{}, opts)
	if ok {
		t.Error("Expected cache miss after clear")
	}
}

// A plan is served under the invocations held when it was made: a scope
// planned under the rewrite's reading of its order, with every invocation
// held and no body planned, never serves the same text planned as a body a
// fixpoint runs or as a query, whose invocations are derived at their
// clauses with their bodies planned.
func TestPlanCache_APlanIsServedUnderTheHeldItWasMadeUnder(t *testing.T) {
	cache := NewPlanCache(10, 0)
	opts := PlannerOptions{}
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?e")}},
		Where: []query.Clause{&query.RuleInvocation{
			Name: datalog.NewSymbol("reaches"),
			Args: []query.PatternElement{query.Variable{Name: datalog.NewSymbol("?e")}},
		}},
	}
	plan := &RealizedPlan{Query: q}
	cache.SetWithOptions(q, query.Bindings{Held: query.HeldEvery}, plan, opts)
	if _, ok := cache.GetWithOptions(q, query.Bindings{Held: query.HeldByFixpoint}, opts); ok {
		t.Error("a plan made with every invocation held is served to a body a fixpoint runs")
	}
	if _, ok := cache.GetWithOptions(q, query.Bindings{Held: query.HeldNone}, opts); ok {
		t.Error("a plan made with every invocation held is served to a query")
	}
	cached, ok := cache.GetWithOptions(q, query.Bindings{Held: query.HeldEvery}, opts)
	if !ok || cached != plan {
		t.Error("a plan is served under the held it was made under")
	}
}

func TestPlanCacheEviction(t *testing.T) {
	cache := NewPlanCache(2, 1*time.Hour) // Small cache size
	opts := PlannerOptions{}

	// Create 3 different queries
	queries := make([]*query.Query, 3)
	plans := make([]*RealizedPlan, 3)

	for i := 0; i < 3; i++ {
		queries[i] = &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: datalog.NewSymbol("?e")},
			},
			Where: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":person/id")},
						query.Constant{Value: int64(i)},
					},
				},
			},
		}

		plans[i] = &RealizedPlan{
			Query: queries[i],
			Phases: []RealizedPhase{
				{
					Query:    queries[i],
					Provides: []query.Symbol{datalog.NewSymbol("?e")},
				},
			},
		}
	}

	// Add first two queries
	cache.SetWithOptions(queries[0], query.Bindings{}, plans[0], opts)
	time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	cache.SetWithOptions(queries[1], query.Bindings{}, plans[1], opts)

	// Both should be in cache
	if _, ok := cache.GetWithOptions(queries[0], query.Bindings{}, opts); !ok {
		t.Error("Expected first query to be cached")
	}
	if _, ok := cache.GetWithOptions(queries[1], query.Bindings{}, opts); !ok {
		t.Error("Expected second query to be cached")
	}

	// Add third query, should evict oldest (first)
	cache.SetWithOptions(queries[2], query.Bindings{}, plans[2], opts)

	// First should be evicted
	if _, ok := cache.GetWithOptions(queries[0], query.Bindings{}, opts); ok {
		t.Error("Expected first query to be evicted")
	}

	// Second and third should still be cached
	if _, ok := cache.GetWithOptions(queries[1], query.Bindings{}, opts); !ok {
		t.Error("Expected second query to still be cached")
	}
	if _, ok := cache.GetWithOptions(queries[2], query.Bindings{}, opts); !ok {
		t.Error("Expected third query to be cached")
	}
}

func TestPlanCacheTTL(t *testing.T) {
	cache := NewPlanCache(10, 50*time.Millisecond) // Short TTL for testing
	opts := PlannerOptions{}

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?e")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Constant{Value: "Bob"},
				},
			},
		},
	}

	plan := &RealizedPlan{
		Query: q,
		Phases: []RealizedPhase{
			{
				Query:    q,
				Provides: []query.Symbol{datalog.NewSymbol("?e")},
			},
		},
	}

	// Set the plan
	cache.SetWithOptions(q, query.Bindings{}, plan, opts)

	// Should be in cache immediately
	if _, ok := cache.GetWithOptions(q, query.Bindings{}, opts); !ok {
		t.Error("Expected query to be cached")
	}

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Should be expired now
	if _, ok := cache.GetWithOptions(q, query.Bindings{}, opts); ok {
		t.Error("Expected query to be expired")
	}
}

func TestPlanCacheSeparatesJoinProjectInsertionOption(t *testing.T) {
	cache := NewPlanCache(10, 0)
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?e")},
		},
	}
	plan := &RealizedPlan{Query: q}
	disabled := PlannerOptions{EnableAlgebraOptimizer: true}
	enabled := disabled
	enabled.EnableJoinProjectInsertion = true

	cache.SetWithOptions(q, query.Bindings{}, plan, disabled)
	if _, ok := cache.GetWithOptions(q, query.Bindings{}, enabled); ok {
		t.Fatal("join-project insertion option must produce a distinct plan cache key")
	}
}

// A plan made under CollectRosters carries a roster and one made without
// carries none, so the two are two entries of one text.
func TestPlanCacheSeparatesCollectRostersOption(t *testing.T) {
	cache := NewPlanCache(10, 0)
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?e")},
		},
	}
	plan := &RealizedPlan{Query: q}
	without := PlannerOptions{EnableAlgebraOptimizer: true}
	with := without
	with.CollectRosters = true

	cache.SetWithOptions(q, query.Bindings{}, plan, without)
	if _, ok := cache.GetWithOptions(q, query.Bindings{}, with); ok {
		t.Fatal("a plan made without the roster option is not served to a request under it")
	}
}

// The symbols bound when a plan is made are part of its key: a plan stored
// under a bound symbol is not served to a request under none and is served
// under the same symbol, a plan stored under none is a second entry of the
// text, and the key is a function of the bound set, the same however the
// set's map iterates.
func TestPlanCacheKeyDistinguishesBoundSymbols(t *testing.T) {
	cache := NewPlanCache(10, 0)
	opts := PlannerOptions{}
	e := datalog.NewSymbol("?e")
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?n")}},
		In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.ScalarInput{Symbol: e}},
		Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: e},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: datalog.NewSymbol("?n")},
		}}},
	}
	plan := &RealizedPlan{Query: q}
	bound := query.Bindings{Symbols: map[query.Symbol]bool{e: true}}

	cache.SetWithOptions(q, bound, plan, opts)
	if _, ok := cache.GetWithOptions(q, query.Bindings{}, opts); ok {
		t.Fatal("a plan made under a bound symbol is not served to a request under none")
	}
	if _, ok := cache.GetWithOptions(q, bound, opts); !ok {
		t.Fatal("a plan made under a bound symbol is served under the same symbol")
	}
	cache.SetWithOptions(q, query.Bindings{}, plan, opts)
	if _, _, size := cache.Stats(); size != 2 {
		t.Fatalf("size = %d after a plan under none beside one under the bound symbol; want 2", size)
	}

	three := query.Bindings{Symbols: map[query.Symbol]bool{
		e: true, datalog.NewSymbol("?n"): true, datalog.NewSymbol("?m"): true,
	}}
	key := cache.computeKeyWithOptions(q, three, opts)
	for i := 0; i < 20; i++ {
		if again := cache.computeKeyWithOptions(q, three, opts); again != key {
			t.Fatalf("the key under three bound symbols differs between two computations: the key depends on the map's iteration order")
		}
	}
}

// A conditional aggregate and the plain aggregate over one text are two
// plans: the find's predicate variable decides what the plan computes, so a
// plan made for the plain aggregate is not served to the conditional one, and
// the conditional one is served under its own find.
func TestPlanCacheDistinguishesAConditionalAggregate(t *testing.T) {
	cache := NewPlanCache(10, 0)
	opts := PlannerOptions{}
	v, p := datalog.NewSymbol("?v"), datalog.NewSymbol("?p")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: datalog.NewKeyword(":n")},
		query.Variable{Name: v},
	}}
	plain := &query.Query{Find: []query.FindElement{query.FindAggregate{Function: datalog.SymMax, Arg: v}}, Where: []query.Clause{pattern}}
	conditional := &query.Query{Find: []query.FindElement{query.FindAggregate{Function: datalog.SymMax, Arg: v, Predicate: p}}, Where: []query.Clause{pattern}}
	plainPlan, conditionalPlan := &RealizedPlan{Query: plain}, &RealizedPlan{Query: conditional}

	cache.SetWithOptions(plain, query.Bindings{}, plainPlan, opts)
	if _, ok := cache.GetWithOptions(conditional, query.Bindings{}, opts); ok {
		t.Fatal("a plan made for the plain aggregate is served to the conditional aggregate over the same text")
	}
	cache.SetWithOptions(conditional, query.Bindings{}, conditionalPlan, opts)
	if cached, ok := cache.GetWithOptions(conditional, query.Bindings{}, opts); !ok || cached != conditionalPlan {
		t.Fatal("the conditional aggregate's plan is served under its own find")
	}
	if cached, ok := cache.GetWithOptions(plain, query.Bindings{}, opts); !ok || cached != plainPlan {
		t.Fatal("the plain aggregate's plan is served under its own find beside the conditional one's")
	}
}

// A cached plan was made under a request when the request's query is
// structurally the plan's, its rule set is structurally the plan's or both
// are absent, its bound symbols are the plan's as a set, and its held
// invocations and options are the plan's; a request differing in any one
// part was not. The parts are compared on a hash hit, so a collision between
// two requests is a miss and never a plan served to the wrong request.
func TestCachedPlanMadeUnderComparesEveryPart(t *testing.T) {
	e, n := datalog.NewSymbol("?e"), datalog.NewSymbol("?n")
	pattern := func() *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: e},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: n},
		}}
	}
	queryOf := func(where ...query.Clause) *query.Query {
		return &query.Query{
			Find:  []query.FindElement{query.FindVariable{Symbol: n}},
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
			Where: where,
		}
	}
	rules := func(body ...query.Clause) *query.RuleSet {
		rs, err := query.NewRuleSet([]*query.Rule{{Name: datalog.NewSymbol("named"), Head: []query.Symbol{e, n}, Body: body}})
		if err != nil {
			t.Fatalf("NewRuleSet: %v", err)
		}
		return rs
	}
	opts := PlannerOptions{EnableAlgebraOptimizer: true}
	made := &cachedPlan{
		plan:    &RealizedPlan{},
		query:   queryOf(pattern(), invocation("named", "?e", "?n")),
		rules:   rules(pattern()),
		bound:   []query.Symbol{e},
		held:    query.HeldByFixpoint,
		options: planOptionsOf(opts),
	}
	same := query.Bindings{Rules: rules(pattern()), Symbols: map[query.Symbol]bool{e: true}, Held: query.HeldByFixpoint}
	if !made.madeUnder(queryOf(pattern(), invocation("named", "?e", "?n")), same, opts) {
		t.Fatal("a plan is not made under a request whose every part is the plan's, each built afresh")
	}

	withRosters := opts
	withRosters.CollectRosters = true
	differing := []struct {
		name     string
		q        *query.Query
		bindings query.Bindings
		opts     PlannerOptions
	}{
		{"the query", queryOf(pattern()), same, opts},
		{"the rule set's body", made.query, query.Bindings{Rules: rules(pattern(), pattern()), Symbols: same.Symbols, Held: same.Held}, opts},
		{"a rule set absent", made.query, query.Bindings{Symbols: same.Symbols, Held: same.Held}, opts},
		{"a bound symbol added", made.query, query.Bindings{Rules: same.Rules, Symbols: map[query.Symbol]bool{e: true, n: true}, Held: same.Held}, opts},
		{"a bound symbol absent", made.query, query.Bindings{Rules: same.Rules, Held: same.Held}, opts},
		{"the held invocations", made.query, query.Bindings{Rules: same.Rules, Symbols: same.Symbols, Held: query.HeldEvery}, opts},
		{"an option", made.query, same, withRosters},
	}
	for _, request := range differing {
		if made.madeUnder(request.q, request.bindings, request.opts) {
			t.Errorf("a plan is made under a request differing in %s", request.name)
		}
	}

	withoutRules := &cachedPlan{plan: made.plan, query: made.query, bound: made.bound, held: made.held, options: made.options}
	if withoutRules.madeUnder(made.query, same, opts) {
		t.Error("a plan made under no rule set is made under a request carrying one")
	}
}

// A cache made with no TTL expires nothing: an entry stored an hour ago is
// served as a hit.
func TestPlanCacheWithNoTTLServesAnOldEntry(t *testing.T) {
	cache := NewPlanCache(10, 0)
	opts := PlannerOptions{}
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?e")},
		},
	}
	plan := &RealizedPlan{Query: q}
	cache.SetWithOptions(q, query.Bindings{}, plan, opts)
	for _, cached := range cache.cache {
		cached.timestamp = time.Now().Add(-time.Hour)
	}
	if _, ok := cache.GetWithOptions(q, query.Bindings{}, opts); !ok {
		t.Fatal("an entry stored an hour ago is served under no TTL")
	}
}

// A plan made under a bound symbol enters the cache and is served again under
// the same bound set, and a plan made under none is another entry of the same
// text.
func TestPlannerCachesAPlanMadeUnderBoundSymbols(t *testing.T) {
	cache := NewPlanCache(100, 0)
	opts := PlannerOptions{}
	opts.Cache = cache
	planner := NewClauseBasedPlanner(nil, opts)
	e := datalog.NewSymbol("?e")
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?n")}},
		In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.ScalarInput{Symbol: e}},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: e},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Variable{Name: datalog.NewSymbol("?n")},
				},
			},
		},
	}
	bound := query.Bindings{Symbols: map[query.Symbol]bool{e: true}}

	if _, err := planner.Plan(q, bound, nil); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if _, err := planner.Plan(q, bound, nil); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	hits, misses, size := cache.Stats()
	if hits != 1 || misses != 1 || size != 1 {
		t.Fatalf("stats = %d hits, %d misses, size %d after two plans under one bound symbol; want 1, 1, 1: the plan made under the bound symbol is served again", hits, misses, size)
	}

	if _, err := planner.Plan(q, query.Bindings{}, nil); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	hits, misses, size = cache.Stats()
	if hits != 1 || misses != 2 || size != 2 {
		t.Fatalf("stats = %d hits, %d misses, size %d after a plan under no bound symbol; want 1, 2, 2: a plan under none is another entry of the text", hits, misses, size)
	}
}

func TestPlannerWithCache(t *testing.T) {
	// Create planner with cache using ClauseBasedPlanner
	cache := NewPlanCache(100, 0)
	opts := PlannerOptions{}
	opts.Cache = cache
	planner := NewClauseBasedPlanner(nil, opts)

	// Create a query
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?e")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Constant{Value: "Charlie"},
				},
			},
		},
	}

	// First plan should be a miss
	_, err := planner.Plan(q, query.Bindings{}, nil)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	hits, misses, size := cache.Stats()
	if hits != 0 {
		t.Errorf("Expected 0 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}
	if size != 1 {
		t.Errorf("Expected cache size 1, got %d", size)
	}

	// Second plan should be a hit
	_, err = planner.Plan(q, query.Bindings{}, nil)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	hits, misses, size = cache.Stats()
	if hits != 1 {
		t.Errorf("Expected 1 hit, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss (unchanged), got %d", misses)
	}
	if size != 1 {
		t.Errorf("Expected cache size 1, got %d", size)
	}

	// Clear cache
	cache.Clear()

	hits, misses, size = cache.Stats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Error("Expected stats to be reset after clear")
	}
}
