package planner

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPlanCache(t *testing.T) {
	cache := NewPlanCache(10, 1*time.Minute)

	// Create a sample query
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: query.Symbol("?e")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Constant{Value: "Alice"},
				},
			},
		},
	}

	// Create a sample plan
	plan := &QueryPlan{
		Phases: []Phase{
			{
				Patterns: []PatternPlan{
					{
						Pattern: &query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: query.Symbol("?e")},
								query.Constant{Value: datalog.NewKeyword(":person/name")},
								query.Constant{Value: "Alice"},
							},
						},
					},
				},
			},
		},
	}

	// Test miss
	cached, ok := cache.Get(q)
	if ok {
		t.Error("Expected cache miss, got hit")
	}
	if cached != nil {
		t.Error("Expected nil plan on cache miss")
	}

	// Store the plan
	cache.Set(q, plan)

	// Test hit
	cached, ok = cache.Get(q)
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
	_, ok = cache.Get(q)
	if ok {
		t.Error("Expected cache miss after clear")
	}
}

func TestPlanCacheEviction(t *testing.T) {
	cache := NewPlanCache(2, 1*time.Hour) // Small cache size

	// Create 3 different queries
	queries := make([]*query.Query, 3)
	plans := make([]*QueryPlan, 3)

	for i := 0; i < 3; i++ {
		queries[i] = &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: query.Symbol("?e")},
			},
			Where: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: query.Symbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":person/id")},
						query.Constant{Value: int64(i)},
					},
				},
			},
		}

		plans[i] = &QueryPlan{
			Phases: []Phase{
				{
					Patterns: []PatternPlan{
						{
							Pattern: queries[i].Where[0].(*query.DataPattern),
						},
					},
				},
			},
		}
	}

	// Add first two queries
	cache.Set(queries[0], plans[0])
	time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	cache.Set(queries[1], plans[1])

	// Both should be in cache
	if _, ok := cache.Get(queries[0]); !ok {
		t.Error("Expected first query to be cached")
	}
	if _, ok := cache.Get(queries[1]); !ok {
		t.Error("Expected second query to be cached")
	}

	// Add third query, should evict oldest (first)
	cache.Set(queries[2], plans[2])

	// First should be evicted
	if _, ok := cache.Get(queries[0]); ok {
		t.Error("Expected first query to be evicted")
	}

	// Second and third should still be cached
	if _, ok := cache.Get(queries[1]); !ok {
		t.Error("Expected second query to still be cached")
	}
	if _, ok := cache.Get(queries[2]); !ok {
		t.Error("Expected third query to be cached")
	}
}

func TestPlanCacheTTL(t *testing.T) {
	cache := NewPlanCache(10, 50*time.Millisecond) // Short TTL for testing

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: query.Symbol("?e")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Constant{Value: "Bob"},
				},
			},
		},
	}

	plan := &QueryPlan{
		Phases: []Phase{
			{
				Patterns: []PatternPlan{
					{
						Pattern: q.Where[0].(*query.DataPattern),
					},
				},
			},
		},
	}

	// Set the plan
	cache.Set(q, plan)

	// Should be in cache immediately
	if _, ok := cache.Get(q); !ok {
		t.Error("Expected query to be cached")
	}

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Should be expired now
	if _, ok := cache.Get(q); ok {
		t.Error("Expected query to be expired")
	}
}

func TestPlannerWithCache(t *testing.T) {
	// Create planner with cache
	cache := NewPlanCache(100, 0)
	planner := NewPlanner(nil, PlannerOptions{Cache: cache})

	// Create a query
	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: query.Symbol("?e")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Constant{Value: "Charlie"},
				},
			},
		},
	}

	// First plan should be a miss
	_, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	hits, misses, size, enabled := planner.CacheStats()
	if !enabled {
		t.Error("Expected cache to be enabled")
	}
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
	_, err = planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	hits, misses, size, _ = planner.CacheStats()
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
	planner.ClearCache()

	hits, misses, size, _ = planner.CacheStats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Error("Expected stats to be reset after clear")
	}

	// Disable cache
	planner.SetCache(nil)

	_, _, _, enabled = planner.CacheStats()
	if enabled {
		t.Error("Expected cache to be disabled")
	}
}
