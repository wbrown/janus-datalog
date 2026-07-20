package storage

import (
	"sync"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseCacheHitMiss(t *testing.T) {
	c := NewParseCache(100)

	q, err := parser.ParseQuery(`[:find ?e :where [?e :foo/bar "baz"]]`)
	if err != nil {
		t.Fatal(err)
	}

	// Miss
	got, ok := c.Get(`[:find ?e :where [?e :foo/bar "baz"]]`)
	if ok {
		t.Fatal("expected miss on empty cache")
	}
	if got != nil {
		t.Fatal("expected nil on miss")
	}

	// Set
	c.Set(`[:find ?e :where [?e :foo/bar "baz"]]`, q)

	// Hit
	got, ok = c.Get(`[:find ?e :where [?e :foo/bar "baz"]]`)
	if !ok {
		t.Fatal("expected hit after Set")
	}
	if got != q {
		t.Fatal("expected same *query.Query pointer")
	}

	// Different string = miss
	_, ok = c.Get(`[:find ?e :where [?e :foo/bar "other"]]`)
	if ok {
		t.Fatal("expected miss for different query")
	}
}

func TestParseCacheStats(t *testing.T) {
	c := NewParseCache(100)

	q, _ := parser.ParseQuery(`[:find ?e :where [?e :a ?v]]`)
	c.Set(`[:find ?e :where [?e :a ?v]]`, q)

	// 1 hit
	c.Get(`[:find ?e :where [?e :a ?v]]`)
	// 2 misses
	c.Get(`[:find ?x :where [?x :b ?v]]`)
	c.Get(`[:find ?y :where [?y :c ?v]]`)

	hits, misses, size := c.Stats()
	if hits != 1 {
		t.Errorf("expected 1 hit, got %d", hits)
	}
	if misses != 2 {
		t.Errorf("expected 2 misses, got %d", misses)
	}
	if size != 1 {
		t.Errorf("expected size 1, got %d", size)
	}
}

func TestParseCacheEviction(t *testing.T) {
	c := NewParseCache(3)

	queries := []string{
		`[:find ?a :where [?a :x "1"]]`,
		`[:find ?b :where [?b :x "2"]]`,
		`[:find ?c :where [?c :x "3"]]`,
	}

	for _, qs := range queries {
		q, _ := parser.ParseQuery(qs)
		c.Set(qs, q)
	}

	_, _, size := c.Stats()
	if size != 3 {
		t.Errorf("expected size 3, got %d", size)
	}

	// Adding a 4th should evict one
	q4, _ := parser.ParseQuery(`[:find ?d :where [?d :x "4"]]`)
	c.Set(`[:find ?d :where [?d :x "4"]]`, q4)

	_, _, size = c.Stats()
	if size != 3 {
		t.Errorf("expected size 3 after eviction, got %d", size)
	}
}

func TestParseCacheNilSafe(t *testing.T) {
	var c *ParseCache

	// All operations on nil cache should be no-ops
	got, ok := c.Get("anything")
	if ok || got != nil {
		t.Fatal("expected nil/false from nil cache")
	}

	c.Set("anything", &query.Query{}) // should not panic
	c.Clear()                         // should not panic

	hits, misses, size := c.Stats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Fatal("expected zeros from nil cache stats")
	}
}

func TestParseCacheSetNilQuery(t *testing.T) {
	c := NewParseCache(100)
	c.Set("test", nil) // should be no-op

	_, _, size := c.Stats()
	if size != 0 {
		t.Errorf("expected size 0 after Set(nil), got %d", size)
	}
}

func TestParseCacheClear(t *testing.T) {
	c := NewParseCache(100)
	q, _ := parser.ParseQuery(`[:find ?e :where [?e :a ?v]]`)
	c.Set(`[:find ?e :where [?e :a ?v]]`, q)
	c.Get(`[:find ?e :where [?e :a ?v]]`)
	c.Get(`[:find ?x :where [?x :b ?v]]`)

	c.Clear()

	hits, misses, size := c.Stats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Errorf("expected all zeros after Clear, got hits=%d misses=%d size=%d", hits, misses, size)
	}
}

func TestParseCacheConcurrent(t *testing.T) {
	c := NewParseCache(100)
	q, _ := parser.ParseQuery(`[:find ?e :where [?e :a ?v]]`)
	c.Set(`[:find ?e :where [?e :a ?v]]`, q)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Get(`[:find ?e :where [?e :a ?v]]`)
			}
		}()
	}
	wg.Wait()

	hits, _, _ := c.Stats()
	if hits != 10000 {
		t.Errorf("expected 10000 hits, got %d", hits)
	}
}
