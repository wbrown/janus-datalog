package storage

import (
	"sync"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestTupleBuilderCacheConcurrency tests that the tuple builder cache is thread-safe
// This reproduces the concurrent map access bug reported by gopher-street team
func TestTupleBuilderCacheConcurrency(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	matcher := NewBadgerMatcher(db.Store())

	// Create test pattern and columns
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: "?v"},
		},
	}
	columns := []query.Symbol{"?e", "?v"}

	// Spawn 1000 goroutines accessing cache concurrently
	// This should trigger the concurrent map access bug if not fixed
	var wg sync.WaitGroup
	errorChan := make(chan error, 1000)

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				builder := matcher.getTupleBuilder(pattern, columns)
				if builder == nil {
					select {
					case errorChan <- nil:
					default:
					}
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errorChan)

	// Check if any goroutine reported an error
	if err := <-errorChan; err != nil {
		t.Errorf("getTupleBuilder returned nil")
	}
}

// TestTupleBuilderCacheSharing tests that AsOf matchers share the cache correctly
func TestTupleBuilderCacheSharing(t *testing.T) {
	db := createTestDB(t)
	defer db.Close()

	baseMatcher := NewBadgerMatcher(db.Store())
	asOfMatcher := baseMatcher.AsOf(100)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":test/attr")},
			query.Variable{Name: "?v"},
		},
	}
	columns := []query.Symbol{"?e", "?v"}

	// Get builder from base matcher
	builder1 := baseMatcher.getTupleBuilder(pattern, columns)

	// Get builder from AsOf matcher - should be the same instance (shared cache)
	builder2 := asOfMatcher.getTupleBuilder(pattern, columns)

	if builder1 != builder2 {
		t.Error("AsOf matcher should share cache with base matcher")
	}
}

func createTestDB(t *testing.T) *Database {
	t.Helper()
	db, err := NewDatabase(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	return db
}
