package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestDatabaseImplementsPatternMatcher(t *testing.T) {
	// Compile-time verification (also in database.go, but explicit in test)
	var _ executor.PatternMatcher = (*Database)(nil)
}

func TestDatabaseMatch(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Add test data
			tx := db.NewTransaction()
			alice := datalog.NewIdentity("user:1")
			bob := datalog.NewIdentity("user:2")

			tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
			tx.Add(alice, datalog.NewKeyword(":user/age"), int64(30))
			tx.Add(bob, datalog.NewKeyword(":user/name"), "Bob")
			tx.Add(bob, datalog.NewKeyword(":user/age"), int64(25))

			_, err := tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Use Database.Match directly (PatternMatcher interface)
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":user/name")},
					query.Variable{Name: datalog.NewSymbol("?name")},
				},
			}

			result, err := db.Match(query.PatternQuery(pattern), nil)
			if err != nil {
				t.Fatalf("Database.Match: unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}

			// Count results by iterating (relations may be lazy/streaming)
			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()

			if count != 2 {
				t.Errorf("expected 2 results, got %d", count)
			}
		})
	}
}

func TestDatabaseMatchWithConstantEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			alice := datalog.NewIdentity("user:alice")
			tx := db.NewTransaction()
			tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
			tx.Add(alice, datalog.NewKeyword(":user/age"), int64(30))
			_, err := tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Query with constant entity — should match only Alice's attributes
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: alice},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			}

			result, err := db.Match(query.PatternQuery(pattern), nil)
			if err != nil {
				t.Fatalf("Database.Match: unexpected error: %v", err)
			}

			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()

			if count != 2 {
				t.Errorf("expected 2 results (name + age), got %d", count)
			}
		})
	}
}
