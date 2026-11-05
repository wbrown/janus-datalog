package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestDatabaseMatch(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Add test data
			tx := db.NewTransaction()
			alice := datalog.NewIdentity("user:1")
			bob := datalog.NewIdentity("user:2")

			require.NoError(t, tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice"))
			require.NoError(t, tx.Add(alice, datalog.NewKeyword(":user/age"), int64(30)))
			require.NoError(t, tx.Add(bob, datalog.NewKeyword(":user/name"), "Bob"))
			require.NoError(t, tx.Add(bob, datalog.NewKeyword(":user/age"), int64(25)))

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

			// The match answers each user with their name.
			ei := query.SymbolIndex(result.Symbols(), datalog.NewSymbol("?e"))
			ni := query.SymbolIndex(result.Symbols(), datalog.NewSymbol("?name"))
			if ei < 0 || ni < 0 {
				t.Fatalf("the match carries ?e and ?name; got %v", result.Symbols())
			}
			var pairs [][]interface{}
			it := result.Iterator()
			for it.Next() {
				tuple := it.Tuple()
				pairs = append(pairs, []interface{}{tuple[ei], tuple[ni]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			assert.ElementsMatch(t, [][]interface{}{{alice, "Alice"}, {bob, "Bob"}}, pairs)
		})
	}
}

func TestDatabaseMatchWithConstantEntity(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			alice := datalog.NewIdentity("user:alice")
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice"))
			require.NoError(t, tx.Add(alice, datalog.NewKeyword(":user/age"), int64(30)))
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

			ai := query.SymbolIndex(result.Symbols(), datalog.NewSymbol("?a"))
			vi := query.SymbolIndex(result.Symbols(), datalog.NewSymbol("?v"))
			if ai < 0 || vi < 0 {
				t.Fatalf("the match carries ?a and ?v; got %v", result.Symbols())
			}
			var pairs [][]interface{}
			it := result.Iterator()
			for it.Next() {
				tuple := it.Tuple()
				pairs = append(pairs, []interface{}{tuple[ai], tuple[vi]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			assert.ElementsMatch(t, [][]interface{}{
				{datalog.NewKeyword(":user/name"), "Alice"},
				{datalog.NewKeyword(":user/age"), int64(30)},
			}, pairs)
		})
	}
}
