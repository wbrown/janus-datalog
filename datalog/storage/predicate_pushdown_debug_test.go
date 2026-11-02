package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPredicatePushdownDebug(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create just 2 days of data with 10 bars each for easier debugging
			tx := db.NewTransaction()

			symbolEntity := datalog.NewIdentity("TEST")
			require.NoError(t, tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "TEST"))

			priceSymbol := datalog.NewKeyword(":price/symbol")
			priceTime := datalog.NewKeyword(":price/time")

			loc, err := time.LoadLocation("America/New_York")
			require.NoError(t, err)

			// Day 1: 10 bars
			for i := 0; i < 10; i++ {
				barEntity := datalog.NewIdentity(fmt.Sprintf("bar-1-%d", i))
				barTime := time.Date(2025, 6, 1, 10+i, 0, 0, 0, loc)
				require.NoError(t, tx.Add(barEntity, priceSymbol, symbolEntity))
				require.NoError(t, tx.Add(barEntity, priceTime, barTime))
			}

			// Day 2: 10 bars
			for i := 0; i < 10; i++ {
				barEntity := datalog.NewIdentity(fmt.Sprintf("bar-2-%d", i))
				barTime := time.Date(2025, 6, 2, 10+i, 0, 0, 0, loc)
				require.NoError(t, tx.Add(barEntity, priceSymbol, symbolEntity))
				require.NoError(t, tx.Add(barEntity, priceTime, barTime))
			}

			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Test patterns
			symbolPattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?b")},
					query.Constant{Value: priceSymbol},
					query.Constant{Value: symbolEntity},
				},
			}

			timePattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?b")},
					query.Constant{Value: priceTime},
					query.Variable{Name: datalog.NewSymbol("?t")},
				},
			}

			matcher := NewPatternMatcher(db.store)

			// First get all bars for symbol
			symbolRel, err := matcher.Match(query.PatternQuery(symbolPattern), nil)
			if err != nil {
				t.Fatalf("Failed to match symbol pattern: %v", err)
			}

			// Materialize so we can iterate AND pass to Match() below (which will call Sorted())
			symbolRel = symbolRel.Materialize()

			symbolCount := 0
			it := symbolRel.Iterator()
			for it.Next() {
				symbolCount++
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, 20, symbolCount, "the symbol pattern matches every bar")

			// countDays answers how many of a relation's tuples fall on each
			// day of ?t.
			countDays := func(t *testing.T, rel executor.Relation) map[int]int {
				ti := query.SymbolIndex(rel.Symbols(), datalog.NewSymbol("?t"))
				require.GreaterOrEqual(t, ti, 0, "the relation carries ?t; got %v", rel.Symbols())
				days := map[int]int{}
				it := rel.Iterator()
				for it.Next() {
					days[it.Tuple()[ti].(time.Time).Day()]++
				}
				require.NoError(t, it.Error())
				require.NoError(t, it.Close())
				return days
			}

			// Test without constraint
			t.Run("WithoutConstraint", func(t *testing.T) {
				start := time.Now()

				timeRel, err := matcher.Match(query.PatternQuery(timePattern), executor.Relations{symbolRel})
				if err != nil {
					t.Fatalf("Failed to match time pattern: %v", err)
				}

				assert.Equal(t, map[int]int{1: 10, 2: 10}, countDays(t, timeRel), "ten bars on each day")
				t.Logf("Without constraint: %v", time.Since(start))
			})

			// Test with constraint
			t.Run("WithConstraint", func(t *testing.T) {
				constraint := &timeExtractionConstraint{
					position:  2,
					extractFn: "day",
					expected:  int64(1),
				}

				start := time.Now()

				// Check what MatchWithConstraints is doing
				t.Logf("Calling MatchWithConstraints with:")
				t.Logf("  Pattern: %s", timePattern)
				t.Logf("  Binding relation size: %d", symbolRel.Size())
				t.Logf("  Constraint: %s", constraint)

				timeRel, err := matcher.MatchWithConstraints(
					query.PatternQuery(timePattern),
					executor.Relations{symbolRel},
					[]executor.StorageConstraint{constraint},
				)
				if err != nil {
					t.Fatalf("Failed to match with constraint: %v", err)
				}

				assert.Equal(t, map[int]int{1: 10}, countDays(t, timeRel), "the day-1 bars alone")
				t.Logf("With constraint: %v", time.Since(start))
			})

			// Test the actual constraint evaluation
			t.Run("ConstraintEvaluation", func(t *testing.T) {
				constraint := &timeExtractionConstraint{
					position:  2,
					extractFn: "day",
					expected:  int64(1),
				}

				// Create test datoms
				testTime1 := time.Date(2025, 6, 1, 10, 0, 0, 0, loc)
				testTime2 := time.Date(2025, 6, 2, 10, 0, 0, 0, loc)

				datom1 := &datalog.Datom{
					E: datalog.NewIdentity("test"),
					A: priceTime,
					V: testTime1,
				}

				datom2 := &datalog.Datom{
					E: datalog.NewIdentity("test"),
					A: priceTime,
					V: testTime2,
				}

				assert.True(t, constraint.Evaluate(datom1), "a day-1 time satisfies day = 1")
				assert.False(t, constraint.Evaluate(datom2), "a day-2 time does not satisfy day = 1")
			})
		})
	}
}
