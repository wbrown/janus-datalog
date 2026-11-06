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

func TestPredicatePushdownTrace(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create 3 days of data, 100 bars each
			symbolEntity := datalog.NewIdentity("TEST")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "TEST"))
			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit symbol: %v", err)
			}

			loc, err := time.LoadLocation("America/New_York")
			require.NoError(t, err)
			for day := 1; day <= 3; day++ {
				tx = db.NewTransaction()
				for i := 0; i < 100; i++ {
					barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%d-%d", day, i))
					barTime := time.Date(2025, 6, day, 9, 30+i, 0, 0, loc)
					require.NoError(t, tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity))
					require.NoError(t, tx.Add(barEntity, datalog.NewKeyword(":price/time"), barTime))
				}
				if _, err := tx.Commit(); err != nil {
					t.Fatalf("Failed to commit day %d: %v", day, err)
				}
			}

			// Patterns
			symbolPattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?b")},
					query.Constant{Value: datalog.NewKeyword(":price/symbol")},
					query.Constant{Value: symbolEntity},
				},
			}

			timePattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?b")},
					query.Constant{Value: datalog.NewKeyword(":price/time")},
					query.Variable{Name: datalog.NewSymbol("?t")},
				},
			}

			matcher := NewPatternMatcher(db.store)

			// Get all bars (300 total)
			symbolRel, err := matcher.Match(query.PatternQuery(symbolPattern), nil)
			require.NoError(t, err)
			t.Logf("Symbol relation size: %d", symbolRel.Size())

			// Check what strategy is selected; the binding relation it hands
			// back stands in for the one passed.
			strategy, symbolRel, err := matcher.analyzeReuseStrategy(timePattern, symbolRel)
			require.NoError(t, err)
			t.Logf("Reuse strategy: Type=%s, Position=%d, Index=%d",
				strategy.Type.String(), strategy.Position, strategy.Index)

			// Now test with constraint
			constraint := &timeExtractionConstraint{
				position:  2,
				extractFn: "day",
				expected:  int64(2),
			}

			// Measure with constraint
			start := time.Now()
			timeRel, err := matcher.MatchWithConstraints(
				query.PatternQuery(timePattern),
				executor.Relations{symbolRel},
				[]executor.StorageConstraint{constraint},
			)
			if err != nil {
				t.Fatalf("Failed: %v", err)
			}

			ti := query.SymbolIndex(timeRel.Symbols(), datalog.NewSymbol("?t"))
			require.GreaterOrEqual(t, ti, 0, "the answer carries ?t; got %v", timeRel.Symbols())
			days := map[int]int{}
			it := timeRel.Iterator()
			for it.Next() {
				days[it.Tuple()[ti].(time.Time).Day()]++
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			elapsed := time.Since(start)

			t.Logf("Results: %v in %v", days, elapsed)
			assert.Equal(t, map[int]int{2: 100}, days, "the day-2 bars alone")

			// Check if iterator reuse is actually happening
			if strategy.Type == SinglePositionReuse {
				t.Logf("Iterator reuse SHOULD be used (position %d, index %s)",
					strategy.Position, strategy.Index)
			} else {
				t.Logf("Iterator reuse NOT used - this explains the slowdown!")
			}
		})
	}
}
