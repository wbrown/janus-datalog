package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func BenchmarkAVETReuse(b *testing.B) {
	// Create test database
	dbPath := "/tmp/bench-avet-reuse"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create symbols
	symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA"}
	tx := db.NewTransaction()
	for _, ticker := range symbols {
		e := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(e, datalog.NewKeyword(":symbol/ticker"), ticker)
	}
	tx.Commit()

	// Load data: 30 days of minute bars
	days := 30
	minutesPerDay := 390

	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))

		for day := 0; day < days; day++ {
			tx := db.NewTransaction()

			for minute := 0; minute < minutesPerDay; minute++ {
				barEntity := datalog.NewIdentity(
					fmt.Sprintf("bar:%s:%d:%d", ticker, day, minute))

				tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
				tx.Add(barEntity, datalog.NewKeyword(":price/minute-of-day"), int64(570+minute))
				tx.Add(barEntity, datalog.NewKeyword(":price/open"), 100.0+float64(minute)*0.1)
			}

			tx.Commit()
		}
	}

	// Pattern: [?b :price/symbol ?s] with multiple symbols bound
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?b"},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Variable{Name: "?s"},
		},
	}

	// Create binding relation with all symbols
	var symbolTuples []executor.Tuple
	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		symbolTuples = append(symbolTuples, executor.Tuple{symbolEntity})
	}
	symbolRel := executor.NewMaterializedRelation(
		[]query.Symbol{"?s"},
		symbolTuples,
	)

	matcher := NewBadgerMatcher(db.store)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := matcher.Match(pattern, executor.Relations{symbolRel})
		if err != nil {
			b.Fatal(err)
		}
		if result.Size() != days*minutesPerDay*len(symbols) {
			b.Fatalf("Expected %d results, got %d",
				days*minutesPerDay*len(symbols), result.Size())
		}
	}
}

func BenchmarkAVETNoReuse(b *testing.B) {
	// Same setup but we'll temporarily disable reuse
	dbPath := "/tmp/bench-avet-no-reuse"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create symbols
	symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA"}
	tx := db.NewTransaction()
	for _, ticker := range symbols {
		e := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(e, datalog.NewKeyword(":symbol/ticker"), ticker)
	}
	tx.Commit()

	// Load data: 30 days of minute bars
	days := 30
	minutesPerDay := 390

	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))

		for day := 0; day < days; day++ {
			tx := db.NewTransaction()

			for minute := 0; minute < minutesPerDay; minute++ {
				barEntity := datalog.NewIdentity(
					fmt.Sprintf("bar:%s:%d:%d", ticker, day, minute))

				tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
				tx.Add(barEntity, datalog.NewKeyword(":price/minute-of-day"), int64(570+minute))
				tx.Add(barEntity, datalog.NewKeyword(":price/open"), 100.0+float64(minute)*0.1)
			}

			tx.Commit()
		}
	}

	// Pattern: [?b :price/symbol ?s] with multiple symbols bound
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?b"},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Variable{Name: "?s"},
		},
	}

	// Create binding relation with all symbols
	var symbolTuples []executor.Tuple
	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		symbolTuples = append(symbolTuples, executor.Tuple{symbolEntity})
	}
	symbolRel := executor.NewMaterializedRelation(
		[]query.Symbol{"?s"},
		symbolTuples,
	)

	// Create a custom matcher that forces no reuse
	// We'll need to temporarily hack the strategy selection
	matcher := NewBadgerMatcher(db.store)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// TODO: Need to force no-reuse strategy
		result, err := matcher.Match(pattern, executor.Relations{symbolRel})
		if err != nil {
			b.Fatal(err)
		}
		if result.Size() != days*minutesPerDay*len(symbols) {
			b.Fatalf("Expected %d results, got %d",
				days*minutesPerDay*len(symbols), result.Size())
		}
	}
}
