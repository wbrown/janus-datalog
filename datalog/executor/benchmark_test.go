package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Benchmark expression evaluation
func BenchmarkExpressionEvaluation(b *testing.B) {
	// Create test data
	columns := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}
	tuples := make([]Tuple, 1000)
	for i := 0; i < 1000; i++ {
		tuples[i] = Tuple{int64(i), int64(i * 2), float64(i) * 1.5}
	}
	rel := NewMaterializedRelation(columns, tuples)

	expr := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:    query.OpAdd,
			Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
			Right: query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
		},
		Binding: datalog.NewSymbol("?sum"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := rel.EvaluateFunction(expr.Function, expr.Binding.(query.Symbol))
		_ = result.Size()
	}
}

// Benchmark aggregation operations
func BenchmarkAggregation(b *testing.B) {
	// Create test data with groups
	columns := []query.Symbol{datalog.NewSymbol("?group"), datalog.NewSymbol("?value")}
	tuples := make([]Tuple, 10000)
	for i := 0; i < 10000; i++ {
		tuples[i] = Tuple{int64(i % 100), float64(i)}
	}
	rel := NewMaterializedRelation(columns, tuples)

	b.Run("single_aggregation", func(b *testing.B) {
		findElements := []query.FindElement{
			query.FindAggregate{Function: "sum", Arg: datalog.NewSymbol("?value")},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := ExecuteAggregations(rel, findElements)
			_ = result.Size()
		}
	})

	b.Run("grouped_aggregation", func(b *testing.B) {
		findElements := []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?group")},
			query.FindAggregate{Function: "sum", Arg: datalog.NewSymbol("?value")},
			query.FindAggregate{Function: "avg", Arg: datalog.NewSymbol("?value")},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := ExecuteAggregations(rel, findElements)
			_ = result.Size()
		}
	})

	b.Run("projection_only", func(b *testing.B) {
		findElements := []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?group")},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := ExecuteAggregations(rel, findElements)
			_ = result.Size()
		}
	})
}

// Benchmark a full query with expressions and aggregations
func BenchmarkFullQuery(b *testing.B) {
	// Create test data
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	scoreAttr := datalog.NewKeyword(":person/score")

	datoms := []datalog.Datom{}

	// Generate test data
	for i := 0; i < 100; i++ {
		person := datalog.NewIdentity("person:" + string(rune('a'+i%26)) + string(rune('0'+i/26)))
		datoms = append(datoms,
			datalog.Datom{E: person, A: nameAttr, V: "Person" + string(rune(i)), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: person, A: ageAttr, V: int64(20 + i%40), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: person, A: scoreAttr, V: float64(50 + i%50), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		)
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Query with expression and aggregation
	queryStr := `[:find ?age (avg ?score)
	              :where [?p :person/age ?age]
	                     [?p :person/score ?score]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := executor.Execute(q)
		if err != nil {
			b.Fatalf("Query execution failed: %v", err)
		}
		_ = result.Size()
	}
}

// Benchmark time-based aggregations
func BenchmarkTimeAggregation(b *testing.B) {
	columns := []query.Symbol{datalog.NewSymbol("?date"), datalog.NewSymbol("?value")}
	tuples := make([]Tuple, 1000)
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 1000; i++ {
		tuples[i] = Tuple{
			baseTime.Add(time.Duration(i) * time.Hour),
			float64(i * 10),
		}
	}
	rel := NewMaterializedRelation(columns, tuples)

	findElements := []query.FindElement{
		query.FindAggregate{Function: "min", Arg: datalog.NewSymbol("?date")},
		query.FindAggregate{Function: "max", Arg: datalog.NewSymbol("?date")},
		query.FindAggregate{Function: "sum", Arg: datalog.NewSymbol("?value")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := ExecuteAggregations(rel, findElements)
		_ = result.Size()
	}
}
