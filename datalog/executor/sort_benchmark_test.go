package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkSortRelation measures the standalone sort cost on a materialized
// relation (10k rows, two symbols, sort key in the relation), including the
// deduplicating result materialization.
func BenchmarkSortRelation(b *testing.B) {
	const n = 10000
	syms := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age")}
	tuples := make([]Tuple, n)
	for i := 0; i < n; i++ {
		tuples[i] = Tuple{fmt.Sprintf("user%d", i), int64((i * 7919) % n)}
	}
	rel := NewMaterializedRelation(syms, tuples)
	orderBy := []query.OrderByClause{{Variable: syms[1], Direction: query.OrderAsc}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := SortRelation(rel, orderBy)
		if result.Size() != n {
			b.Fatalf("expected %d rows, got %d", n, result.Size())
		}
	}
}

// benchmarkOrderByQuery runs a full Execute with the given query over 10k
// two-attribute entities, so the numbers include planning, matching, join,
// sort, and finalization.
func benchmarkOrderByQuery(b *testing.B, queryStr string) {
	const n = 10000
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	datoms := make([]datalog.Datom, 0, 2*n)
	for i := 0; i < n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("user:%d", i))
		datoms = append(datoms,
			datalog.Datom{E: e, A: nameAttr, V: fmt.Sprintf("user%d", i), Tx: tx},
			datalog.Datom{E: e, A: ageAttr, V: int64((i * 7919) % n), Tx: tx},
		)
	}
	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("parse: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := executor.Execute(q)
		if err != nil {
			b.Fatalf("execute: %v", err)
		}
		if result.Size() != 10000 {
			b.Fatalf("expected 10000 rows, got %d", result.Size())
		}
	}
}

// Projected sort key: sort key is a :find symbol.
func BenchmarkOrderByProjectedKey(b *testing.B) {
	benchmarkOrderByQuery(b, `[:find ?name ?age
	                           :where [?e :user/name ?name]
	                                  [?e :user/age ?age]
	                           :order-by [[?age :asc]]]`)
}

// Non-projected sort key: pays symbol retention through the final phase
// plus the post-sort strip projection.
func BenchmarkOrderByNonProjectedKey(b *testing.B) {
	benchmarkOrderByQuery(b, `[:find ?name
	                           :where [?e :user/name ?name]
	                                  [?e :user/age ?age]
	                           :order-by [[?age :asc]]]`)
}
