package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkSortRelation measures the standalone sort cost on a materialized
// relation (10k tuples, two symbols, sort key in the relation), including the
// deduplicating result materialization.
func BenchmarkSortRelation(b *testing.B) {
	const n = 10000
	syms := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?age")}
	tuples := make([]Tuple, n)
	for i := 0; i < n; i++ {
		tuples[i] = Tuple{fmt.Sprintf("user%d", i), int64((i * 7919) % n)}
	}
	rel := NewMaterializedRelation(syms, tuples)
	orderBy := []query.OrderByClause{{Variable: syms[1], Descending: false}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := SortRelation(rel, orderBy)
		if result.Size() != n {
			b.Fatalf("expected %d tuples, got %d", n, result.Size())
		}
	}
}
