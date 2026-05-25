package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestHashJoin_TxNameVariablesDoNotDropRows is a regression test for
// BUG_HASHJOIN_TX_SYMBOL_NAME_DROPS_ROWS.
//
// HashJoin must perform a pure relational join. It must NOT infer
// "latest-transaction-wins" deduplication from a build-side column's *name*.
// ?t, ?tx, ?txid, and ?transaction are ordinary user variables (time, ticker,
// task, type, ...). Distinct rows that share the join key but differ in such a
// column must all survive the join. CRDT/temporal resolution belongs to the
// storage layer (EATV ordering), not to a generic join operator.
func TestHashJoin_TxNameVariablesDoNotDropRows(t *testing.T) {
	for _, name := range []string{"?t", "?tx", "?txid", "?transaction"} {
		t.Run(name, func(t *testing.T) {
			// Build side (smaller of the two → selected as the hash build side):
			// two distinct rows sharing the join key ?g, differing only in the
			// name-clashing column, with integer values that the buggy path
			// treats as transaction IDs.
			buildCols := []query.Symbol{datalog.NewSymbol("?g"), datalog.NewSymbol(name)}
			buildTuples := []Tuple{
				{"g1", int64(1)},
				{"g1", int64(2)},
			}
			build := NewMaterializedRelation(buildCols, buildTuples)

			// Probe side (larger → not the build side): one matching key.
			probeCols := []query.Symbol{datalog.NewSymbol("?g"), datalog.NewSymbol("?label")}
			probeTuples := []Tuple{
				{"g1", "x"},
				{"g2", "y"},
				{"g3", "z"},
			}
			probe := NewMaterializedRelation(probeCols, probeTuples)

			joined := HashJoinWithOptions(build, probe,
				[]query.Symbol{datalog.NewSymbol("?g")}, ExecutorOptions{})
			got := collectTuples(joined)

			if len(got) != 2 {
				t.Fatalf("%s used as an ordinary build-side column: expected 2 rows "+
					"(both values preserved), got %d: %v", name, len(got), got)
			}

			tIdx := SymbolIndex(joined, datalog.NewSymbol(name))
			if tIdx < 0 {
				t.Fatalf("output is missing column %s; symbols=%v", name, joined.Symbols())
			}
			seen := map[int64]bool{}
			for _, row := range got {
				if v, ok := row[tIdx].(int64); ok {
					seen[v] = true
				}
			}
			if !seen[1] || !seen[2] {
				t.Fatalf("%s: expected both values 1 and 2 in output, got %v (rows=%v)",
					name, seen, got)
			}
		})
	}
}
