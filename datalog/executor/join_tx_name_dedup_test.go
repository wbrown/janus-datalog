package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestHashJoin_TxNameVariablesDoNotDropTuples is a regression test for
// BUG_HASHJOIN_TX_SYMBOL_NAME_DROPS_TUPLES.
//
// HashJoin must perform a pure relational join. It must NOT infer
// "latest-transaction-wins" deduplication from a build-side symbol's *name*.
// ?t, ?tx, ?txid, and ?transaction are ordinary user variables (time, ticker,
// task, type, ...). Distinct tuples that share the join key but differ in such a
// symbol must all survive the join. CRDT/temporal resolution belongs to the
// storage layer (EATV ordering), not to a generic join operator.
func TestHashJoin_TxNameVariablesDoNotDropTuples(t *testing.T) {
	for _, name := range []string{"?t", "?tx", "?txid", "?transaction"} {
		t.Run(name, func(t *testing.T) {
			// Build side (smaller of the two → selected as the hash build side):
			// two distinct tuples sharing the join key ?g, differing only in the
			// name-clashing symbol, with integer values that the buggy path
			// treats as transaction IDs.
			buildSymbols := []query.Symbol{datalog.NewSymbol("?g"), datalog.NewSymbol(name)}
			buildTuples := []Tuple{
				{"g1", int64(1)},
				{"g1", int64(2)},
			}
			build := NewMaterializedRelation(buildSymbols, buildTuples)

			// Probe side (larger → not the build side): one matching key.
			probeSymbols := []query.Symbol{datalog.NewSymbol("?g"), datalog.NewSymbol("?label")}
			probeTuples := []Tuple{
				{"g1", "x"},
				{"g2", "y"},
				{"g3", "z"},
			}
			probe := NewMaterializedRelation(probeSymbols, probeTuples)

			joined := HashJoinWithOptions(build, probe,
				[]query.Symbol{datalog.NewSymbol("?g")}, ExecutorOptions{})
			got := collectTuples(joined)

			if len(got) != 2 {
				t.Fatalf("%s used as an ordinary build-side symbol: expected 2 tuples "+
					"(both values preserved), got %d: %v", name, len(got), got)
			}

			tIdx := SymbolIndex(joined, datalog.NewSymbol(name))
			if tIdx < 0 {
				t.Fatalf("output is missing symbol %s; symbols=%v", name, joined.Symbols())
			}
			seen := map[int64]bool{}
			for _, tuple := range got {
				if v, ok := tuple[tIdx].(int64); ok {
					seen[v] = true
				}
			}
			if !seen[1] || !seen[2] {
				t.Fatalf("%s: expected both values 1 and 2 in output, got %v (tuples=%v)",
					name, seen, got)
			}
		})
	}
}
