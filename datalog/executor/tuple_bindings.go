package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// bindTuple writes tuple's values into bindings keyed by the corresponding
// symbol, overwriting any previously bound value for a symbol the tuple
// carries. Callers own the map lifecycle (clearing between tuples,
// pre-populating constant bindings). A tuple narrower than its symbol list
// is a broken Relation invariant — every tuple carries exactly its
// relation's symbols — so indexing panics rather than silently binding a
// partial row.
func bindTuple(bindings map[query.Symbol]interface{}, symbols []query.Symbol, tuple Tuple) {
	for i, sym := range symbols {
		bindings[sym] = tuple[i]
	}
}
