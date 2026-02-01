package query

import "fmt"

// HistoryPredicateType indicates the type of history query
type HistoryPredicateType int

const (
	// HistoryAll returns all historical values: [(history)]
	HistoryAll HistoryPredicateType = iota
	// HistoryAsOf returns value as of a specific transaction: [(as-of ?tx N)]
	HistoryAsOf
)

// HistoryPredicate is a special predicate that modifies query behavior
// to return historical values instead of just current values.
//
// Usage:
//   [(history)]        - Return all versions of matched values
//   [(as-of ?tx 5000)] - Return value as of Lamport time 5000
//
// This predicate doesn't filter results - it changes how the matcher
// retrieves data. Eval() always returns true.
type HistoryPredicate struct {
	Type          HistoryPredicateType
	TxVar         Symbol // For as-of: which variable holds Tx
	TargetLamport uint64 // For as-of: filter to Lamport <= this value
}

// Pattern interface
func (h *HistoryPredicate) String() string {
	switch h.Type {
	case HistoryAll:
		return "(history)"
	case HistoryAsOf:
		return fmt.Sprintf("(as-of %s %d)", h.TxVar.String(), h.TargetLamport)
	default:
		return "(history ?)"
	}
}

// Clause interface
func (h *HistoryPredicate) clause() {}

// Predicate interface
func (h *HistoryPredicate) RequiredSymbols() []Symbol {
	if h.Type == HistoryAsOf && h.TxVar != nil {
		return []Symbol{h.TxVar}
	}
	return nil
}

// Eval always returns true - history predicates don't filter results,
// they modify how data is retrieved.
func (h *HistoryPredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	return true, nil
}

// Selectivity returns 1.0 since this predicate doesn't filter anything
func (h *HistoryPredicate) Selectivity() float64 {
	return 1.0
}

// CanPushToStorage returns true - this predicate is handled at the storage level
func (h *HistoryPredicate) CanPushToStorage() bool {
	return true
}

// IsHistoryQuery returns true if the query contains a history predicate
func (q *Query) IsHistoryQuery() bool {
	for _, clause := range q.Where {
		if _, ok := clause.(*HistoryPredicate); ok {
			return true
		}
	}
	return false
}

// GetHistoryPredicate returns the history predicate if present, nil otherwise
func (q *Query) GetHistoryPredicate() *HistoryPredicate {
	for _, clause := range q.Where {
		if hp, ok := clause.(*HistoryPredicate); ok {
			return hp
		}
	}
	return nil
}

// TxRangePredicate filters results to a transaction range.
// Usage: [(tx-between ?tx 1000 2000)]
// Returns true if the bound ?tx variable's Lamport value is in [Low, High].
type TxRangePredicate struct {
	TxVar Symbol // Variable holding the transaction/ElementID
	Low   uint64 // Minimum Lamport value (inclusive)
	High  uint64 // Maximum Lamport value (inclusive)
}

// Pattern interface
func (t *TxRangePredicate) String() string {
	return fmt.Sprintf("(tx-between %s %d %d)", t.TxVar.String(), t.Low, t.High)
}

// Clause interface
func (t *TxRangePredicate) clause() {}

// Predicate interface
func (t *TxRangePredicate) RequiredSymbols() []Symbol {
	return []Symbol{t.TxVar}
}

// Eval checks if the transaction is within the specified range
func (t *TxRangePredicate) Eval(bindings map[Symbol]interface{}) (bool, error) {
	txVal, ok := bindings[t.TxVar]
	if !ok {
		return false, fmt.Errorf("tx-between: variable %s not bound", t.TxVar)
	}

	// Handle different transaction representations
	var lamport uint64
	switch v := txVal.(type) {
	case uint64:
		lamport = v
	case int64:
		lamport = uint64(v)
	case int:
		lamport = uint64(v)
	default:
		// Try to extract Lamport from ElementID if available
		if elemID, ok := txVal.(interface{ GetLamport() uint64 }); ok {
			lamport = elemID.GetLamport()
		} else {
			return false, fmt.Errorf("tx-between: cannot extract Lamport from %T", txVal)
		}
	}

	return lamport >= t.Low && lamport <= t.High, nil
}

// Selectivity estimates fraction of transactions that pass
// This is a rough estimate - actual selectivity depends on data distribution
func (t *TxRangePredicate) Selectivity() float64 {
	// Assume uniform distribution, estimate based on range width
	// This is very rough - real systems would use statistics
	return 0.1 // Default estimate: 10% of transactions
}

// CanPushToStorage returns true - this can be optimized at storage level
func (t *TxRangePredicate) CanPushToStorage() bool {
	return true
}
