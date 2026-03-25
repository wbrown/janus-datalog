package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

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
	case datalog.ElementID:
		lamport = v.Lamport
	case *datalog.ElementID:
		if v != nil {
			lamport = v.Lamport
		}
	default:
		return false, fmt.Errorf("tx-between: cannot extract Lamport from %T", txVal)
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
