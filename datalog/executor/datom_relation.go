package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// DatomIterator works with datoms from query results
type DatomIterator struct {
	datoms  []datalog.Datom
	pos     int
	symbols []query.Symbol
	pattern PatternBinding
	current Tuple
}

func NewDatomIterator(datoms []datalog.Datom, binding PatternBinding) *DatomIterator {
	// Build symbols from binding
	var symbols []query.Symbol
	if binding.EntitySym != nil {
		symbols = append(symbols, *binding.EntitySym)
	}
	if binding.AttributeSym != nil {
		symbols = append(symbols, *binding.AttributeSym)
	}
	if binding.ValueSym != nil {
		symbols = append(symbols, *binding.ValueSym)
	}
	if binding.TxSym != nil {
		symbols = append(symbols, *binding.TxSym)
	}

	return &DatomIterator{
		datoms:  datoms,
		pos:     -1,
		symbols: symbols,
		pattern: binding,
	}
}

func (it *DatomIterator) Next() bool {
	it.pos++
	if it.pos >= len(it.datoms) {
		return false
	}

	datom := it.datoms[it.pos]

	// Build tuple from datom based on binding
	var tuple Tuple
	if it.pattern.EntitySym != nil {
		tuple = append(tuple, datom.E)
	}
	if it.pattern.AttributeSym != nil {
		tuple = append(tuple, datom.A)
	}
	if it.pattern.ValueSym != nil {
		tuple = append(tuple, datom.V)
	}
	if it.pattern.TxSym != nil {
		tuple = append(tuple, datom.Tx)
	}

	it.current = tuple
	return true
}

func (it *DatomIterator) Tuple() Tuple {
	return it.current
}

func (it *DatomIterator) Close() error {
	return nil
}

func (it *DatomIterator) Error() error { return nil }

// NewDatomRelation creates a relation from datoms
func NewDatomRelation(datoms []datalog.Datom, binding PatternBinding) Relation {
	// Build symbols from binding
	var symbols []query.Symbol
	symbolCount := 0
	if binding.EntitySym != nil {
		symbols = append(symbols, *binding.EntitySym)
		symbolCount++
	}
	if binding.AttributeSym != nil {
		symbols = append(symbols, *binding.AttributeSym)
		symbolCount++
	}
	if binding.ValueSym != nil {
		symbols = append(symbols, *binding.ValueSym)
		symbolCount++
	}
	if binding.TxSym != nil {
		symbols = append(symbols, *binding.TxSym)
		symbolCount++
	}

	// Pre-allocate tuples slice with exact capacity
	tuples := make([]Tuple, 0, len(datoms))

	// Build tuples directly without iterator overhead
	for _, datom := range datoms {
		// Pre-allocate tuple with exact size
		tuple := make(Tuple, 0, symbolCount)

		if binding.EntitySym != nil {
			tuple = append(tuple, datom.E)
		}
		if binding.AttributeSym != nil {
			tuple = append(tuple, datom.A)
		}
		if binding.ValueSym != nil {
			tuple = append(tuple, datom.V)
		}
		if binding.TxSym != nil {
			tuple = append(tuple, datom.Tx)
		}

		tuples = append(tuples, tuple)
	}

	return NewMaterializedRelation(symbols, tuples)
}
