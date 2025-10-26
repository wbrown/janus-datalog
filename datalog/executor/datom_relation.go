package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// DatomIterator works with datoms from query results
type DatomIterator struct {
	datoms  []datalog.Datom
	pos     int
	columns []query.Symbol
	pattern PatternBinding
	current Tuple
}

func NewDatomIterator(datoms []datalog.Datom, binding PatternBinding) *DatomIterator {
	// Build columns from binding
	var columns []query.Symbol
	if binding.EntitySym != nil {
		columns = append(columns, *binding.EntitySym)
	}
	if binding.AttributeSym != nil {
		columns = append(columns, *binding.AttributeSym)
	}
	if binding.ValueSym != nil {
		columns = append(columns, *binding.ValueSym)
	}
	if binding.TxSym != nil {
		columns = append(columns, *binding.TxSym)
	}

	return &DatomIterator{
		datoms:  datoms,
		pos:     -1,
		columns: columns,
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

// NewDatomRelation creates a relation from datoms
func NewDatomRelation(datoms []datalog.Datom, binding PatternBinding) Relation {
	// Build columns from binding
	var columns []query.Symbol
	columnCount := 0
	if binding.EntitySym != nil {
		columns = append(columns, *binding.EntitySym)
		columnCount++
	}
	if binding.AttributeSym != nil {
		columns = append(columns, *binding.AttributeSym)
		columnCount++
	}
	if binding.ValueSym != nil {
		columns = append(columns, *binding.ValueSym)
		columnCount++
	}
	if binding.TxSym != nil {
		columns = append(columns, *binding.TxSym)
		columnCount++
	}

	// Pre-allocate tuples slice with exact capacity
	tuples := make([]Tuple, 0, len(datoms))

	// Build tuples directly without iterator overhead
	for _, datom := range datoms {
		// Pre-allocate tuple with exact size
		tuple := make(Tuple, 0, columnCount)

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

	return NewMaterializedRelation(columns, tuples)
}
