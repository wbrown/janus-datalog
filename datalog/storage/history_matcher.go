package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// HistoryMatcher implements PatternMatcher for querying history indices.
// It reads from the history index set which contains all assertions and retractions
// with Op (operation) included. This enables full audit trail queries.
//
// History patterns can have 5 elements: [?e ?a ?v ?tx ?op]
// When ?op is bound, results are filtered by that value.
// When ?op is unbound, it's included in the result relation.
type HistoryMatcher struct {
	store   *BadgerStore
	txID    uint64 // For as-of on history (0 = all history)
	options executor.ExecutorOptions
}

// NewHistoryMatcher creates a new history pattern matcher
func NewHistoryMatcher(store *BadgerStore) *HistoryMatcher {
	return &HistoryMatcher{store: store}
}

// NewHistoryMatcherWithOptions creates a new history pattern matcher with options
func NewHistoryMatcherWithOptions(store *BadgerStore, opts executor.ExecutorOptions) *HistoryMatcher {
	return &HistoryMatcher{store: store, options: opts}
}

// AsOf creates a history matcher limited to transactions up to txID
func (m *HistoryMatcher) AsOf(txID uint64) *HistoryMatcher {
	return &HistoryMatcher{store: m.store, txID: txID, options: m.options}
}

var _ executor.PatternMatcher = (*HistoryMatcher)(nil)

// Match implements PatternMatcher.Match for history queries
func (m *HistoryMatcher) Match(pattern *query.DataPattern, bindings executor.Relations) (executor.Relation, error) {
	columns := pattern.Symbols()

	// Extract constraints from pattern
	e, a, v, tx, op := m.extractPatternValues(pattern)

	// Choose history index based on bound values (same logic as current-state)
	index := m.chooseHistoryIndex(e, a, v, tx)

	// Build prefix for scan range
	start, end := m.buildScanRange(index, e, a, v, tx)

	// Scan history index using specialized history iterator
	histKeyIter, err := NewHistoryKeyOnlyIterator(m.store, index, start, end)
	if err != nil {
		return nil, fmt.Errorf("history scan failed: %w", err)
	}

	// Create iterator that filters and builds tuples
	histIter := &historyIterator{
		matcher:  m,
		index:    index,
		histIter: histKeyIter,
		pattern:  pattern,
		columns:  columns,
		e:        e,
		a:        a,
		v:        v,
		tx:       tx,
		op:       op,
	}

	return executor.NewStreamingRelationWithOptions(columns, histIter, m.options), nil
}

func (m *HistoryMatcher) extractPatternValues(pattern *query.DataPattern) (e, a, v, tx, op interface{}) {
	extract := func(elem query.PatternElement) interface{} {
		if c, ok := elem.(query.Constant); ok {
			return c.Value
		}
		return nil
	}
	if elem := pattern.GetE(); elem != nil {
		e = extract(elem)
	}
	if elem := pattern.GetA(); elem != nil {
		a = extract(elem)
	}
	if elem := pattern.GetV(); elem != nil {
		v = extract(elem)
	}
	if elem := pattern.GetT(); elem != nil {
		tx = extract(elem)
	}
	if elem := pattern.GetOp(); elem != nil {
		op = extract(elem)
	}
	return
}

func (m *HistoryMatcher) chooseHistoryIndex(e, a, v, tx interface{}) IndexType {
	if e != nil {
		return EAVT_HISTORY
	} else if a != nil {
		if v != nil {
			return AVET_HISTORY
		}
		return AEVT_HISTORY
	} else if v != nil {
		return VAET_HISTORY
	} else if tx != nil {
		return TAEV_HISTORY
	}
	return EAVT_HISTORY
}

func (m *HistoryMatcher) buildScanRange(index IndexType, e, a, v, tx interface{}) ([]byte, []byte) {
	encoder := m.store.encoder

	// Build prefix parts based on what's bound
	var parts [][]byte

	switch index {
	case EAVT_HISTORY:
		if eId, ok := e.(datalog.Identity); ok {
			parts = append(parts, eId.Bytes()[:])
		}
	case AEVT_HISTORY, AVET_HISTORY:
		if aKw, ok := a.(datalog.Keyword); ok {
			aStorage := ToStorageDatom(datalog.Datom{A: datalog.NewKeyword(aKw.String())}).A
			parts = append(parts, aStorage[:])
		}
	case VAET_HISTORY:
		if v != nil {
			sDatom := ToStorageDatom(datalog.Datom{V: v})
			vType := byte(datalog.Type(sDatom.V))
			vData := datalog.ValueBytes(sDatom.V)
			parts = append(parts, append([]byte{vType}, vData...))
		}
	case TAEV_HISTORY:
		if txID, ok := tx.(uint64); ok {
			storageTx := NewTxFromUint(txID)
			parts = append(parts, storageTx[:])
		}
	}

	return encoder.EncodePrefixRange(index, parts...)
}

// historyIterator iterates over history index results
type historyIterator struct {
	matcher         *HistoryMatcher
	index           IndexType
	histIter        *HistoryKeyOnlyIterator
	pattern         *query.DataPattern
	columns         []query.Symbol
	e, a, v, tx, op interface{}
	current         executor.Tuple
	done            bool
}

func (i *historyIterator) Next() bool {
	if i.done {
		return false
	}

	for i.histIter.Next() {
		datom, err := i.histIter.Datom()
		if err != nil {
			i.done = true
			return false
		}

		opBool := i.histIter.Op() == OpAssert

		// Filter by txID if as-of is set
		if i.matcher.txID > 0 && datom.Tx > i.matcher.txID {
			continue
		}

		// Filter by Op if bound
		if i.op != nil {
			if opConst, ok := i.op.(bool); ok && opBool != opConst {
				continue
			}
		}

		// Build tuple including Op
		i.current = query.HistoryDatomToTuple(*datom, opBool, i.pattern, i.columns)
		return true
	}

	i.done = true
	return false
}

func (i *historyIterator) Tuple() executor.Tuple { return i.current }
func (i *historyIterator) Close() error          { return i.histIter.Close() }
