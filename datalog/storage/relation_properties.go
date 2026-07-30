package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// scanProjectionPreservesSet reports whether a pattern scan's datom→tuple
// projection is injective over the stream the scan emits — whether the
// resulting relation is a set with no restoration pass. The emitted stream's
// candidate key depends on the mode:
//
//   - History mode emits raw operation records. Every datom carries its own
//     ElementID (Transaction.Add draws clock.Next() per datom), so Tx alone
//     is a candidate key: a covered Tx position makes any projection
//     injective. Without Tx, re-assertions of the same (E, A, V) at later
//     transactions project to identical tuples.
//   - Current/as-of resolution emits one tuple per (E, A) group for effective
//     cardinality-one — including CardinalityUnknown/schemaless, which the
//     resolver defaults to LWW (CRDTResolvingIterator.startNewGroup) — and
//     for cardinality-vector (one resolved vector per group). Declared
//     CardinalityMany emits one tuple per (E, A, V). With A not constant the
//     cardinality varies per attribute, so the conservative key is the
//     superkey {E, A, V}.
//
// A key component is covered when the pattern binds it to a constant (it
// does not vary across the stream) or a variable (it appears in the tuple).
// A wildcard — or an absent Tx position — drops a varying key component,
// letting two distinct stream tuples project to the same tuple.
func scanProjectionPreservesSet(pattern *query.DataPattern, provider schema.SchemaProvider, history bool) bool {
	covered := func(elem query.PatternElement) bool {
		return elem != nil && !elem.IsBlank()
	}

	if history {
		return covered(pattern.GetT())
	}

	if !covered(pattern.GetE()) || !covered(pattern.GetA()) {
		return false
	}

	keyNeedsV := true
	if aConst, ok := pattern.GetA().(query.Constant); ok {
		keyNeedsV = false
		if provider != nil {
			if aKw, ok := aConst.Value.(datalog.Keyword); ok {
				if def := provider.GetAttribute(aKw); def != nil && def.Cardinality == schema.CardinalityMany {
					keyNeedsV = true
				}
			}
		}
	}
	if keyNeedsV && !covered(pattern.GetV()) {
		return false
	}
	return true
}

// unboundScanProperties reports only guarantees directly proven by the chosen
// physical index and CRDT mode. Unsupported shapes deliberately return no
// properties rather than inferring from planner intent.
func unboundScanProperties(
	pattern *query.DataPattern,
	index IndexType,
	cardinality datalog.Keyword,
	history bool,
) executor.RelationProperties {
	if history || index != AETV || cardinality != schema.CardinalityOne {
		return executor.RelationProperties{}
	}
	if _, ok := pattern.GetA().(query.Constant); !ok {
		return executor.RelationProperties{}
	}
	entity, ok := pattern.GetE().(query.Variable)
	if !ok {
		return executor.RelationProperties{}
	}

	// AETV is [A][E][Tx↓][V]. With A constant, current/as-of
	// CardinalityOne resolution emits at most one tuple per E in E order.
	return executor.RelationProperties{
		Ordering: []query.OrderByClause{{
			Variable: entity.Name,
		}},
		Keys: [][]query.Symbol{{entity.Name}},
	}
}

func validatedVBoundProperties(
	pattern *query.DataPattern,
	strategy ReuseStrategy,
) executor.RelationProperties {
	if !strategy.NeedsValidation || strategy.Index != AVET {
		return executor.RelationProperties{}
	}
	if _, ok := pattern.GetA().(query.Constant); !ok {
		return executor.RelationProperties{}
	}
	if _, ok := pattern.GetV().(query.Constant); !ok {
		return executor.RelationProperties{}
	}
	entity, ok := pattern.GetE().(query.Variable)
	if !ok {
		return executor.RelationProperties{}
	}

	// AVET is [A][V][E][Tx↓]. With A and V constant, candidate validation
	// preserves E order and emits at most one current winner per E.
	return executor.RelationProperties{
		Ordering: []query.OrderByClause{{
			Variable: entity.Name,
		}},
		Keys: [][]query.Symbol{{entity.Name}},
	}
}

func historyATEVProperties(
	q *query.Query,
	pattern *query.DataPattern,
	history bool,
) (executor.RelationProperties, bool) {
	if !history || q.Limit == nil || len(q.OrderBy) < 1 || len(q.OrderBy) > 2 {
		return executor.RelationProperties{}, false
	}
	if _, ok := pattern.GetA().(query.Constant); !ok {
		return executor.RelationProperties{}, false
	}
	entity, entityOK := pattern.GetE().(query.Variable)
	tx, txOK := pattern.GetT().(query.Variable)
	if !entityOK || !txOK {
		return executor.RelationProperties{}, false
	}
	if q.OrderBy[0] != (query.OrderByClause{Variable: tx.Name, Descending: true}) {
		return executor.RelationProperties{}, false
	}
	if len(q.OrderBy) == 2 &&
		q.OrderBy[1] != (query.OrderByClause{Variable: entity.Name}) {
		return executor.RelationProperties{}, false
	}

	return executor.RelationProperties{
		Ordering: append([]query.OrderByClause(nil), q.OrderBy...),
	}, true
}

func historyTAEVProperties(
	q *query.Query,
	pattern *query.DataPattern,
	history bool,
) (executor.RelationProperties, bool) {
	if !history || q.Limit == nil || len(q.OrderBy) < 1 || len(q.OrderBy) > 3 {
		return executor.RelationProperties{}, false
	}
	entity, entityOK := pattern.GetE().(query.Variable)
	attribute, attributeOK := pattern.GetA().(query.Variable)
	_, valueOK := pattern.GetV().(query.Variable)
	tx, txOK := pattern.GetT().(query.Variable)
	if !entityOK || !attributeOK || !valueOK || !txOK {
		return executor.RelationProperties{}, false
	}

	physicalOrder := []query.OrderByClause{
		{Variable: tx.Name, Descending: true},
		{Variable: attribute.Name},
		{Variable: entity.Name},
	}
	for i, order := range q.OrderBy {
		if order != physicalOrder[i] {
			return executor.RelationProperties{}, false
		}
	}

	// TAEV is [Tx↓][A][E][V]. ElementIDs identify individual operations, so
	// Tx descending is already a total order; A and E are valid optional
	// physical tie-break prefixes.
	return executor.RelationProperties{
		Ordering: append([]query.OrderByClause(nil), q.OrderBy...),
	}, true
}

func historyAETVProperties(
	q *query.Query,
	pattern *query.DataPattern,
	history bool,
) (executor.RelationProperties, bool) {
	if !history || q.Limit == nil || len(q.OrderBy) != 2 {
		return executor.RelationProperties{}, false
	}
	if _, ok := pattern.GetA().(query.Constant); !ok {
		return executor.RelationProperties{}, false
	}
	entity, entityOK := pattern.GetE().(query.Variable)
	_, valueOK := pattern.GetV().(query.Variable)
	tx, txOK := pattern.GetT().(query.Variable)
	if !entityOK || !valueOK || !txOK {
		return executor.RelationProperties{}, false
	}

	if q.OrderBy[0] != (query.OrderByClause{Variable: entity.Name}) ||
		q.OrderBy[1] != (query.OrderByClause{Variable: tx.Name, Descending: true}) {
		return executor.RelationProperties{}, false
	}

	// AETV is [A][E][Tx↓][V]. With A constant and no value filter, raw
	// history tuples are emitted in the requested total E/Tx order.
	return executor.RelationProperties{
		Ordering: append([]query.OrderByClause(nil), q.OrderBy...),
	}, true
}

func historyEATVProperties(
	q *query.Query,
	pattern *query.DataPattern,
	history bool,
) (executor.RelationProperties, bool) {
	if !history || q.Limit == nil || len(q.OrderBy) != 2 {
		return executor.RelationProperties{}, false
	}
	if _, ok := pattern.GetE().(query.Constant); !ok {
		return executor.RelationProperties{}, false
	}
	attribute, attributeOK := pattern.GetA().(query.Variable)
	_, valueOK := pattern.GetV().(query.Variable)
	tx, txOK := pattern.GetT().(query.Variable)
	if !attributeOK || !valueOK || !txOK {
		return executor.RelationProperties{}, false
	}

	if q.OrderBy[0] != (query.OrderByClause{Variable: attribute.Name}) ||
		q.OrderBy[1] != (query.OrderByClause{Variable: tx.Name, Descending: true}) {
		return executor.RelationProperties{}, false
	}

	// EATV is [E][A][Tx↓][V]. With E constant and no value filter, raw
	// history tuples are emitted in the requested total A/Tx order.
	return executor.RelationProperties{
		Ordering: append([]query.OrderByClause(nil), q.OrderBy...),
	}, true
}
