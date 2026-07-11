package storage

import (
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// unboundScanProperties reports only guarantees directly proven by the chosen
// physical index and CRDT mode. Unsupported shapes deliberately return no
// properties rather than inferring from planner intent.
func unboundScanProperties(
	pattern *query.DataPattern,
	index IndexType,
	cardinality schema.Cardinality,
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
			Variable:  entity.Name,
			Direction: query.OrderAsc,
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
			Variable:  entity.Name,
			Direction: query.OrderAsc,
		}},
		Keys: [][]query.Symbol{{entity.Name}},
	}
}

func historyATEVProperties(
	q *query.Query,
	pattern *query.DataPattern,
	history bool,
) (executor.RelationProperties, bool) {
	if !history || q == nil || q.Limit == nil || len(q.OrderBy) < 1 || len(q.OrderBy) > 2 {
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
	if q.OrderBy[0] != (query.OrderByClause{Variable: tx.Name, Direction: query.OrderDesc}) {
		return executor.RelationProperties{}, false
	}
	if len(q.OrderBy) == 2 &&
		q.OrderBy[1] != (query.OrderByClause{Variable: entity.Name, Direction: query.OrderAsc}) {
		return executor.RelationProperties{}, false
	}

	return executor.RelationProperties{
		Ordering: append([]query.OrderByClause(nil), q.OrderBy...),
	}, true
}
