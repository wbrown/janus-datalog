package executor

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// thetaJoinWithPredicate performs a nested-loop join with a predicate filter.
// This replaces Product() + filterWithPredicateAndLookup for multi-relation predicates,
// avoiding the StreamingRelation.Iterator() panic by using BufferedIterator for the inner.
func thetaJoinWithPredicate(relevantRels []Relation, pred query.Predicate, lookup query.EntityLookup, constants map[query.Symbol]interface{}, opts ExecutorOptions) Relation {
	if len(relevantRels) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, opts)
	}
	if len(relevantRels) == 1 {
		return filterWithPredicateAndLookup(relevantRels[0], pred, lookup, constants)
	}

	// For 2 relations: streaming nested-loop theta-join
	if len(relevantRels) == 2 {
		return thetaJoinPair(relevantRels[0], relevantRels[1], pred, lookup, constants, opts)
	}

	// For 3+ relations: iteratively pair-wise theta-join
	// Buffer all inner relations, join the first two, then join result with third, etc.
	result := thetaJoinPair(relevantRels[0], relevantRels[1], nil, nil, constants, opts)
	for i := 2; i < len(relevantRels); i++ {
		result = thetaJoinPair(result, relevantRels[i], nil, nil, constants, opts)
	}
	// Apply predicate filter on final combined result
	return filterWithPredicateAndLookup(result, pred, lookup, constants)
}

// thetaJoinPair performs a nested-loop join between two relations with optional predicate.
// The outer relation streams; the inner is buffered for re-iteration via BufferedIterator.
func thetaJoinPair(outer, inner Relation, pred query.Predicate, lookup query.EntityLookup, constants map[query.Symbol]interface{}, opts ExecutorOptions) Relation {
	outerSyms := outer.Symbols()
	innerSyms := inner.Symbols()
	combinedSyms := make([]query.Symbol, 0, len(outerSyms)+len(innerSyms))
	combinedSyms = append(combinedSyms, outerSyms...)
	combinedSyms = append(combinedSyms, innerSyms...)

	// Buffer the inner relation for re-iteration
	innerBuf := NewBufferedIterator(inner.Iterator())

	bindings := make(map[query.Symbol]interface{}, len(combinedSyms)+len(constants))

	// Check if predicate needs database lookup
	dbFuncPred, isDbFuncPred := pred.(*query.DatabaseFunctionPredicate)

	var filtered []Tuple
	var joinErr error

	outerIt := outer.Iterator()
	firstOuter := true
outerLoop:
	for outerIt.Next() {
		outerTuple := outerIt.Tuple()

		// Reset inner for each outer tuple (first pass streams, subsequent re-read buffer)
		if !firstOuter {
			innerBuf.Reset()
		}
		firstOuter = false

		for innerBuf.Next() {
			innerTuple := innerBuf.Tuple()

			// Combine tuples
			combined := make(Tuple, len(outerTuple)+len(innerTuple))
			copy(combined, outerTuple)
			copy(combined[len(outerTuple):], innerTuple)

			if pred != nil {
				// Populate bindings
				for k := range bindings {
					delete(bindings, k)
				}
				for sym, val := range constants {
					bindings[sym] = val
				}
				bindTuple(bindings, outerSyms, outerTuple)
				bindTuple(bindings, innerSyms, innerTuple)

				// Evaluate predicate
				var passes bool
				var err error
				if isDbFuncPred && lookup != nil {
					passes, err = dbFuncPred.EvalWithLookup(bindings, lookup)
				} else {
					passes, err = pred.Eval(bindings)
				}
				if err != nil {
					// Fail fast — predicate eval errors are real errors, not
					// "treat as false." Surface to the consumer; do not
					// silently drop the pair.
					joinErr = err
					break outerLoop
				}
				if !passes {
					continue
				}
			}

			filtered = append(filtered, combined)
		}
	}
	// Deferred scan errors: a failed outer or inner iteration must not be
	// presented as a completed join. First error wins.
	if err := outerIt.Error(); joinErr == nil {
		joinErr = err
	}
	if err := innerBuf.Error(); joinErr == nil {
		joinErr = err
	}
	if closeErr := outerIt.Close(); joinErr == nil {
		joinErr = closeErr
	}
	if closeErr := innerBuf.Close(); joinErr == nil {
		joinErr = closeErr
	}

	result := NewMaterializedRelationWithOptions(combinedSyms, filtered, opts)
	if joinErr != nil {
		result.err = joinErr
	}
	return result
}

// crossJoinWithExpression performs a nested-loop cross-join between multiple relations,
// then evaluates an expression on each combined tuple.
// This replaces Product() + evaluateExpressionWithLookup for multi-relation expressions,
// avoiding the StreamingRelation.Iterator() panic.
func crossJoinWithExpression(relevantRels []Relation, expr *query.Expression, lookup query.EntityLookup, constants map[query.Symbol]interface{}, opts ExecutorOptions) Relation {
	if len(relevantRels) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, opts)
	}
	if len(relevantRels) == 1 {
		return evaluateExpressionWithLookup(relevantRels[0], expr, lookup, constants)
	}

	// Cross-join all relations first (no predicate filter)
	var joined Relation
	if len(relevantRels) == 2 {
		joined = thetaJoinPair(relevantRels[0], relevantRels[1], nil, nil, constants, opts)
	} else {
		// Iteratively pair-wise cross-join
		joined = thetaJoinPair(relevantRels[0], relevantRels[1], nil, nil, constants, opts)
		for i := 2; i < len(relevantRels); i++ {
			joined = thetaJoinPair(joined, relevantRels[i], nil, nil, constants, opts)
		}
	}

	// Evaluate expression on the combined relation
	return evaluateExpressionWithLookup(joined, expr, lookup, constants)
}
