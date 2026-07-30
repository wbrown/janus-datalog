package executor

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// thetaJoinWithPredicate performs a nested-loop join with a predicate filter.
// This replaces Product() + filterWithPredicateAndLookup for multi-relation predicates,
// avoiding the StreamingRelation.Iterator() panic by using BufferedIterator for the inner.
// env is the query scope's environment relation; see filterWithPredicateAndLookup.
//
// Eager: errors are knowable synchronously and return in-band.
func thetaJoinWithPredicate(relevantRels []Relation, pred query.Predicate, lookup query.EntityLookup, env Relation, opts ExecutorOptions) (Relation, error) {
	if len(relevantRels) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, opts), nil
	}
	if len(relevantRels) == 1 {
		return filterWithPredicateAndLookup(relevantRels[0], pred, lookup, env)
	}

	// For 2 relations: streaming nested-loop theta-join
	if len(relevantRels) == 2 {
		return thetaJoinPair(relevantRels[0], relevantRels[1], pred, lookup, env, opts)
	}

	// For 3+ relations: iteratively pair-wise theta-join
	// Buffer all inner relations, join the first two, then join result with third, etc.
	result, err := thetaJoinPair(relevantRels[0], relevantRels[1], nil, nil, env, opts)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(relevantRels); i++ {
		result, err = thetaJoinPair(result, relevantRels[i], nil, nil, env, opts)
		if err != nil {
			return nil, err
		}
	}
	// Apply predicate filter on final combined result
	return filterWithPredicateAndLookup(result, pred, lookup, env)
}

// thetaJoinPair performs a nested-loop join between two relations with optional predicate.
// The outer relation streams; the inner is buffered for re-iteration via BufferedIterator.
// env is the query scope's environment relation, bound into each pair's
// evaluation by reference.
//
// Eager: evaluation, iteration, and close errors are knowable synchronously
// and return in-band.
func thetaJoinPair(outer, inner Relation, pred query.Predicate, lookup query.EntityLookup, env Relation, opts ExecutorOptions) (Relation, error) {
	outerSyms := outer.Symbols()
	innerSyms := inner.Symbols()
	envSymbols, envTuple := environmentTuple(env)
	combinedSyms := make([]query.Symbol, 0, len(outerSyms)+len(innerSyms))
	combinedSyms = append(combinedSyms, outerSyms...)
	combinedSyms = append(combinedSyms, innerSyms...)

	// Buffer the inner relation for re-iteration
	innerBuf := NewBufferedIterator(inner.Iterator())

	bindings := make(map[query.Symbol]interface{}, len(combinedSyms)+len(envSymbols))

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
				// Populate bindings: the environment's tuple first (relation
				// data of the same name overwrites — it wins locally)
				for k := range bindings {
					delete(bindings, k)
				}
				for i := range envSymbols {
					bindings[envSymbols[i]] = envTuple[i]
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
	// A failed outer or inner iteration must not be presented as a
	// completed join. First error wins; all errors return in-band.
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
	if joinErr != nil {
		return nil, joinErr
	}

	return NewMaterializedRelationWithOptions(combinedSyms, filtered, opts), nil
}

// crossJoinWithExpression performs a nested-loop cross-join between multiple relations,
// then evaluates an expression on each combined tuple.
// This replaces Product() + evaluateExpressionWithLookup for multi-relation expressions,
// avoiding the StreamingRelation.Iterator() panic.
// env is the query scope's environment relation; see evaluateExpressionWithLookup.
//
// Eager: errors are knowable synchronously and return in-band.
func crossJoinWithExpression(relevantRels []Relation, expr *query.Expression, lookup query.EntityLookup, env Relation, opts ExecutorOptions) (Relation, error) {
	if len(relevantRels) == 0 {
		return NewMaterializedRelationWithOptions(nil, nil, opts), nil
	}
	if len(relevantRels) == 1 {
		return evaluateExpressionWithLookup(relevantRels[0], expr, lookup, env)
	}

	// Cross-join all relations first (no predicate filter)
	joined, err := thetaJoinPair(relevantRels[0], relevantRels[1], nil, nil, env, opts)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(relevantRels); i++ {
		joined, err = thetaJoinPair(joined, relevantRels[i], nil, nil, env, opts)
		if err != nil {
			return nil, err
		}
	}

	// Evaluate expression on the combined relation
	return evaluateExpressionWithLookup(joined, expr, lookup, env)
}
