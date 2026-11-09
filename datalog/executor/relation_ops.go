package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// materializeRelationsForPattern materializes relations that share symbols with a pattern.
// This is needed for binding-based filtering during pattern matching.
func materializeRelationsForPattern(pattern *query.DataPattern, relations Relations) Relations {
	// Extract pattern symbols - what variables does this pattern bind?
	patternSymbols := pattern.Symbols()
	if len(patternSymbols) == 0 {
		// No variables in pattern - no bindings needed
		return relations
	}

	// Build set for fast lookup
	patternSymbolSet := make(map[query.Symbol]bool)
	for _, sym := range patternSymbols {
		patternSymbolSet[sym] = true
	}

	// Materialize relations that share symbols with the pattern
	result := make(Relations, len(relations))
	for i, rel := range relations {
		hasSharedSymbol := false
		for _, sym := range rel.Symbols() {
			if patternSymbolSet[sym] {
				hasSharedSymbol = true
				break
			}
		}

		if hasSharedSymbol {
			// This relation shares symbols with the pattern - materialize it
			// It will be used for binding-based filtering AND joining
			result[i] = rel.Materialize()
		} else {
			// No shared symbols - keep as-is (pure streaming)
			result[i] = rel
		}
	}

	return result
}

// filterWithPredicateAndLookup filters a relation using a predicate.
// evaluation is a database-function predicate's evaluation through its
// source's lookup, nil for a predicate its own Eval decides.
// env is the query scope's environment relation (single-valued :in
// parameters); its one tuple binds into every tuple's evaluation — the join of
// the environment into the operator, with the per-tuple bindings map as
// operator-internal scratch.
//
// A filter whose result can fail as it is read, the predicate failing on a
// value of the relation (canFailOnAValueOver) or the relation evaluating as
// it is read, is a stream carrying the evaluating mark. Its failure is the
// stream's error, raised where a reader that sees every other clause reads it
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, the failure contract).
//
// Any other filter is eager: the scan completes before returning, so every
// error — source iteration, close — is knowable synchronously and returns
// in-band. The deferred-error convention is exclusively for
// lazily-discovered errors on streaming relations; deferring a known error
// here left it to consumers that inspect relations structurally (emptiness
// branches) and laundered it into a silent empty
// (BUG_MISSING_ON_LOOKUPLESS_MATCHER_SILENTLY_EMPTY).
func filterWithPredicateAndLookup(rel Relation, pred query.Predicate, evaluation query.PredicateEvaluation, env Relation) (result Relation, resultErr error) {
	if properties := rel.Properties(); properties.Evaluates || canFailOnAValueOver(pred, properties.Identities) {
		properties = properties.clone()
		properties.Evaluates = true
		filter := NewPredicateFilterIterator(rel.Iterator(), rel.Symbols(), pred, evaluation, env)
		return NewStreamingRelationWithProperties(rel.Symbols(), filter, rel.Options(), properties), nil
	}

	symbols := rel.Symbols()
	needsCopy := rel.RequiresCopy()
	envSymbols, envTuple := environmentTuple(env)

	// Pre-allocate filtered only for materialized relations to avoid forcing materialization
	var filtered []Tuple
	if _, ok := rel.(*MaterializedRelation); ok {
		if size := rel.Size(); size >= 0 {
			filtered = make([]Tuple, 0, size)
		}
	}

	// Reuse single bindings map to avoid repeated allocations
	bindings := make(map[query.Symbol]interface{}, len(symbols)+len(envSymbols))

	// Named return + closure so the deferred Close() also runs on panic
	// (predicate Eval is user-supplied code and can panic), without losing
	// the Close error.
	iter := rel.Iterator()
	defer func() {
		resultErr = errjoin.Join(resultErr, iter.Close())
	}()

	for iter.Next() {
		tuple := iter.Tuple()

		// Clear and populate bindings map
		for k := range bindings {
			delete(bindings, k)
		}
		// Pre-populate from the environment's tuple (a relation symbol of
		// the same name overwrites below — relation data wins locally)
		for i := range envSymbols {
			bindings[envSymbols[i]] = envTuple[i]
		}
		bindTuple(bindings, symbols, tuple)

		// Evaluate the predicate
		var passes bool
		var err error
		if evaluation != nil {
			passes, err = evaluation(bindings)
		} else {
			passes, err = pred.Eval(bindings)
		}
		if err != nil {
			// Fail fast — predicate eval errors are real errors, not
			// "treat as false."
			return nil, err
		}

		if passes {
			if needsCopy {
				tuple = copyTuple(tuple)
			}
			filtered = append(filtered, tuple)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	// Extract options from source relation to preserve configuration
	opts := rel.Options()
	return NewMaterializedRelationWithProperties(symbols, filtered, opts, rel.Properties()), nil
}

// evaluateExpressionWithLookup answers rel joined with what expr's binding
// reads from its value on each tuple, as a stream that evaluates as it is
// pulled: a tuple nothing pulls is never evaluated, and an evaluation failure
// surfaces when the result is driven. The evaluation it is given is a
// database function's evaluation through its source's lookup, and is nil for
// a function its own Eval computes. The environment env is the query scope's
// relation of single-valued :in parameters; its one tuple binds into every
// tuple's evaluation.
func evaluateExpressionWithLookup(rel Relation, expr *query.Expression, evaluation query.Evaluation, env Relation) Relation {
	evaluator := NewFunctionEvaluatorIterator(rel.Iterator(), rel.Symbols(), expr, evaluation, env)

	// Only extension symbols touch the properties: bound positions are
	// filtered, never rewritten, so orderings and keys naming them stay
	// valid. The expanding forms read any number of tuples per input tuple,
	// so the input's keys do not survive them; the scalar and tuple forms
	// read one tuple by construction.
	properties := rel.Properties()
	if evaluator.expands {
		properties = expansionProperties(properties, expr.BoundVariables(), evaluator.Symbols())
	} else {
		for _, symbol := range evaluator.align.extensionSymbols() {
			properties = properties.addSymbol(symbol)
		}
	}
	// The stream evaluates the expression as it is pulled, which can fail on
	// a value unless the expression fails on no value over rel's identity
	// symbols (expressionFailsOnNoValueOver).
	properties = properties.clone()
	properties.Evaluates = properties.Evaluates || !expressionFailsOnNoValueOver(expr, rel.Properties().Identities)
	return NewStreamingRelationWithProperties(evaluator.Symbols(), evaluator, rel.Options(), properties)
}

// getUniqueCombinations extracts unique value combinations for the given symbols.
func getUniqueCombinations(rel Relation, syms []query.Symbol) (combos []Tuple, resultErr error) {
	if rel == nil || len(syms) == 0 {
		return nil, nil
	}

	symIndices, err := query.SymbolIndexTable(rel.Symbols(), syms)
	if err != nil {
		return nil, fmt.Errorf("unique combinations: %w", err)
	}

	// Each combination is held in the set under its whole-tuple key, probed
	// by the source tuple at the symbols' positions, so a combination held is
	// rejected before it is built (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T4).
	seen := NewTupleSet(symIndices, allPositions(len(symIndices)), 0)
	iter := rel.Iterator()
	defer func() {
		resultErr = errjoin.Join(resultErr, iter.Close())
	}()
	for iter.Next() {
		tuple := iter.Tuple()
		_, hash, held := seen.Find(tuple)
		if held {
			continue
		}
		combo := make(Tuple, len(symIndices))
		for i, idx := range symIndices {
			combo[i] = tuple[idx]
		}
		seen.Add(combo, hash)
		combos = append(combos, combo)
	}
	if e := iter.Error(); resultErr == nil {
		resultErr = e
	}
	return combos, resultErr
}

// countOverlap counts how many symbols from targetSyms are present in refSyms
func countOverlap(refSyms, targetSyms []query.Symbol) int {
	symSet := make(map[query.Symbol]bool, len(refSyms))
	for _, sym := range refSyms {
		symSet[sym] = true
	}
	count := 0
	for _, sym := range targetSyms {
		if symSet[sym] {
			count++
		}
	}
	return count
}

// unionRelations creates a union of multiple relations, projecting to common symbols
func unionRelations(relations []Relation, syms []query.Symbol, opts ExecutorOptions) Relation {
	if len(relations) == 0 {
		return NewMaterializedRelationWithOptions(syms, nil, opts)
	}

	// Each projected tuple is held in the set under its whole-tuple key,
	// probed by each relation's tuple at that relation's positions of the
	// symbols, so a tuple held is rejected before it is projected
	// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T4).
	positions := allPositions(len(syms))
	seen := NewTupleSet(positions, positions, 0)
	var allTuples []Tuple
	var firstErr error

	for _, rel := range relations {
		symIndices, err := query.SymbolIndexTable(rel.Symbols(), syms)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("union: %w", err)
			}
			continue
		}

		iter := rel.Iterator()
		for iter.Next() {
			tuple := iter.Tuple()
			_, hash, held := seen.FindAt(tuple, symIndices)
			if held {
				continue
			}
			projected := make(Tuple, len(syms))
			for i, idx := range symIndices {
				projected[i] = tuple[idx]
			}
			seen.Add(projected, hash)
			allTuples = append(allTuples, projected)
		}
		// Capture a branch's deferred scan error so the union doesn't drop it.
		branchErr := errjoin.Join(iter.Error(), iter.Close())
		if branchErr != nil && firstErr == nil {
			firstErr = branchErr
		}
	}

	result := newMaterializedRelationFromSet(
		syms,
		allTuples,
		opts,
		deduplicatedProperties(syms),
	)
	result.err = firstErr
	return result
}

// equalSymbols checks if two symbol slices are equal
func equalSymbols(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
