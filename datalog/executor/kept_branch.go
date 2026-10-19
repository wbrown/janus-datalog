package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// The kept branch of a windowed form (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md
// §4.3, §4.6): a branch's index over its result symbols, keyed on the header
// symbols the subject carries, built from the branch's plan over a window's
// run and read whole, grown by every window's build and probed per window
// tuple. It reads nothing of the form.

// cachedBranch is a branch's index, kept across the windows: a JoinIndex
// keyed probe-side by the subject tuple's key positions and stored-side by
// the branch tuple's, carrying the branch's result symbols for output
// projection.
type cachedBranch struct {
	*JoinIndex
	branchSyms []query.Symbol
}

// newCachedBranch is an empty index over a branch's result symbols, keyed
// probe-side by the subject tuple's positions of the key symbols and
// stored-side by the branch tuple's positions of the symbols the result
// holds each key under, storedSyms, the key symbol itself for all but a
// collection argument's, which the result holds under the member's inner
// symbol. A key the result does not hold is no key, and a result holding
// none of them is grouped under one key, so every subject tuple probes the
// whole result: the shape of a branch reading nothing.
func newCachedBranch(branchSyms, outerSyms, keySyms, storedSyms []query.Symbol) *cachedBranch {
	var bIdx, oIdx []int
	for i, ksym := range keySyms {
		oi := query.SymbolIndex(outerSyms, ksym)
		if oi < 0 {
			continue
		}
		if bi := query.SymbolIndex(branchSyms, storedSyms[i]); bi >= 0 {
			oIdx = append(oIdx, oi)
			bIdx = append(bIdx, bi)
		}
	}
	return &cachedBranch{
		JoinIndex:  NewJoinIndex(oIdx, bIdx, 0),
		branchSyms: branchSyms,
	}
}

// buildBranchTuples runs a branch's plan over run, the unit relation when
// run is nil, presents the answer under the enclosing names when the
// framing carries a substitution, and reads it whole: the branch's tuples,
// with the presented answer's symbols.
func buildBranchTuples(executor *DefaultQueryExecutor, ctx *Context, f framing, branch branchRun, run *MaterializedRelation) ([]Tuple, []query.Symbol, error) {
	var input Relation = NewUnitRelation(executor.options)
	if run != nil {
		input = run
	}
	result, err := executor.runPlan(ctx, branch.plan, input)
	if err != nil {
		return nil, nil, err
	}
	presented, err := f.present(result)
	if err != nil {
		return nil, nil, err
	}
	tuples, err := collectBranch(presented)
	if err != nil {
		return nil, nil, err
	}
	return tuples, presented.Symbols(), nil
}

// present is a branch's answer under the enclosing names: the answer
// projected onto the first of its symbols standing for each enclosing name
// and renamed through the substitution. A collection member's symbol stands
// for its argument at the member's grain and for no enclosing name, so it is
// kept under its own name, the name the kept branch holds a member's key
// under. A symbol the substitution does not name, a constant argument's, and
// a later symbol standing for a name an earlier one stands for, a repeated
// argument's, whose value the run bound equal to the first's, are dropped by
// the projection. An answer the substitution names nothing of, a branch
// form's, is returned as it is.
func (f framing) present(answer Relation) (Relation, error) {
	symbols := answer.Symbols()
	var keep, renamed, named []query.Symbol
	for _, sym := range symbols {
		if query.ContainsSymbol(f.members, sym) {
			keep = append(keep, sym)
			continue
		}
		to, ok := f.subst.Of(sym)
		if !ok || query.ContainsSymbol(named, to) {
			continue
		}
		keep = append(keep, sym)
		renamed = append(renamed, sym)
		named = append(named, to)
	}
	if len(keep) == 0 {
		return answer, nil
	}
	projected := answer
	if !symbolsMatch(keep, symbols) {
		var err error
		projected, err = answer.Project(keep)
		if err != nil {
			return nil, err
		}
	}
	return projected.Rename(f.subst.Over(renamed))
}

// collectBranch reads a branch's result whole. The index retains the tuples
// for the query's lifetime, so a tuple a source reusing its workspace
// yields is copied.
func collectBranch(branchResult Relation) ([]Tuple, error) {
	needsCopy := branchResult.RequiresCopy()
	var collected []Tuple
	if n := branchResult.Size(); n >= 0 {
		collected = make([]Tuple, 0, n)
	}
	iter := branchResult.Iterator()
	for iter.Next() {
		t := iter.Tuple()
		if needsCopy {
			t = copyTuple(t)
		}
		collected = append(collected, t)
	}
	iterErr := iter.Error()
	if closeErr := iter.Close(); iterErr == nil {
		iterErr = closeErr
	}
	if iterErr != nil {
		return nil, iterErr
	}
	return collected, nil
}
