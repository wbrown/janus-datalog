package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

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

// present is a branch's answer under the enclosing names. Each name the
// substitution gives stands for one attribute of the presented answer. The
// first of the answer's symbols standing for a name is renamed to it. Every
// later symbol standing for that name, a repeated argument's or a repeated
// binding name's, is unified with the first: the answer is selected on their
// equality, as an invocation's repeated argument selects, and the projection
// drops the later symbol. A symbol standing for several names is presented
// under the first and copied under each further one through the identity
// expression, since a rename gives an attribute one name; a further name the
// presented answer already carries is unified the same way, with no copy. A
// collection member's symbol stands for its argument at the member's grain
// and for no enclosing name, so it is kept under its own name, the name the
// kept branch holds a member's key under, and copied under its further
// names. A symbol the substitution does not name, a constant argument's, is
// dropped by the projection. An answer the substitution names nothing of, a
// branch form's, is returned as it is.
func (f framing) present(answer Relation) (Relation, error) {
	symbols := answer.Symbols()
	var keep, renamed []query.Symbol
	var carried []query.Symbol   // each name the presented answer carries
	var carriedAt []int          // the answer position presented under it
	var equated [][2]int         // answer positions one name stands for
	var copied [][2]query.Symbol // a presented name and a further name it is copied under
	for i, sym := range symbols {
		var presentedAs query.Symbol
		if query.ContainsSymbol(f.members, sym) {
			keep = append(keep, sym)
			presentedAs = sym
			carried = append(carried, sym)
			carriedAt = append(carriedAt, i)
		} else {
			to, ok := f.subst.Of(sym)
			if !ok {
				continue
			}
			presentedAs = to
			if j := query.SymbolIndex(carried, to); j >= 0 {
				equated = append(equated, [2]int{carriedAt[j], i})
			} else {
				keep = append(keep, sym)
				renamed = append(renamed, sym)
				carried = append(carried, to)
				carriedAt = append(carriedAt, i)
			}
		}
		for _, further := range f.subst.Further(sym) {
			if j := query.SymbolIndex(carried, further); j >= 0 {
				equated = append(equated, [2]int{carriedAt[j], i})
				continue
			}
			copied = append(copied, [2]query.Symbol{presentedAs, further})
			carried = append(carried, further)
			carriedAt = append(carriedAt, i)
		}
	}
	if len(keep) == 0 {
		return answer, nil
	}
	presented := answer
	if len(equated) > 0 {
		presented = presented.Select(func(t Tuple) bool {
			for _, pair := range equated {
				if !datalog.ValuesEqual(t[pair[0]], t[pair[1]]) {
					return false
				}
			}
			return true
		})
	}
	var err error
	if !symbolsMatch(keep, symbols) {
		if presented, err = presented.Project(keep); err != nil {
			return nil, err
		}
	}
	if presented, err = presented.Rename(f.subst.Over(renamed)); err != nil {
		return nil, err
	}
	for _, c := range copied {
		presented = presented.EvaluateFunction(query.IdentityFunction{Arg: query.VariableTerm{Symbol: c[0]}}, c[1])
	}
	return presented, nil
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
