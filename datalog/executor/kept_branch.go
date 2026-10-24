package executor

import (
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
// stored-side by the branch tuple's positions of the same symbols. A key the
// result does not hold is no key, and a result holding none of them is
// grouped under one key, so every subject tuple probes the whole result: the
// shape of a branch reading nothing.
func newCachedBranch(branchSyms, outerSyms, keySyms []query.Symbol) *cachedBranch {
	var bIdx, oIdx []int
	for _, ksym := range keySyms {
		oi := query.SymbolIndex(outerSyms, ksym)
		if oi < 0 {
			continue
		}
		if bi := query.SymbolIndex(branchSyms, ksym); bi >= 0 {
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
// symbol the substitution does not name, a constant argument's, is dropped
// by the projection. An answer the substitution names nothing of, a branch
// form's, is returned as it is.
func (f framing) present(answer Relation) (Relation, error) {
	symbols := answer.Symbols()
	// The names the answer's positions carry, one entry per name in position
	// order: a position's presented name, the one the substitution gives its
	// symbol, then each further name the substitution gives it. A symbol the
	// substitution does not name carries none.
	var names []query.Symbol
	var at []int       // the answer position carrying the entry's name
	var further []bool // the entry is a further name, not the position's presented name
	presentedAt := make([]query.Symbol, len(symbols))
	for i, sym := range symbols {
		to, ok := f.subst.Of(sym)
		if !ok {
			continue
		}
		presentedAt[i] = to
		names = append(names, presentedAt[i])
		at = append(at, i)
		further = append(further, false)
		for _, name := range f.subst.Further(sym) {
			names = append(names, name)
			at = append(at, i)
			further = append(further, true)
		}
	}
	// A name's first entry carries it: a presented name keeps its symbol,
	// renamed to the name unless the symbol is a member, and a further name
	// is copied from its position's presented name. The positions of all of
	// a name's entries are unified.
	_, groups := query.SymbolGroups(names)
	var keep, renamed []query.Symbol
	var equated [][]int          // per name standing for several positions, those positions
	var copied [][2]query.Symbol // a presented name and a further name copied from it
	for _, group := range groups {
		first := group[0]
		carrier := at[first]
		if further[first] {
			copied = append(copied, [2]query.Symbol{presentedAt[carrier], names[first]})
		} else {
			keep = append(keep, symbols[carrier])
			renamed = append(renamed, symbols[carrier])
		}
		if len(group) > 1 {
			positions := make([]int, len(group))
			for k, entry := range group {
				positions[k] = at[entry]
			}
			equated = append(equated, positions)
		}
	}
	if len(keep) == 0 {
		return answer, nil
	}
	presented := answer
	if len(equated) > 0 {
		presented = presented.Select(func(t Tuple) bool { return query.AgreesAt(t, equated) })
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
