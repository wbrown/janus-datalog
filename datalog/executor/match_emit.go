package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The emit the forms with outputs share
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.3): a probe's matches,
// filtered to those agreeing with the subject tuple on the carried outputs,
// projected onto the result's symbols with the symbols a branch lacks filled
// from the subject tuple, and drained one tuple at a time. It reads nothing
// of the form.

// matchEmit is the shared emit's state: the subject's symbols and the
// result's, the carried outputs, the projection memoized per branch, and the
// probe's matches being drained with the plan and the subject tuple they are
// projected under.
type matchEmit struct {
	outerSyms  []query.Symbol
	outputSyms []query.Symbol
	carried    []carriedOutput

	// branchPlans memoizes each branch's output projection, keyed by branch
	// index with the branch's result symbols as the plan's cache key. Per
	// branch the symbols are fixed for the iterator's lifetime (same clauses,
	// same input shape), so the projection is planned once.
	branchPlans map[int]branchPlan

	// The drain: a probe's matches being emitted, a key's group of a branch's
	// index, which is already a set.
	matches  []Tuple
	matchPos int
	plan     projectionPlan
	outer    Tuple
}

// carriedOutput is an output the subject or the environment binds, with
// where its value is read per subject tuple: the subject tuple's position,
// or the environment's value when the environment alone binds it.
type carriedOutput struct {
	symbol   query.Symbol
	outerPos int
	envValue interface{}
}

// agreeing filters a probe's matches to the tuples agreeing with the
// subject tuple on the carried outputs, allocating only when one disagrees.
// With no carried output every match agrees.
func (e *matchEmit) agreeing(matches []Tuple, branchSyms []query.Symbol, outerTuple Tuple) []Tuple {
	if len(e.carried) == 0 {
		return matches
	}
	kept := matches
	filtered := false
	for i, match := range matches {
		if e.agrees(match, branchSyms, outerTuple) {
			if filtered {
				kept = append(kept, match)
			}
			continue
		}
		if !filtered {
			kept = append([]Tuple(nil), matches[:i]...)
			filtered = true
		}
	}
	return kept
}

// agrees reports whether a branch tuple carries the subject tuple's value
// at every carried output it carries.
func (e *matchEmit) agrees(branchTuple Tuple, branchSyms []query.Symbol, outerTuple Tuple) bool {
	for _, output := range e.carried {
		bi := query.SymbolIndex(branchSyms, output.symbol)
		if bi < 0 {
			continue
		}
		want := output.envValue
		if output.outerPos >= 0 {
			want = outerTuple[output.outerPos]
		}
		if !datalog.ValuesEqual(branchTuple[bi], want) {
			return false
		}
	}
	return true
}

// projectionPlan precomputes, per output position, where a branch tuple's
// value comes from: a branch-tuple position, else an outer-tuple position
// (a branch such as a ground default doesn't produce every output symbol —
// the outer tuple's values fill the gaps), else nothing. The mapping depends
// only on the branch, output, and outer symbol lists — per-branch constants —
// so one plan serves every tuple the branch emits. identity means the branch
// symbols already match the output symbols exactly and tuples pass through
// unprojected; the emit sites own the copy-vs-alias decision for that case.
// A symbol both sides carry reads the branch's value, which the carried
// output filter has already matched to the outer tuple's.
type projectionPlan struct {
	identity  bool
	branchPos []int // per output position; -1 = not in the branch tuple
	outerPos  []int // per output position; -1 = not in the outer tuple
}

func newProjectionPlan(srcSyms, dstSyms, outerSyms []query.Symbol) projectionPlan {
	if symbolsMatch(srcSyms, dstSyms) {
		return projectionPlan{identity: true}
	}
	branchPos := make([]int, len(dstSyms))
	outerPos := make([]int, len(dstSyms))
	for i, sym := range dstSyms {
		branchPos[i] = query.SymbolIndex(srcSyms, sym)
		outerPos[i] = query.SymbolIndex(outerSyms, sym)
	}
	return projectionPlan{branchPos: branchPos, outerPos: outerPos}
}

// project maps a branch tuple to the output symbols, filling positions the
// branch doesn't produce from the outer tuple. Not called on identity plans.
func (p projectionPlan) project(tuple, outerTuple Tuple) Tuple {
	result := make(Tuple, len(p.branchPos))
	for i, bp := range p.branchPos {
		if bp >= 0 {
			result[i] = tuple[bp]
			continue
		}
		if op := p.outerPos[i]; op >= 0 && op < len(outerTuple) {
			result[i] = outerTuple[op]
		}
	}
	return result
}

// branchPlan is a memoized projectionPlan together with the branch symbols
// it was computed from — the symbols are the memo's validity key.
type branchPlan struct {
	srcSyms []query.Symbol
	plan    projectionPlan
}

// projectionFor returns the memoized output projection for a branch,
// computing and storing it on first sight of the branch's result symbols.
func (e *matchEmit) projectionFor(branchIdx int, branchSyms []query.Symbol) projectionPlan {
	if bp, ok := e.branchPlans[branchIdx]; ok && symbolsMatch(bp.srcSyms, branchSyms) {
		return bp.plan
	}
	plan := newProjectionPlan(branchSyms, e.outputSyms, e.outerSyms)
	if e.branchPlans == nil {
		e.branchPlans = make(map[int]branchPlan)
	}
	e.branchPlans[branchIdx] = branchPlan{srcSyms: branchSyms, plan: plan}
	return plan
}

// begin starts draining a probe's matches for a branch under the subject
// tuple, and returns the first match projected onto the result's symbols;
// false with no match. The matches are a key's group of a branch result that
// is already a set, so nothing re-deduplicates here, and an identity-shaped
// tuple aliases the index's.
func (e *matchEmit) begin(branchIdx int, branchSyms []query.Symbol, matches []Tuple, outerTuple Tuple) (Tuple, bool) {
	if len(matches) == 0 {
		return nil, false
	}
	e.plan = e.projectionFor(branchIdx, branchSyms)
	e.matches = matches
	e.matchPos = 1
	e.outer = outerTuple
	if e.plan.identity {
		return matches[0], true
	}
	return e.plan.project(matches[0], outerTuple), true
}

// next is the drain's next match projected onto the result's symbols; false
// when the drain is empty, which ends it.
func (e *matchEmit) next() (Tuple, bool) {
	if e.matchPos >= len(e.matches) {
		e.matches = nil
		e.outer = nil
		return nil, false
	}
	match := e.matches[e.matchPos]
	e.matchPos++
	if e.plan.identity {
		return match, true
	}
	return e.plan.project(match, e.outer), true
}
