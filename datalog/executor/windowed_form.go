package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The composition of a windowed form
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.3): a window of the
// subject is read, each frame's run over it is computed, each branch is built
// over its run, and each window tuple is answered by probing the built
// branches. The window reader, the frames, the kept branch and the
// continuations are the modules every form shares; a form supplies its
// framing and its policy.

// framing is how a form frames its branches over its subject, the one value
// the composition reads: the form's name, which the module events report
// under; the subject; the header's required run and its find beyond it, the
// outputs as variables for a branch form and the inner :find for a subquery;
// what a branch is given; the run symbols, the given symbols ordered against
// the subject; the index's key, the header symbols the subject carries that
// a built branch is probed by, in the subject's order; the frames'
// environment relation, the values the frames read for the given symbols the
// subject lacks; the substitution of a subquery's call, what each symbol of
// the frames and of the answer stands for in the enclosing scope, read once
// from the call's text, the inner :in symbols against the arguments and the
// inner :find elements against the binding, under which a frame symbol is
// found in the subject by the symbol standing for it and the answer is
// presented under the enclosing names, a branch form's symbols standing for
// themselves; the members, the given symbols bound to one member per element
// of their argument's value, a subquery's collection inputs; and whether a
// branch holding no invocation is first planned over the required run
// alone, the union's check that the language's scope admits the branch
// before its seek binds the carried outputs into it.
type framing struct {
	form             string
	subject          Relation
	required         []query.Symbol
	find             []query.FindElement
	given            []query.Symbol
	runSyms          []query.Symbol
	key              []query.Symbol
	environment      Relation
	subst            query.Substitution
	members          []query.Symbol
	planOverRequired bool
}

// storedKey is the symbol a presented answer holds each key symbol under:
// the key symbol itself, or, for a collection argument's, the member symbol
// standing for it, which the presentation keeps under its own name.
func (f framing) storedKey() []query.Symbol {
	if len(f.members) == 0 {
		return f.key
	}
	stored := make([]query.Symbol, len(f.key))
	for i, sym := range f.key {
		stored[i] = sym
		for _, member := range f.members {
			if to, ok := f.subst.Of(member); ok && to == sym {
				stored[i] = member
				break
			}
		}
	}
	return stored
}

// frameBranches frames each branch as a scope under the header and plans it
// once. A branch holding an invocation is framed over the required run, so
// the invocation is seeded by the header; a branch reading nothing of what
// it is given is framed over the required symbols the environment alone
// binds, so its one run carries the input's values; every other branch is
// framed over the run symbols. The find is the given symbols the branch
// provides beyond its frame, as variables, and the header's find. A plan's
// refusal is the form's, naming the branch counted from one.
func frameBranches(executor *DefaultQueryExecutor, ctx *Context, branches [][]query.Clause, f framing) ([]branchRun, error) {
	// The required symbols the subject lacks, each found in it by the symbol
	// it stands for, which the environment alone binds: the planner refuses
	// a required run nothing binds.
	outerSyms := f.subject.Symbols()
	var fromEnvironment []query.Symbol
	for _, sym := range f.required {
		standsFor := sym
		if to, ok := f.subst.Of(sym); ok {
			standsFor = to
		}
		if !query.ContainsSymbol(outerSyms, standsFor) {
			fromEnvironment = append(fromEnvironment, sym)
		}
	}
	plans := make([]branchRun, len(branches))
	for i, branch := range branches {
		var run branchRun
		switch {
		case holdsInvocation(branch):
			// The required run alone, in the run symbols' order, so every
			// keyed branch of a fallback is framed alike and the keys one
			// branch answers are read at the same positions by the next.
			for _, sym := range f.runSyms {
				if query.ContainsSymbol(f.required, sym) {
					run.frame.required = append(run.frame.required, sym)
				}
			}
		case len(branchReads(branch, f.given, ctx.Rules())) == 0:
			run.frame.required = fromEnvironment
			run.readsNothing = true
		default:
			if f.planOverRequired {
				if _, err := executor.planScope(ctx, scope{required: f.required, find: f.find, clauses: branch}); err != nil {
					return nil, fmt.Errorf("branch %d: %w", i+1, err)
				}
			}
			run.frame.required = f.runSyms
		}
		for _, sym := range branchProvides(branch, ctx.Rules()) {
			if query.ContainsSymbol(f.given, sym) && !query.ContainsSymbol(run.frame.required, sym) {
				run.frame.find = append(run.frame.find, query.FindVariable{Symbol: sym})
			}
		}
		outputs := extractFindSymbols(f.find)
		for position, elem := range f.find {
			if sym := outputs[position]; !query.ContainsSymbol(run.frame.required, sym) && !query.ContainsSymbol(run.frame.outputs(), sym) {
				run.frame.find = append(run.frame.find, elem)
			}
		}
		run.frame.clauses = branch
		plan, err := executor.planScope(ctx, run.frame)
		if err != nil {
			return nil, fmt.Errorf("branch %d: %w", i+1, err)
		}
		run.plan = plan
		plans[i] = run
	}
	return plans, nil
}

// formPolicy is a form's own rules over the composition. narrow is the run
// a branch is built over, from its frame's run: the fallback leaves out the
// keys the earlier branches answered; nil takes the run as it is. build is a
// build of a branch's tuples in place of its plan over the run: the
// fallback's from the EA cache; nil builds from the plan, as does a build
// answering with no symbols. built is told of each build: the fallback
// records the keys it answered; nil tells nothing. answer answers the
// window's tuples in turn from the built branches, setting the current
// tuple, and reports false when the window is answered.
type formPolicy struct {
	narrow func(run *MaterializedRelation, branchIdx int) *MaterializedRelation
	build  func(branchIdx int, run *MaterializedRelation) ([]Tuple, []query.Symbol, error)
	built  func(branchIdx int, branchSyms []query.Symbol, tuples []Tuple)
	answer func() bool
}

// windowedIterator is the composition's iterator: it reads the subject in
// windows through the reader, runs each branch once per window over its
// frame's run, keeps each branch's index across the windows, and answers
// each window tuple under the form's policy.
type windowedIterator struct {
	executor *DefaultQueryExecutor
	ctx      *Context
	framing  framing
	plans    []branchRun
	options  ExecutorOptions

	// reader reads the subject into the windows the branches are run over
	// and probed for; windowPos is the tuple of its window being answered.
	reader    *windowReader
	windowPos int

	// frames is each distinct frame of the plans resolved against the
	// subject and the environment, holding the keys its windows have run and
	// the window's run; frameOf is each branch's frame by index into frames.
	frames  []windowFrame
	frameOf []int

	// The branches' indexes, kept across the windows and grown by each
	// window's build. Key: branch index. A branch no window has built yet
	// has none and answers nothing. See ALGEBRA.md "OR-Fallback Branch
	// Caching" for specification.
	branchCache map[int]*cachedBranch

	// continued is the evaluations the invocations under this iterator
	// continue across its windows: the enclosing form execution's, shared
	// through the context, or this iterator's own, which ownsContinued
	// records and the iterator completes at the subject's exhaustion and at
	// Close.
	continued     *ruleContinuations
	ownsContinued bool

	policy       formPolicy
	currentTuple Tuple
	done         bool
	err          error
}

// continuationsFor is the context a windowed form's iterator runs under and
// the continuations its branches' invocations continue across the windows:
// the enclosing form execution's, shared through the context, or new ones
// the iterator owns, reported by the third result, which it completes at
// the subject's exhaustion and at Close. The ownership stays with the
// outermost windowed form's iterator, and a nested form's iterator finds the
// context's and shares them.
func continuationsFor(ctx *Context) (*Context, *ruleContinuations, bool) {
	if ctx.continued != nil {
		return ctx, ctx.continued, false
	}
	continued := newRuleContinuations()
	return ctx.WithContinuations(continued), continued, true
}

// newWindowedIterator composes the modules over the framing's subject for
// its form: the frames of plans resolved against the subject and the
// framing's environment relation, the continuations, and the reader over
// the subject's iterator, counting the window event's distinct keys over
// the subject's positions the required run's symbols stand for.
func newWindowedIterator(executor *DefaultQueryExecutor, ctx *Context, f framing, plans []branchRun, options ExecutorOptions) windowedIterator {
	it := windowedIterator{executor: executor, ctx: ctx, framing: f, plans: plans, options: options}
	it.frames, it.frameOf = windowFrames(plans, f)
	it.ctx, it.continued, it.ownsContinued = continuationsFor(ctx)
	outerSyms := f.subject.Symbols()
	var keyPos []int
	for _, sym := range f.required {
		standsFor := sym
		if to, ok := f.subst.Of(sym); ok {
			standsFor = to
		}
		pos := query.SymbolIndex(outerSyms, standsFor)
		if pos < 0 {
			continue
		}
		known := false
		for _, seen := range keyPos {
			if seen == pos {
				known = true
				break
			}
		}
		if !known {
			keyPos = append(keyPos, pos)
		}
	}
	it.reader = newWindowReader(f.subject.Iterator(), f.form, f.subject.RequiresCopy(), keyPos, options)
	return it
}

// Next answers the next tuple from the window in hand under the form's
// policy, and reads the next window when the one in hand is answered,
// ending at the subject's end or on a failure.
func (it *windowedIterator) Next() bool {
	for {
		if it.done {
			return false
		}
		if it.policy.answer() {
			return true
		}
		if !it.nextWindow() {
			return false
		}
	}
}

// nextWindow reads the next window of the subject through the reader and
// runs each branch over it. It returns false at the subject's end, completing
// an owned continuation, and on a failure of the subject or of a branch,
// with it.err set; either way the iterator is done.
func (it *windowedIterator) nextWindow() bool {
	if !it.reader.next() {
		it.err = it.reader.err
		if it.err == nil && it.ownsContinued {
			it.continued.complete()
		}
		it.done = true
		return false
	}
	it.windowPos = 0
	if !it.runWindow() {
		it.done = true
		return false
	}
	return true
}

// runWindow runs each branch over the window: a branch framed over no
// subject symbol once, at the first window, over the environment's tuple or
// the unit relation; every other branch over the window's keys its frame
// has not run, narrowed under the form's policy. A branch whose run holds no
// key is not run. Returns false with it.err set when a build fails.
func (it *windowedIterator) runWindow() bool {
	for branchIdx := range it.plans {
		frame := &it.frames[it.frameOf[branchIdx]]
		if len(frame.subjectPos) == 0 {
			if it.reader.ordinal == 1 && !it.buildBranch(branchIdx, frame.environmentRun(it.options)) {
				return false
			}
			continue
		}
		run := frame.windowRun(it.reader.window, it.reader.ordinal, it.options)
		if it.policy.narrow != nil {
			run = it.policy.narrow(run, branchIdx)
		}
		if run.Size() == 0 {
			continue
		}
		if !it.buildBranch(branchIdx, run) {
			return false
		}
	}
	return true
}

// buildBranch builds a branch over run, the window's keys for it: through
// the policy's build when it has one and answers with symbols, otherwise
// through the kept branch's build from the branch's plan. It adds the
// result to the branch's index, created at the first build and kept across
// the windows, reports the build and tells the policy of it. Returns false
// with it.err set when the build fails.
func (it *windowedIterator) buildBranch(branchIdx int, run *MaterializedRelation) bool {
	var tuples []Tuple
	var branchSyms []query.Symbol
	var err error
	if it.policy.build != nil {
		tuples, branchSyms, err = it.policy.build(branchIdx, run)
	}
	if err == nil && branchSyms == nil {
		tuples, branchSyms, err = buildBranchTuples(it.executor, it.ctx, it.framing, it.plans[branchIdx], run)
	}
	if err != nil {
		it.err = err
		return false
	}
	it.reportCacheBuild(branchIdx, branchSyms, len(tuples))
	cb, built := it.branchCache[branchIdx]
	if !built {
		cb = newCachedBranch(branchSyms, it.framing.subject.Symbols(), it.framing.key, it.framing.storedKey())
		it.cache(branchIdx, cb)
	}
	for _, tuple := range tuples {
		cb.Add(tuple)
	}
	if it.policy.built != nil {
		it.policy.built(branchIdx, branchSyms, tuples)
	}
	return true
}

// reportCacheBuild emits the build event for a branch's window build, under
// the form.
func (it *windowedIterator) reportCacheBuild(branchIdx int, branchSyms []query.Symbol, size int) {
	if it.options.Handler == nil {
		return
	}
	it.options.Handler(annotations.Event{
		Name: annotations.ScopeBranchBuild,
		Data: map[string]interface{}{
			annotations.KeyForm: it.framing.form,
			"branch":            branchIdx,
			"branch_syms":       branchSyms,
			"outer_syms":        it.framing.subject.Symbols(),
			"branch_size":       size,
		},
	})
}

// cache records a built branch for probing.
func (it *windowedIterator) cache(branchIdx int, cb *cachedBranch) {
	if it.branchCache == nil {
		it.branchCache = make(map[int]*cachedBranch)
	}
	it.branchCache[branchIdx] = cb
}

func (it *windowedIterator) Tuple() Tuple {
	return it.currentTuple
}

// Close closes the subject's iterator and completes an owned continuation.
func (it *windowedIterator) Close() error {
	var closeErr error
	if it.reader != nil {
		closeErr = it.reader.close()
	}
	if it.ownsContinued {
		it.continued.complete()
	}
	it.done = true
	return closeErr
}

// Error returns any error encountered during iteration
func (it *windowedIterator) Error() error {
	return it.err
}
