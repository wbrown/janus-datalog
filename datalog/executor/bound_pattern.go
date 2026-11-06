package executor

import (
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A data pattern over a streaming group composed over the modules
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2, §4.6, §4.8): the
// pattern is one body under the header of the variables it shares with the
// group, built once per window over the window's distinct keys less those an
// earlier window ran, each key sought by the matcher, and probed per window
// tuple. The result is the subject's tuples each joined with the pattern's
// matches, over the subject's symbols and the pattern's other variables; it
// stands in the consumed groups' place, reports no size, and reads the
// subject once.

// streamsThrough reports whether a group sharing one of the symbols, a
// pattern's or an invocation's variables, reports no size: such a group is
// read once, in windows, where a group held in memory binds the pattern
// through the matcher or seeds the invocation through its projection.
func streamsThrough(symbols []query.Symbol, groups Relations) bool {
	for _, group := range groups {
		if containsAny(group.Symbols(), symbols) && group.Size() < 0 {
			return true
		}
	}
	return false
}

// executeBoundPattern runs the pattern as one body over the window of its
// subject, the groups holding a variable it names joined into one, under
// the header of the variables the subject carries, with the pattern's other
// variables as the outputs, framed through planner.WindowFraming as the
// planner's walk frames it. The result's keys survive when the pattern
// answers at most one tuple per subject tuple, a cardinality-one attribute
// keyed by a required entity, as a fallback's do. Its identity symbols are
// the subject's and the pattern's entity variable (AnswerIdentities).
func (e *DefaultQueryExecutor) executeBoundPattern(ctx *Context, pattern *query.DataPattern, groups Relations) (Relations, error) {
	symbols := pattern.Symbols()
	subject, consumed := e.findOuterRelation(symbols, groups)
	outerSyms := subject.Symbols()
	framed := planner.WindowFraming(symbols, outerSyms)
	required, find := framed.Required, framed.Find
	outputs := make([]query.Symbol, len(find))
	for i, elem := range find {
		outputs[i] = elem.(query.FindVariable).Symbol
	}
	resultSyms := append(append([]query.Symbol(nil), outerSyms...), outputs...)
	carriedSet := make(map[query.Symbol]bool, len(required))
	for _, sym := range required {
		carriedSet[sym] = true
	}
	atMostOne := patternAnswersAtMostOne(pattern, required, carriedSet, e.matcher)
	properties := orProperties(subject.Properties(), outputs, nil, resultSyms, atMostOne)
	for _, symbol := range AnswerIdentities(pattern) {
		if !query.ContainsSymbol(properties.Identities, symbol) {
			properties.Identities = append(properties.Identities, symbol)
		}
	}

	f := framing{
		form:      "pattern",
		subject:   subject,
		required:  required,
		find:      find,
		given:     required,
		standsFor: required,
		key:       keySymbols(required, outerSyms),
	}
	plans, err := frameBranches(e, ctx, [][]query.Clause{{pattern}}, f)
	if err != nil {
		return nil, err
	}
	it := &subqueryIterator{
		windowedIterator: newWindowedIterator(e, ctx, f, plans, e.options),
		matchEmit:        matchEmit{outerSyms: outerSyms, outputSyms: resultSyms},
	}
	it.policy = formPolicy{answer: it.answer}
	ownFind := make([]query.FindElement, len(symbols))
	for i, sym := range symbols {
		ownFind[i] = query.FindVariable{Symbol: sym}
	}
	join := &patternJoinIterator{
		matcher: e.matcher,
		own:     &query.Query{Find: ownFind, Where: []query.Clause{pattern}},
		join:    it,
	}
	result := NewStreamingRelationWithProperties(resultSyms, join, e.options, properties)
	return replaceConsumedGroups(groups, consumed, result, e.options.Handler), nil
}

// patternJoinIterator answers the windowed join of a subject with a pattern.
// At its first pull it reads whether the pattern has a match, the join's
// other side: every match under the subject's bindings is one of the
// pattern's own, so where it has none the join is empty, and the iterator
// answers nothing and reads nothing of the subject
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T5). The read runs as the
// result is pulled, so its scan failure surfaces when the result is driven,
// as the join's do.
type patternJoinIterator struct {
	matcher PatternMatcher
	own     *query.Query // the pattern alone, finding its variables
	join    Iterator     // the windowed join
	checked bool
	empty   bool
	err     error
}

// Next reads the pattern's own matches at the first pull, then answers the
// join's next tuple where they hold one.
func (it *patternJoinIterator) Next() bool {
	if !it.checked {
		it.checked = true
		own, err := it.matcher.Match(it.own, nil)
		if err != nil {
			it.err = err
			return false
		}
		has, readErr, closeErr := hasTuple(own)
		if err := errjoin.Join(readErr, closeErr); err != nil {
			it.err = err
			return false
		}
		it.empty = !has
	}
	if it.err != nil || it.empty {
		return false
	}
	return it.join.Next()
}

// failedTuple answers the windowed join's failed tuple, when the join answers
// one (failedTupleIterator), so a reader of the pattern's result decides it at
// its key as a reader of the join would.
func (it *patternJoinIterator) failedTuple() (Tuple, []int, bool) {
	failing, ok := it.join.(failedTupleIterator)
	if !ok {
		return nil, nil, false
	}
	return failing.failedTuple()
}

// resume resumes the windowed join past the tuple failedTuple answered.
func (it *patternJoinIterator) resume() {
	it.join.(failedTupleIterator).resume()
}

// Tuple returns the join's current tuple.
func (it *patternJoinIterator) Tuple() Tuple {
	return it.join.Tuple()
}

// RequiresCopy is the join's copy answer.
func (it *patternJoinIterator) RequiresCopy() bool {
	return it.join.RequiresCopy()
}

// Error returns the failure reading the pattern's own matches, else the
// join's.
func (it *patternJoinIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.join.Error()
}

// Close closes the join.
func (it *patternJoinIterator) Close() error {
	return it.join.Close()
}
