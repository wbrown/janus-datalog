package executor

import "github.com/wbrown/janus-datalog/datalog/query"

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
// variables as the outputs. The result's keys survive when the pattern
// answers at most one tuple per subject tuple, a cardinality-one attribute
// keyed by a required entity, as a fallback's do.
func (e *DefaultQueryExecutor) executeBoundPattern(ctx *Context, pattern *query.DataPattern, groups Relations) (Relations, error) {
	symbols := pattern.Symbols()
	subject, consumed := e.findOuterRelation(symbols, groups)
	outerSyms := subject.Symbols()
	var required, outputs []query.Symbol
	for _, sym := range symbols {
		if query.ContainsSymbol(outerSyms, sym) {
			required = append(required, sym)
		} else {
			outputs = append(outputs, sym)
		}
	}
	find := make([]query.FindElement, len(outputs))
	for i, sym := range outputs {
		find[i] = query.FindVariable{Symbol: sym}
	}
	resultSyms := append(append([]query.Symbol(nil), outerSyms...), outputs...)
	carriedSet := make(map[query.Symbol]bool, len(required))
	for _, sym := range required {
		carriedSet[sym] = true
	}
	atMostOne := patternAnswersAtMostOne(pattern, required, carriedSet, e.matcher)
	properties := orProperties(subject.Properties(), outputs, nil, resultSyms, atMostOne)

	f := framing{
		form:     "pattern",
		subject:  subject,
		required: required,
		find:     find,
		given:    required,
		key:      keySymbols(required, outerSyms),
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
	result := NewStreamingRelationWithProperties(resultSyms, it, e.options, properties)
	return replaceConsumedGroups(groups, consumed, result, e.options.Handler), nil
}
