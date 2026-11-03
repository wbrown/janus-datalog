package planner

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// SourceAnswer is one question the get-else scan rewrite asked while a plan
// was made: whether a match of Attr on Source answers, per entity, the one
// value the source's lookup answers, and the answer it got.
type SourceAnswer struct {
	Source query.Symbol
	Attr   datalog.Keyword
	Answer bool
}

// matchAnswersLookup is the options' answer for source and attr: their
// MatchAnswersLookup's, and false for every source when they carry none.
func (opts PlannerOptions) matchAnswersLookup(source query.Symbol, attr datalog.Keyword) bool {
	return opts.MatchAnswersLookup != nil && opts.MatchAnswersLookup(source, attr)
}

// answersAlike reports whether the options answer each recorded question as
// it was answered when the plan was made.
func (opts PlannerOptions) answersAlike(recorded []SourceAnswer) bool {
	for _, question := range recorded {
		if opts.matchAnswersLookup(question.Source, question.Attr) != question.Answer {
			return false
		}
	}
	return true
}

// askedAnswers is the questions one planning call's get-else rewrites ask,
// each answered by the planner's options and recorded with its answer.
type askedAnswers struct {
	options PlannerOptions
	asked   []SourceAnswer
}

// answer is the MatchAnswersLookup a planning call hands its passes: the
// options' answer, recorded.
func (a *askedAnswers) answer(source query.Symbol, attr datalog.Keyword) bool {
	answer := a.options.matchAnswersLookup(source, attr)
	a.asked = append(a.asked, SourceAnswer{Source: source, Attr: attr, Answer: answer})
	return answer
}

// evaluationAnswers is the recorded questions of every body plan the
// evaluations embed, each stratum's plain, delta and lower bodies.
func evaluationAnswers(evaluations map[*query.RuleInvocation]*InvocationEvaluation) []SourceAnswer {
	var recorded []SourceAnswer
	for _, evaluation := range evaluations {
		for _, stratum := range evaluation.Strata {
			for _, kind := range [][]PlannedBody{stratum.Bodies.Plain, stratum.Bodies.Delta, stratum.Bodies.Lower} {
				for _, body := range kind {
					recorded = append(recorded, body.Plan.SourceAnswers...)
				}
			}
		}
	}
	return recorded
}
