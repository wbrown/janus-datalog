package parser

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/internal/queryfuzz"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Query.String is a query's identity: the text of a parsed query, of the
// prepared query and of every phase the planner emits parses back to the
// same text and the same structure (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md
// §6). The queries are queryfuzz's, drawn at random over the whole grammar,
// so a renderer that drops a value's type or a clause's brackets is caught.

// roundTrip renders q, parses the rendering and reports how the result
// differs from q: the rendering does not parse, renders differently, or
// parses to another structure. It is "" when the rendering reads back as q.
func roundTrip(q *query.Query) string {
	first := q.String()
	again, err := ParseQuery(first)
	if err != nil {
		return fmt.Sprintf("the rendering does not parse: %v\n    rendering: %s", err, first)
	}
	if second := again.String(); second != first {
		return fmt.Sprintf("the rendering changes on reparse\n    first:  %s\n    second: %s", first, second)
	}
	if !reflect.DeepEqual(q, again) {
		return fmt.Sprintf("the structure changes on reparse\n    rendering: %s", first)
	}
	return ""
}

// boundByInputs is the symbols a query's :in entries bind, which the planner
// plans under as execution does.
func boundByInputs(q *query.Query) map[query.Symbol]bool {
	bound := map[query.Symbol]bool{}
	for _, in := range query.BindingInputs(q.In) {
		switch in := in.(type) {
		case query.ScalarInput:
			bound[in.Symbol] = true
		case query.CollectionInput:
			bound[in.Symbol] = true
		case query.TupleInput:
			for _, s := range in.Symbols {
				bound[s] = true
			}
		case query.RelationInput:
			for _, s := range in.Symbols {
				bound[s] = true
			}
		}
	}
	return bound
}

// roundTripFailures collects the failures of one class of round trip, keyed
// by the first line of the failure's description, each with the texts that
// produced it.
type roundTripFailures map[string][]string

func (f roundTripFailures) add(text, failure string) {
	head := strings.SplitN(failure, "\n", 2)[0]
	f[head] = append(f[head], text+"\n    "+failure)
}

// report writes every class of failure under label: its count and its first
// three instances.
func (f roundTripFailures) report(t *testing.T, label string) {
	t.Helper()
	heads := make([]string, 0, len(f))
	for head := range f {
		heads = append(heads, head)
	}
	sort.Strings(heads)
	for _, head := range heads {
		instances := f[head]
		shown := instances
		if len(shown) > 3 {
			shown = shown[:3]
		}
		t.Errorf("%s: %d queries, %s\n  %s", label, len(instances), head, strings.Join(shown, "\n  "))
	}
}

// Every query the parser accepts renders to text the parser reads back as
// the same query, as does the prepared query and every phase the planner
// emits for it under either optimizer path, planned under the symbols its
// inputs bind; and the two optimizer paths accept and refuse the same
// prepared query. A failure names its class, its count and its first
// instances.
func TestQueryStringRoundTripsRandomQueries(t *testing.T) {
	rules, err := ParseRules(queryfuzz.Rules)
	if err != nil {
		t.Fatalf("ParseRules: %v", err)
	}
	planners := map[bool]*planner.ClauseBasedPlanner{}
	for _, algebra := range []bool{true, false} {
		planners[algebra] = planner.NewClauseBasedPlanner(nil, planner.PlannerOptions{EnableAlgebraOptimizer: algebra})
	}
	parsed, prepared := roundTripFailures{}, roundTripFailures{}
	phases := map[bool]roundTripFailures{true: {}, false: {}}
	verdicts := roundTripFailures{}
	accepted := 0
	for _, seed := range queryfuzz.Seeds {
		d := queryfuzz.NewDraw(seed)
		for i := 0; i < queryfuzz.PerSeed; i++ {
			text := d.QueryText()
			q, err := ParseQuery(text)
			if err != nil {
				continue
			}
			accepted++
			if failure := roundTrip(q); failure != "" {
				parsed.add(text, failure)
			}
			var ruleSet *query.RuleSet
			for _, in := range q.In {
				if _, ok := in.(query.RulesInput); ok {
					ruleSet = rules
				}
			}
			prep, err := query.Prepare(q, ruleSet)
			if err != nil {
				continue
			}
			if failure := roundTrip(prep); failure != "" {
				prepared.add(text, failure)
			}
			bindings := query.Bindings{Symbols: boundByInputs(q), Rules: ruleSet}
			planErr := map[bool]error{}
			for _, algebra := range []bool{true, false} {
				plan, err := planners[algebra].Plan(prep, bindings, nil)
				planErr[algebra] = err
				if err != nil {
					continue
				}
				for pi, phase := range plan.Phases {
					if failure := roundTrip(phase.Query); failure != "" {
						phases[algebra].add(text, fmt.Sprintf("phase %d: %s", pi, failure))
					}
				}
			}
			if (planErr[true] == nil) != (planErr[false] == nil) {
				verdicts.add(text, fmt.Sprintf("the optimizer paths disagree\n    algebra on:  %v\n    algebra off: %v", planErr[true], planErr[false]))
			}
		}
	}
	if accepted == 0 {
		t.Fatal("the parser accepted no drawn query")
	}
	parsed.report(t, "parsed query")
	prepared.report(t, "prepared query")
	phases[true].report(t, "planned phase, algebra on")
	phases[false].report(t, "planned phase, algebra off")
	verdicts.report(t, "planner verdict")
}
