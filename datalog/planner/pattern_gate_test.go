package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A pattern reading a variable a pending expression binds runs after the
// expression when the expression can run before it, and before it when the
// expression waits, through the pending clauses binding what it needs, on a
// variable the pattern itself binds; the expression then unifies with the
// variable the pattern bound. Each case names the clause that leads and the
// clause that follows, on both planner paths.
func TestPatternLeadsTheExpressionThatWaitsOnIt(t *testing.T) {
	cases := []struct {
		name, text, leads, follows string
	}{
		{"an expression reading what the pattern binds",
			`[:find ?y :where [?e :p ?y] [(identity ?e) ?y]]`,
			`[?e :p ?y]`, `[(identity ?e) ?y]`},
		{"two patterns and two expressions in a cycle",
			`[:find ?a :where [?a :p ?y] [(identity ?b) ?y] [?b :q ?z] [(identity ?a) ?z]]`,
			`[?a :p ?y]`, `[(identity ?a) ?z]`},
		{"a subquery between the expression and the pattern",
			`[:find ?y :where [?e :p ?y] [(q [:find ?v :in $ ?e :where [?e :r ?v]] $ ?e) [[?v]]] [(identity ?v) ?y]]`,
			`[?e :p ?y]`, `[(identity ?v) ?y]`},
		{"a pattern held behind an expression waiting on another held pattern",
			`[:find ?item :where [?e :p ?y] [(identity ?e) ?y] [?item :color ?c] [(identity ?y) ?item]]`,
			`[(identity ?y) ?item]`, `[?item :color ?c]`},
		{"an expression that can run first",
			`[:find ?label :where [?c :container/items ?vec] [(enumerate ?vec) [?idx ?item]] [?item :item/color :red] [?item :item/label ?label]]`,
			`[(enumerate ?vec) [[?idx ?item]]]`, `[?item :item/color :red]`},
	}
	paths := []struct {
		name    string
		options PlannerOptions
	}{
		{"clause-based", PlannerOptions{}},
		{"algebra", PlannerOptions{EnableAlgebraOptimizer: true}},
	}
	for _, path := range paths {
		for _, c := range cases {
			t.Run(path.name+"/"+c.name, func(t *testing.T) {
				parsed, err := parser.ParseQuery(c.text)
				if err != nil {
					t.Fatalf("ParseQuery: %v", err)
				}
				prepared, err := query.Prepare(parsed, nil)
				if err != nil {
					t.Fatalf("Prepare: %v", err)
				}
				plan, err := NewClauseBasedPlanner(nil, path.options).PlanQuery(prepared, query.Bindings{}, nil)
				if err != nil {
					t.Fatalf("PlanQuery: %v", err)
				}
				position := func(rendered string) (int, int) {
					for i, phase := range plan.Phases {
						for j, clause := range phase.Query.Where {
							if clause.String() == rendered {
								return i, j
							}
						}
					}
					return -1, -1
				}
				leadsPhase, leadsIndex := position(c.leads)
				if leadsPhase < 0 {
					t.Fatalf("the plan carries no %s:\n%s", c.leads, plan)
				}
				followsPhase, followsIndex := position(c.follows)
				if followsPhase < 0 {
					t.Fatalf("the plan carries no %s:\n%s", c.follows, plan)
				}
				if followsPhase < leadsPhase || (followsPhase == leadsPhase && followsIndex < leadsIndex) {
					t.Errorf("%s runs at phase %d clause %d, before %s at phase %d clause %d:\n%s",
						c.follows, followsPhase, followsIndex, c.leads, leadsPhase, leadsIndex, plan)
				}
			})
		}
	}
}

// pendingBlockers closes a clause's blockers over the pending clauses: the
// symbols the clause waits on, what the pending clauses binding them wait on
// in turn, and, for a pending pattern among those, the unavailable variables
// pending expressions bind, which the pattern gate holds the pattern on.
func TestPendingBlockers(t *testing.T) {
	cases := []struct {
		name, text, of string
		waits          []string
	}{
		{"an expression reading what a pattern binds, and the pattern's gate",
			`[:find ?y :where [?e :p ?y] [(identity ?e) ?y]]`,
			`[(identity ?e) ?y]`, []string{"?e", "?y"}},
		{"through a cycle of patterns and expressions",
			`[:find ?a :where [?a :p ?y] [(identity ?b) ?y] [?b :q ?z] [(identity ?a) ?z]]`,
			`[(identity ?b) ?y]`, []string{"?b", "?z", "?a", "?y"}},
		{"through a subquery",
			`[:find ?y :where [?e :p ?y] [(q [:find ?v :in $ ?e :where [?e :r ?v]] $ ?e) [[?v]]] [(identity ?v) ?y]]`,
			`[(identity ?v) ?y]`, []string{"?v", "?e", "?y"}},
		{"through an expression and the pattern it waits on",
			`[:find ?item :where [?e :p ?y] [(identity ?e) ?y] [?item :color ?c] [(identity ?y) ?item]]`,
			`[(identity ?y) ?item]`, []string{"?y", "?e"}},
		{"stopping at a pattern no expression holds",
			`[:find ?label :where [?c :container/items ?vec] [(enumerate ?vec) [?idx ?item]] [?item :item/color :red] [?item :item/label ?label]]`,
			`[(enumerate ?vec) [[?idx ?item]]]`, []string{"?vec"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(c.text)
			if err != nil {
				t.Fatalf("ParseQuery: %v", err)
			}
			var of query.Clause
			for _, clause := range parsed.Where {
				if clause.String() == c.of {
					of = clause
				}
			}
			if of == nil {
				t.Fatalf("the query carries no %s", c.of)
			}
			sel := selection{
				remaining: parsed.Where,
				selected:  map[int]bool{},
				available: map[query.Symbol]bool{},
			}
			got := sel.pendingBlockers(of)
			if len(got) != len(c.waits) {
				t.Errorf("%s waits on %d symbols, want %v, got %v", c.of, len(got), c.waits, got)
			}
			for _, name := range c.waits {
				found := false
				for sym := range got {
					if sym.String() == name {
						found = true
					}
				}
				if !found {
					t.Errorf("%s does not wait on %s: %v", c.of, name, got)
				}
			}
		})
	}
}
