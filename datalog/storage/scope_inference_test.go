package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An or, not or or-default is a header form whose header the planner infers
// from the text, so a variable a branch reads that another clause binds is
// bound before the form runs, in every written order and on both planner
// paths, and a query in which no order binds it is refused naming the form
// and the variable.

// taggedEntities commits e0, carrying :t/tag :t/kind and :t/n 5, and e1,
// carrying :t/n 50, and returns the entities' names by identity.
func taggedEntities(t testing.TB, d *Database) map[datalog.Identity]string {
	t.Helper()
	names := map[datalog.Identity]string{}
	tx := d.NewTransaction()
	for i, facts := range [][2]interface{}{
		{kw(":t/tag"), kw(":t/kind")},
		{kw(":t/n"), int64(5)},
		{kw(":t/n"), int64(50)},
	} {
		name := "e0"
		if i == 2 {
			name = "e1"
		}
		id := datalog.NewIdentity(name)
		names[id] = name
		require.NoError(t, tx.Add(id, facts[0].(datalog.Keyword), facts[1]))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
	return names
}

// renderedRows reads a result into one string per tuple, an identity by its
// name in names and any other value by fmt, sorted.
func renderedRows(t testing.TB, rel executor.Relation, names map[datalog.Identity]string) []string {
	t.Helper()
	var rows []string
	require.NoError(t, executor.ForEach(rel, func(tuple executor.Tuple) error {
		parts := make([]string, len(tuple))
		for i, v := range tuple {
			if id, ok := v.(datalog.Identity); ok {
				parts[i] = names[id]
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		rows = append(rows, strings.Join(parts, " "))
		return nil
	}))
	sort.Strings(rows)
	return rows
}

// orderings is every ordering of n positions.
func orderings(n int) [][]int {
	if n == 0 {
		return [][]int{{}}
	}
	var out [][]int
	for _, rest := range orderings(n - 1) {
		for at := 0; at <= len(rest); at++ {
			order := make([]int, 0, n)
			order = append(order, rest[:at]...)
			order = append(order, n-1)
			order = append(order, rest[at:]...)
			out = append(out, order)
		}
	}
	return out
}

// An or reading a variable another clause binds answers the same rows in
// every written order of the query's clauses, on both planner paths,
// whichever clause binds the variable: a ground, a chain of expressions, an
// or, an or-join or a subquery.
func TestPlan_OrderIndependentUnderBothPaths(t *testing.T) {
	anchor := `[?q :t/n _]`
	reading := `(or [?q ?a ?v] [?q :t/tag :t/none])`
	binders := []struct {
		name    string
		clauses []string
	}{
		{"ground", []string{`[(ground :t/tag) ?a]`}},
		{"identity chain", []string{`[(ground :t/tag) ?b]`, `[(identity ?b) ?a]`}},
		{"or", []string{`(or [(ground :t/tag) ?a] [(ground :t/other) ?a])`}},
		{"or-join", []string{`(or-join [?a] [(ground :t/tag) ?a] [(ground :t/other) ?a])`}},
		{"subquery", []string{`[(q [:find ?a :in $ :where [(ground :t/tag) ?a]] $) [[?a]]]`}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := taggedEntities(t, d)
			for _, binder := range binders {
				clauses := append([]string{anchor, reading}, binder.clauses...)
				for _, order := range orderings(len(clauses)) {
					ordered := make([]string, len(order))
					for i, at := range order {
						ordered[i] = clauses[at]
					}
					text := "[:find ?q :where " + strings.Join(ordered, " ") + "]"
					rel, err := d.Query(text)
					require.NoError(t, err, "%s: %s", binder.name, text)
					require.Equal(t, []string{"e0"}, renderedRows(t, rel, names), "%s: %s", binder.name, text)
				}
			}
		})
	}
}

// A query in which no order binds a variable an or's branch reads before the
// or runs is refused naming the form and the variable: a binder that itself
// needs the or's output, and two ors each reading a variable the other alone
// binds in one branch.
func TestPlan_RefusesAFormNoOrderBinds(t *testing.T) {
	queries := []struct {
		name, text, variable, form string
	}{
		{"binder needing the form's output",
			`[:find ?e ?x ?a :where [?e :t/n _] (or [?e ?a ?x] [?e :t/tag ?x]) [(identity ?x) ?a]]`,
			"?a", "[?e ?a ?x]"},
		{"two forms each reading what the other binds in one branch",
			`[:find ?e :where [?e :t/n ?n] (or [?m :t/n ?n] [?e :t/tag _]) (or [?e :t/n ?m] [?e :t/tag _])]`,
			"?m", "[?m :t/n ?n]"},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			taggedEntities(t, d)
			for _, q := range queries {
				rel, err := d.Query(q.text)
				if err == nil {
					err = executor.ForEach(rel, func(executor.Tuple) error { return nil })
				}
				require.Error(t, err, "%s answers; it is refused", q.name)
				require.Contains(t, err.Error(), q.variable, "%s: the refusal names the variable", q.name)
				require.Contains(t, err.Error(), q.form, "%s: the refusal names the form", q.name)
			}
		})
	}
}

// Both planner paths order a branch under the rule set, so an invocation of
// a predicate with a required position written before the branch clause that
// binds the position runs after it (docs/proposals/NESTED_SCOPES_AS_QUERIES.md
// §2.5).
func TestPlan_AlgebraBridgeOrdersUnderTheRuleSet(t *testing.T) {
	const rules = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`
	const text = `[:find ?x ?u :in $ % :where [?x :t/n ?v] (or-join [?x ?u] (and (succ ?w ?u) [?x :t/n ?w]) (and [?x :t/tag _] [(ground 0) ?u]))]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := taggedEntities(t, d)
			rel, err := d.Query(text, mustRuleSet(t, rules))
			require.NoError(t, err)
			require.Equal(t, []string{"e0 0", "e0 6", "e1 51"}, renderedRows(t, rel, names))
		})
	}
}

// An or-default reading an :in symbol carries it in its required run, and the
// query's environment binds it for every group, on both planner paths.
func TestPlan_OrDefaultReadingAnInputRunsOnBothPaths(t *testing.T) {
	const text = `[:find ?x ?k :in $ ?s :where [?x :t/n _] (or-default (and [?x :t/n ?n] [(= ?n ?s)] [(ground 1) ?k]) [(ground 0) ?k])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := taggedEntities(t, d)
			rel, err := d.Query(text, int64(5))
			require.NoError(t, err)
			require.Equal(t, []string{"e0 1", "e1 0"}, renderedRows(t, rel, names))
		})
	}
}

// Explain, ExplainAlgebra and Analyze plan the prepared query, on both
// planner paths: a query holding a plain or returns phases of header forms
// from each, and a query the preparation refuses returns the refusal from
// each. The refused query declares % twice and is built from a parsed
// query's parts, since ParseQuery refuses the text.
func TestExplain_PlansThePreparedQuery(t *testing.T) {
	const plainOr = `[:find ?q :where [?q :t/n _] (or [?q :t/tag ?x] [?q :t/n ?x])]`
	const rules = `[[(follows ?a ?b) [?a :t/next ?b]]]`
	parsed, err := parser.ParseQuery(`[:find ?a ?b :in $ % :where (follows ?a ?b)]`)
	require.NoError(t, err)
	twice := &query.Query{
		Find:  parsed.Find,
		In:    append(append([]query.InputSpec{}, parsed.In...), query.RulesInput{}),
		Where: parsed.Where,
	}
	want := query.ValidateInputShapes(twice.In)
	require.Error(t, want, "the fixture declares % twice, which ValidateInputShapes refuses")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			taggedEntities(t, d)
			rs := mustRuleSet(t, rules)

			plan, err := d.Explain(plainOr)
			require.NoError(t, err)
			requirePhasesOfHeaderForms(t, "Explain", plan)
			expl, err := d.ExplainAlgebra(plainOr)
			require.NoError(t, err)
			requirePhasesOfHeaderForms(t, "ExplainAlgebra", expl.Plan)
			analyzed, err := d.Analyze(plainOr)
			require.NoError(t, err)
			requirePhasesOfHeaderForms(t, "Analyze", analyzed.Plan)

			for _, entry := range []struct {
				name string
				call func() error
			}{
				{"Explain", func() error { _, err := d.Explain(twice, rs, rs); return err }},
				{"ExplainAlgebra", func() error { _, err := d.ExplainAlgebra(twice, rs, rs); return err }},
				{"Analyze", func() error { _, err := d.Analyze(twice, rs, rs); return err }},
			} {
				err := entry.call()
				require.Error(t, err, "%s plans a query the preparation refuses", entry.name)
				require.Contains(t, err.Error(), want.Error(), "%s: the refusal is the preparation's", entry.name)
			}
		})
	}
}

// requirePhasesOfHeaderForms asserts that no phase of plan holds an or, a not
// or an or-default: every nested form a phase carries states its header.
func requirePhasesOfHeaderForms(t *testing.T, entry string, plan *planner.RealizedPlan) {
	t.Helper()
	for i, phase := range plan.Phases {
		query.WalkClauses(phase.Query.Where, func(c query.Clause) bool {
			switch c.(type) {
			case *query.OrClause, *query.NotClause, *query.OrDefaultClause:
				t.Errorf("%s: phase %d holds the headerless form %s:\n%s", entry, i, c, plan)
			}
			return true
		})
	}
}
