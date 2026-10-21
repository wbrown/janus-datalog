package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Prepare composes the four static steps, each a function of the text and
// the rule set: ValidateInputShapes on :in, ValidateStaticClauseShapes on
// :where, InferHeaders, and the check that a rule set was given when the
// query invokes a predicate and that every invocation names a predicate it
// defines at its arity. A query a step refuses gets that step's own error
// from Prepare, and a query every step accepts comes back with every nested
// form a header form.
func TestPrepare_ComposesTheStaticSteps(t *testing.T) {
	t.Run("two % inputs get the input-shape refusal", func(t *testing.T) {
		q := findIn([]string{"?a", "?b"}, []InputSpec{RulesInput{}, RulesInput{}}, invoke("follows", "?a", "?b"))
		want := ValidateInputShapes(q.In)
		if want == nil {
			t.Fatal("the fixture declares % twice, which ValidateInputShapes refuses")
		}
		_, err := Prepare(q, mustRuleSet(t, followsRules()))
		if err == nil || err.Error() != want.Error() {
			t.Fatalf("Prepare = %v, want the input-shape refusal %v", err, want)
		}
	})

	t.Run("a subquery binding disagreeing with its :find gets the clause-shape refusal", func(t *testing.T) {
		inner := &Query{
			Find:  []FindElement{FindVariable{Symbol: sym("?v")}},
			Where: []Clause{scopePattern("?x", "a/b", "?v")},
		}
		call := &SubqueryPattern{
			Query:   inner,
			Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}},
			Binding: RelationBinding{Variables: []Symbol{sym("?p"), sym("?q")}},
		}
		q := findIn([]string{"?p"}, nil, call)
		want := ValidateStaticClauseShapes(q.Where)
		if want == nil {
			t.Fatal("the fixture's relation binding declares two symbols for a one-element :find, which ValidateStaticClauseShapes refuses")
		}
		_, err := Prepare(q, nil)
		if err == nil || err.Error() != want.Error() {
			t.Fatalf("Prepare = %v, want the clause-shape refusal %v", err, want)
		}
	})

	t.Run("an or sharing nothing with its scope gets the inference's refusal", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "p", "_"),
			&OrClause{Branches: [][]Clause{
				{scopePattern("?x", "q", "_")},
				{scopePattern("?x", "r", "_")},
			}},
		)
		_, want := InferHeaders(q, nil)
		if want == nil {
			t.Fatal("the fixture's or shares nothing with its scope, which InferHeaders refuses")
		}
		_, err := Prepare(q, nil)
		if err == nil || err.Error() != want.Error() {
			t.Fatalf("Prepare = %v, want the inference's refusal %v", err, want)
		}
	})

	t.Run("an invocation of a predicate the rule set does not define gets the invocation check's refusal", func(t *testing.T) {
		q := findIn([]string{"?a", "?b"}, []InputSpec{RulesInput{}}, invoke("reaches", "?a", "?b"))
		_, err := Prepare(q, mustRuleSet(t, followsRules()))
		var unknown *UnknownRuleError
		if !errors.As(err, &unknown) || unknown.Name != sym("reaches") {
			t.Fatalf("Prepare = %v, want *UnknownRuleError naming reaches", err)
		}
	})

	t.Run("an invocation with no rule set given gets the invocation check's refusal", func(t *testing.T) {
		q := findIn([]string{"?a", "?b"}, []InputSpec{RulesInput{}}, invoke("follows", "?a", "?b"))
		_, err := Prepare(q, nil)
		if err == nil || !strings.Contains(err.Error(), "no rule set was given") {
			t.Fatalf("Prepare = %v, want the refusal naming the missing rule set", err)
		}
	})

	t.Run("a query of plain forms comes back prepared, every nested form a header form", func(t *testing.T) {
		q := findIn([]string{"?e", "?x"}, nil,
			scopePattern("?e", "t/n", "_"),
			&OrClause{Branches: [][]Clause{
				{scopePattern("?e", "t/tag", "?x")},
				{scopePattern("?e", "t/other", "?x"), &NotClause{Clauses: []Clause{scopePattern("?x", "t/hidden", "_")}}},
			}},
		)
		want := renderedClauses(t, q, nil)
		prepared, err := Prepare(q, nil)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		WalkClauses(prepared.Where, func(c Clause) bool {
			switch c.(type) {
			case *OrClause, *NotClause, *OrDefaultClause:
				t.Errorf("prepared query holds the headerless form %s", c)
			}
			return true
		})
		for i, c := range prepared.Where {
			if got := c.String(); got != want[i] {
				t.Errorf("prepared clause %d = %s, want the inferred %s", i, got, want[i])
			}
		}
	})
}

// BenchmarkPrepare measures preparation, which runs once per execution: a
// query holding a plain or over a branch holding a plain not, which the
// inference rewrites, and the same query written as the header forms the
// inference writes for it, which it returns as it is.
func BenchmarkPrepare(b *testing.B) {
	cases := []struct {
		name string
		q    *Query
	}{
		{"plain-or", findIn([]string{"?e", "?x"}, nil,
			scopePattern("?e", "t/n", "_"),
			&OrClause{Branches: [][]Clause{
				{scopePattern("?e", "t/tag", "?x")},
				{scopePattern("?e", "t/other", "?x"), &NotClause{Clauses: []Clause{scopePattern("?x", "t/hidden", "_")}}},
			}},
		)},
		{"header-forms", findIn([]string{"?e", "?x"}, nil,
			scopePattern("?e", "t/n", "_"),
			&OrJoinClause{OutputVars: []Symbol{sym("?e"), sym("?x")}, Branches: [][]Clause{
				{scopePattern("?e", "t/tag", "?x")},
				{scopePattern("?e", "t/other", "?x"), &NotJoinClause{RequiredVars: []Symbol{sym("?x")}, Clauses: []Clause{scopePattern("?x", "t/hidden", "_")}}},
			}},
		)},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Prepare(c.q, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
