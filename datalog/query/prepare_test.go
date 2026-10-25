package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Prepare composes the five static steps, each a function of the text and
// the rule set: ValidateQuery, the check that no variable lies in a namespace
// the engine mints into, InferHeaders, UnifyPatterns, and the check that a
// rule set was given when the query invokes a predicate and that every
// invocation names a predicate it defines at its arity. A query a step
// refuses gets that step's own error from Prepare, and a query every step
// accepts comes back with every nested form a header form.
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

	t.Run("an invocation under a :in declaring no % gets the rules-input refusal", func(t *testing.T) {
		q := findIn([]string{"?a", "?b"}, nil, invoke("follows", "?a", "?b"))
		want := ValidateRulesInput(q.In, q.Where)
		if want == nil {
			t.Fatal("the fixture invokes a predicate under a :in declaring no %, which ValidateRulesInput refuses")
		}
		_, err := Prepare(q, nil)
		if err == nil || err.Error() != want.Error() {
			t.Fatalf("Prepare = %v, want the rules-input refusal %v", err, want)
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

// A variable in a namespace the engine mints into is refused by Prepare
// wherever it stands, naming the variable, the namespace and the repair,
// before the inference and the unification write the engine's own,
// ?unify/<name>.<position> and ?scope/input.<position>, into the prepared
// text, which the parser reads back. A variable outside those namespaces is
// accepted.
func TestPrepare_RefusesAVariableInAnEngineNamespace(t *testing.T) {
	dollar := Constant{Value: datalog.SymDollar}
	v := sym("?unify/x.2")
	identity := func(from, to Symbol) *Expression {
		return &Expression{Function: IdentityFunction{Arg: VariableTerm{Symbol: from}}, Binding: to}
	}
	inner := func(find Symbol, where ...Clause) *Query {
		return &Query{Find: []FindElement{FindVariable{Symbol: find}}, In: []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: sym("?x")}}, Where: where}
	}
	call := func(q *Query, argument, binding Symbol) *SubqueryPattern {
		return &SubqueryPattern{Query: q, Inputs: []PatternElement{dollar, Variable{Name: argument}}, Binding: RelationBinding{Variables: []Symbol{binding}}}
	}
	sorted := func(q *Query, by Symbol) *Query {
		q.OrderBy = []OrderByClause{{Variable: by}}
		return q
	}
	cases := []struct {
		name  string
		q     *Query
		rules *RuleSet
	}{
		{"in :find", findIn([]string{"?unify/x.2"}, nil, scopePattern("?e", "p", "?w")), nil},
		{"in :in", findIn([]string{"?w"}, []InputSpec{ScalarInput{Symbol: v}}, scopePattern("?e", "p", "?w")), nil},
		{"in :order-by, bound by a scalar input", sorted(findIn([]string{"?w"}, []InputSpec{ScalarInput{Symbol: v}}, scopePattern("?e", "p", "?w")), v), nil},
		{"in a pattern", findIn([]string{"?e"}, nil, scopePattern("?e", "p", "?unify/x.2")), nil},
		{"as an expression's input", findIn([]string{"?w"}, nil, scopePattern("?e", "p", "?w"), identity(v, sym("?k"))), nil},
		{"as an expression's binding", findIn([]string{"?w"}, nil, scopePattern("?e", "p", "?w"), identity(sym("?w"), v)), nil},
		{"in a predicate", findIn([]string{"?w"}, nil, scopePattern("?e", "p", "?w"), &Comparison{Op: datalog.SymLT, Left: VariableTerm{Symbol: v}, Right: ConstantTerm{Value: int64(10)}}), nil},
		{"as an invocation's argument", findIn([]string{"?b"}, []InputSpec{RulesInput{}}, invoke("follows", "?unify/x.2", "?b")), mustRuleSet(t, followsRules())},
		{"in a not-join's header", findIn([]string{"?e"}, nil, scopePattern("?e", "p", "_"), &NotJoinClause{RequiredVars: []Symbol{v}, Clauses: []Clause{scopePattern("?unify/x.2", "q", "_")}}), nil},
		{"in an or's branch", findIn([]string{"?e"}, nil, scopePattern("?e", "p", "_"), &OrClause{Branches: [][]Clause{{scopePattern("?e", "q", "?unify/x.2")}, {scopePattern("?e", "r", "?unify/x.2")}}}), nil},
		{"in an or-join's branch", findIn([]string{"?e"}, nil, scopePattern("?e", "p", "_"), &OrJoinClause{OutputVars: []Symbol{sym("?e")}, Branches: [][]Clause{{scopePattern("?e", "q", "?unify/x.2")}, {scopePattern("?e", "r", "?unify/x.2")}}}), nil},
		{"as a subquery's argument", findIn([]string{"?n"}, nil, scopePattern("?e", "p", "?w"), call(inner(sym("?y"), scopePattern("?x", "q", "?y")), v, sym("?n"))), nil},
		{"as a subquery's binding", findIn([]string{"?e"}, nil, scopePattern("?e", "p", "?w"), call(inner(sym("?y"), scopePattern("?x", "q", "?y")), sym("?w"), v)), nil},
		{"in an inner query's find", findIn([]string{"?n"}, nil, scopePattern("?e", "p", "?w"), call(inner(v, scopePattern("?x", "q", "?unify/x.2")), sym("?w"), sym("?n"))), nil},
		{"in an inner query two levels down", findIn([]string{"?n"}, nil, scopePattern("?e", "p", "?w"),
			call(inner(sym("?y"), call(inner(sym("?z"), scopePattern("?x", "q", "?unify/x.2"), identity(v, sym("?z"))), sym("?x"), sym("?y"))), sym("?w"), sym("?n"))), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Prepare(c.q, c.rules)
			var refused *EngineNamespaceError
			if !errors.As(err, &refused) {
				t.Fatalf("want an EngineNamespaceError naming %s, got %v", v, err)
			}
			if refused.Variable != v {
				t.Errorf("the refusal names %s, want %s", refused.Variable, v)
			}
			if refused.Rule != nil {
				t.Errorf("a query's own refusal names no rule, got %s", refused.Rule)
			}
			if !strings.Contains(err.Error(), "unify") || !strings.Contains(err.Error(), "rename") {
				t.Errorf("the refusal names the namespace and the repair: %v", err)
			}
		})
	}
	for _, name := range []string{"?scope/input.1", "?magic.bf/x.0", "?adorned.bf/x", "?delta/x", "?unify.mine/x.1"} {
		t.Run(name+" in a pattern", func(t *testing.T) {
			_, err := Prepare(findIn([]string{"?e"}, nil, scopePattern("?e", "p", name)), nil)
			var refused *EngineNamespaceError
			if !errors.As(err, &refused) || refused.Variable != sym(name) {
				t.Fatalf("want an EngineNamespaceError naming %s, got %v", name, err)
			}
		})
	}
	for _, name := range []string{"?unify", "?unify-me", "?scoped/x", "?x.unify/y", "?mine/unify"} {
		t.Run(name+" is accepted", func(t *testing.T) {
			if _, err := Prepare(findIn([]string{"?e"}, nil, scopePattern("?e", "p", name)), nil); err != nil {
				t.Fatalf("%s lies in no namespace the engine mints into: %v", name, err)
			}
		})
	}
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
