package query

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// InferHeaders rewrites every or, not and or-default as the header form that
// states its interface in the text, computing the header from the text's
// occurrences: a variable inside the form that occurs elsewhere in its
// enclosing scope is shared and one occurring only inside one branch is
// local. A shared variable the form binds in every branch is an output and
// every other shared variable is required; a not binds nothing for its
// scope, so its shared variables are all required. A written or-join header
// is read strictly: every branch binds every output of a flat header.

// groundInto is the expression [(ground value) ?binding].
func groundInto(value interface{}, binding string) *Expression {
	return &Expression{Function: &GroundFunction{Value: value}, Binding: sym(binding)}
}

// findIn builds a query finding the named variables, with $ and the given
// inputs after it, over where.
func findIn(find []string, in []InputSpec, where ...Clause) *Query {
	q := &Query{In: append([]InputSpec{DatabaseInput{Name: datalog.SymDollar}}, in...), Where: where}
	for _, name := range find {
		q.Find = append(q.Find, FindVariable{Symbol: sym(name)})
	}
	return q
}

// renderedClauses infers q's headers under rules and renders each top-level
// clause of the result.
func renderedClauses(t *testing.T, q *Query, rules *RuleSet) []string {
	t.Helper()
	inferred, err := InferHeaders(q, rules)
	if err != nil {
		t.Fatalf("InferHeaders: %v", err)
	}
	out := make([]string, len(inferred.Where))
	for i, c := range inferred.Where {
		out[i] = c.String()
	}
	return out
}

func TestInferHeaders_OrOutputsAndRequired(t *testing.T) {
	// ?e and ?x are bound by every branch and occur outside the or, so they
	// are outputs; ?a is bound by one branch and occurs in the ground, so it
	// is required; ?l occurs only inside branch 2, so it is local.
	q := findIn([]string{"?e", "?x"}, nil,
		scopePattern("?e", "t/n", "_"),
		groundInto(datalog.NewKeyword(":t/tag"), "?a"),
		&OrClause{Branches: [][]Clause{
			{scopePattern("?e", "?a", "?x")},
			{scopePattern("?e", "t/tag", "?x"), scopePattern("?x", "p", "?l")},
		}},
	)
	got := renderedClauses(t, q, nil)
	want := "(or-join [[?a] ?e ?x] [?e ?a ?x] (and [?e :t/tag ?x] [?x :p ?l]))"
	if got[2] != want {
		t.Errorf("inferred or = %s, want %s", got[2], want)
	}
}

func TestInferHeaders_NotAndOrDefaultInterfaces(t *testing.T) {
	t.Run("a not's shared variables are required and its other variables local", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "p", "?v"),
			&NotClause{Clauses: []Clause{scopePattern("?e", "q", "?v"), scopePattern("?w", "r", "?w")}},
		)
		got := renderedClauses(t, q, nil)
		want := "(not-join [?e ?v] [?e :q ?v] [?w :r ?w])"
		if got[1] != want {
			t.Errorf("inferred not = %s, want %s", got[1], want)
		}
	})

	t.Run("an or-default's every-branch-bound variable nothing else binds is an output", func(t *testing.T) {
		q := findIn([]string{"?e", "?v"}, nil,
			scopePattern("?e", "k", "_"),
			&OrDefaultClause{Branches: [][]Clause{
				{scopePattern("?e", "q", "?v")},
				{groundInto(int64(0), "?v")},
			}},
		)
		got := renderedClauses(t, q, nil)
		want := "(or-default-join [[?e] ?v] [?e :q ?v] [(ground 0) ?v])"
		if got[1] != want {
			t.Errorf("inferred or-default = %s, want %s", got[1], want)
		}
	})

	t.Run("an or-default's every-branch-bound variable another clause binds is an output", func(t *testing.T) {
		q := findIn([]string{"?e", "?v"}, nil,
			scopePattern("?e", "k", "_"),
			scopePattern("?v", "r", "_"),
			&OrDefaultClause{Branches: [][]Clause{
				{scopePattern("?e", "q", "?v")},
				{groundInto(int64(0), "?v")},
			}},
		)
		got := renderedClauses(t, q, nil)
		want := "(or-default-join [[?e] ?v] [?e :q ?v] [(ground 0) ?v])"
		if got[2] != want {
			t.Errorf("inferred or-default = %s, want %s", got[2], want)
		}
	})

	t.Run("an or-default's every-branch-bound variable an input binds is an output", func(t *testing.T) {
		q := findIn([]string{"?e"}, []InputSpec{ScalarInput{Symbol: sym("?v")}},
			scopePattern("?e", "k", "_"),
			&OrDefaultClause{Branches: [][]Clause{
				{scopePattern("?e", "q", "?v")},
				{groundInto(int64(0), "?v")},
			}},
		)
		got := renderedClauses(t, q, nil)
		want := "(or-default-join [[?e] ?v] [?e :q ?v] [(ground 0) ?v])"
		if got[1] != want {
			t.Errorf("inferred or-default = %s, want %s", got[1], want)
		}
	})
}

func TestInferHeaders_NestedFormReadsTheEnclosingHeader(t *testing.T) {
	// ?y occurs at the top level, but the forms below sit inside a header
	// form whose header does not carry ?y, so inside them ?y is local.
	inner := func() *OrClause {
		return &OrClause{Branches: [][]Clause{
			{scopePattern("?x", "r", "?y")},
			{scopePattern("?x", "s", "?y")},
		}}
	}
	t.Run("inside an or-join branch", func(t *testing.T) {
		other := scopePattern("?e", "t", "_")
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "p", "?y"),
			&OrJoinClause{OutputVars: []Symbol{sym("?e")}, Branches: [][]Clause{
				{scopePattern("?e", "q", "?x"), inner()},
				{other},
			}},
		)
		inferred, err := InferHeaders(q, nil)
		if err != nil {
			t.Fatalf("InferHeaders: %v", err)
		}
		want := "(or-join [?e] (and [?e :q ?x] (or-join [?x] [?x :r ?y] [?x :s ?y])) [?e :t _])"
		if got := inferred.Where[1].String(); got != want {
			t.Errorf("inferred or-join = %s, want %s", got, want)
		}
		if inferred.Where[0] != q.Where[0] {
			t.Errorf("the pattern before the form is copied; a clause on no path to a rewritten form is shared")
		}
		if inferred.Where[1] == q.Where[1] {
			t.Errorf("the or-join holding a rewritten form is shared; a form on the path to a rewritten form is copied")
		}
		if outer, ok := inferred.Where[1].(*OrJoinClause); !ok || outer.Branches[1][0] != other {
			t.Errorf("the branch holding no rewritten form is copied; its clauses are shared")
		}
	})

	t.Run("inside a not-join body", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "p", "?y"),
			&NotJoinClause{RequiredVars: []Symbol{sym("?e")}, Clauses: []Clause{
				scopePattern("?e", "q", "?x"),
				&NotClause{Clauses: []Clause{scopePattern("?x", "r", "?y")}},
			}},
		)
		got := renderedClauses(t, q, nil)
		want := "(not-join [?e] [?e :q ?x] (not-join [?x] [?x :r ?y]))"
		if got[1] != want {
			t.Errorf("inferred not-join = %s, want %s", got[1], want)
		}
	})

	t.Run("inside an or-default-join branch", func(t *testing.T) {
		q := findIn([]string{"?e", "?k"}, nil,
			scopePattern("?e", "p", "?y"),
			&OrDefaultJoinClause{RequiredVars: []Symbol{sym("?e")}, OutputVars: []Symbol{sym("?k")}, Branches: [][]Clause{
				{scopePattern("?e", "q", "?x"), inner(), groundInto(int64(1), "?k")},
				{groundInto(int64(0), "?k")},
			}},
		)
		got := renderedClauses(t, q, nil)
		want := "(or-default-join [[?e] ?k] (and [?e :q ?x] (or-join [?x] [?x :r ?y] [?x :s ?y]) [(ground 1) ?k]) [(ground 0) ?k])"
		if got[1] != want {
			t.Errorf("inferred or-default-join = %s, want %s", got[1], want)
		}
	})

	t.Run("inside a subquery body", func(t *testing.T) {
		sub := &SubqueryPattern{
			Query: findIn([]string{"?x"}, []InputSpec{ScalarInput{Symbol: sym("?e")}},
				scopePattern("?e", "q", "?x"), inner()),
			Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: sym("?e")}},
			Binding: TupleBinding{Variables: []Symbol{sym("?x")}},
		}
		q := findIn([]string{"?e", "?x"}, nil, scopePattern("?e", "p", "?y"), sub)
		inferred, err := InferHeaders(q, nil)
		if err != nil {
			t.Fatalf("InferHeaders: %v", err)
		}
		inferredSub, ok := inferred.Where[1].(*SubqueryPattern)
		if !ok {
			t.Fatalf("clause 1 = %T, want a subquery", inferred.Where[1])
		}
		want := "(or-join [?x] [?x :r ?y] [?x :s ?y])"
		if got := inferredSub.Query.Where[1].String(); got != want {
			t.Errorf("inferred inner or = %s, want %s", got, want)
		}
	})
}

func TestInferHeaders_FindAndInputsAreOccurrences(t *testing.T) {
	branches := func() [][]Clause {
		return [][]Clause{
			{scopePattern("?e", "?a", "?v")},
			{scopePattern("?e", "t", "?v")},
		}
	}
	t.Run("a variable in :in is shared and required, one in :find shared and output", func(t *testing.T) {
		q := findIn([]string{"?e", "?v"}, []InputSpec{ScalarInput{Symbol: sym("?a")}},
			scopePattern("?e", "k", "_"),
			&OrClause{Branches: branches()},
		)
		got := renderedClauses(t, q, nil)
		want := "(or-join [[?a] ?e ?v] [?e ?a ?v] [?e :t ?v])"
		if got[1] != want {
			t.Errorf("inferred or = %s, want %s", got[1], want)
		}
	})
	t.Run("a variable occurring only inside the form is local", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "k", "_"),
			&OrClause{Branches: branches()},
		)
		got := renderedClauses(t, q, nil)
		want := "(or-join [?e] [?e ?a ?v] [?e :t ?v])"
		if got[1] != want {
			t.Errorf("inferred or = %s, want %s", got[1], want)
		}
	})
}

func TestInferHeaders_RequiredPositionIsConsumedUnderTheRuleSet(t *testing.T) {
	// (succ ?w ?u) binds ?w under no rule set and consumes it under the set
	// that requires succ's first position, so ?w is an output of the or
	// without the rule set and required with it.
	succ := defineRule("succ", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?m"))
	succ.Required = []Symbol{sym("?n")}
	rs := mustRuleSet(t, []*Rule{succ})
	q := findIn([]string{"?x", "?u"}, []InputSpec{RulesInput{}},
		scopePattern("?x", "val", "?w"),
		&OrClause{Branches: [][]Clause{
			{invoke("succ", "?w", "?u")},
			{scopePattern("?x", "c", "?u"), scopePattern("?x", "d", "?w")},
		}},
	)
	got := renderedClauses(t, q, rs)
	want := "(or-join [[?w ?x] ?u] (succ ?w ?u) (and [?x :c ?u] [?x :d ?w]))"
	if got[1] != want {
		t.Errorf("inferred or under the rule set = %s, want %s", got[1], want)
	}
	got = renderedClauses(t, q, nil)
	want = "(or-join [[?x] ?w ?u] (succ ?w ?u) (and [?x :c ?u] [?x :d ?w]))"
	if got[1] != want {
		t.Errorf("inferred or with no rule set = %s, want %s", got[1], want)
	}
}

func TestInferHeaders_HeaderFormsStand(t *testing.T) {
	t.Run("a query of complete header forms is returned as it is", func(t *testing.T) {
		q := findIn([]string{"?e", "?v"}, nil,
			scopePattern("?e", "k", "_"),
			&OrJoinClause{OutputVars: []Symbol{sym("?e")}, Branches: [][]Clause{
				{scopePattern("?e", "p", "_")},
				{scopePattern("?e", "q", "_")},
			}},
			&NotJoinClause{RequiredVars: []Symbol{sym("?e")}, Clauses: []Clause{scopePattern("?e", "r", "_")}},
			&OrDefaultJoinClause{RequiredVars: []Symbol{sym("?e")}, OutputVars: []Symbol{sym("?v")}, Branches: [][]Clause{
				{scopePattern("?e", "s", "?v")},
				{groundInto(int64(0), "?v")},
			}},
		)
		inferred, err := InferHeaders(q, nil)
		if err != nil {
			t.Fatalf("InferHeaders: %v", err)
		}
		if inferred != q {
			t.Errorf("a query of complete header forms is rebuilt; it is returned as it is")
		}
	})

	t.Run("a flat or-join header variable some branch does not bind is refused", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "t/n", "_"),
			groundInto(datalog.NewKeyword(":t/tag"), "?a"),
			&OrJoinClause{OutputVars: []Symbol{sym("?e"), sym("?a")}, Branches: [][]Clause{
				{scopePattern("?e", "?a", "?v")},
				{scopePattern("?e", "t/tag", "t/none")},
			}},
		)
		_, err := InferHeaders(q, nil)
		if err == nil {
			t.Fatalf("a flat header variable branch 2 does not bind is inferred; it is refused")
		}
		for _, part := range []string{"(or-join [?e ?a]", "branch 2 does not bind ?a", "move ?a into the required run"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("the refusal %q does not carry %q", err, part)
			}
		}
	})

	t.Run("a variable an or-join branch reads outside the header is refused", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "n", "?n"),
			&OrJoinClause{OutputVars: []Symbol{sym("?e")}, Branches: [][]Clause{
				{scopePattern("?e", "p", "_"), &Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: sym("?n")}, Right: ConstantTerm{Value: int64(5)}}},
				{scopePattern("?e", "q", "_")},
			}},
		)
		_, err := InferHeaders(q, nil)
		if err == nil {
			t.Fatalf("a branch reading ?n outside the header is inferred; it is refused")
		}
		for _, part := range []string{"(or-join [?e]", "?n"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("the refusal %q does not carry %q", err, part)
			}
		}
	})
}

func TestInferHeaders_NotSharingNothingIsRefused(t *testing.T) {
	q := findIn([]string{"?e"}, nil,
		scopePattern("?e", "p", "_"),
		&NotClause{Clauses: []Clause{scopePattern("?x", "q", "?x")}},
	)
	_, err := InferHeaders(q, nil)
	if err == nil {
		t.Fatalf("a not sharing no variable with its scope is inferred; it is refused")
	}
	if !strings.Contains(err.Error(), "(not [?x :q ?x])") {
		t.Errorf("the refusal %q does not name the not", err)
	}
}

// A written header is read strictly, under the rule set
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4).
func TestInferHeaders_ChecksAWrittenHeaderUnderTheRuleSet(t *testing.T) {
	succ := defineRule("succ", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?m"))
	succ.Required = []Symbol{sym("?n")}
	rs := mustRuleSet(t, []*Rule{succ})
	refused := func(t *testing.T, q *Query, rules *RuleSet, parts ...string) {
		t.Helper()
		_, err := InferHeaders(q, rules)
		if err == nil {
			t.Fatalf("the query is inferred; it is refused naming %q", parts)
		}
		for _, part := range parts {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("the refusal %q does not carry %q", err, part)
			}
		}
	}

	t.Run("an or-join output a branch consumes under the rule set is not bound by it", func(t *testing.T) {
		q := findIn([]string{"?e", "?w"}, []InputSpec{RulesInput{}},
			scopePattern("?e", "k", "_"),
			&OrJoinClause{OutputVars: []Symbol{sym("?e"), sym("?w")}, Branches: [][]Clause{
				{invoke("succ", "?w", "?e")},
				{scopePattern("?e", "c", "?w")},
			}},
		)
		refused(t, q, rs, "(or-join [?e ?w]", "branch 1 does not bind ?w", "move ?w into the required run")
		if _, err := InferHeaders(q, nil); err != nil {
			t.Errorf("with no rule set the invocation binds ?w, so the header stands; got %v", err)
		}
	})

	t.Run("an or-default-join output a branch's required position consumes is not bound by it", func(t *testing.T) {
		q := findIn([]string{"?e", "?w"}, []InputSpec{RulesInput{}},
			scopePattern("?e", "k", "_"),
			&OrDefaultJoinClause{RequiredVars: []Symbol{sym("?e")}, OutputVars: []Symbol{sym("?w")}, Branches: [][]Clause{
				{scopePattern("?e", "c", "?w")},
				{invoke("succ", "?w", "?e")},
			}},
		)
		refused(t, q, rs, "(or-default-join [[?e] ?w]", "branch 2 does not bind ?w", "move ?w into the required run")
	})

	t.Run("a not-join body reads a variable it neither binds nor names in its header", func(t *testing.T) {
		notJoin := &NotJoinClause{RequiredVars: []Symbol{sym("?e")}, Clauses: []Clause{
			scopePattern("?e", "q", "?x"),
			&Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: sym("?x")}, Right: VariableTerm{Symbol: sym("?w")}},
		}}
		unbound := findIn([]string{"?e"}, nil, scopePattern("?e", "p", "?w"), notJoin)
		refused(t, unbound, nil, "(not-join [?e]", "?w")
		input := findIn([]string{"?e"}, []InputSpec{ScalarInput{Symbol: sym("?w")}}, scopePattern("?e", "p", "_"), notJoin)
		refused(t, input, nil, "(not-join [?e]", "?w")
	})

	t.Run("a not-join header names a variable its body neither binds nor reads", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "p", "?z"),
			&NotJoinClause{RequiredVars: []Symbol{sym("?e"), sym("?z")}, Clauses: []Clause{scopePattern("?e", "q", "_")}},
		)
		refused(t, q, nil, "(not-join [?e ?z]", "?z")
	})
}

// A variable a branch's predicate, expression, subquery input or required
// position reads, which neither the branch nor the header binds, is refused
// naming the form and the variable, and a form's sharing is checked after
// that (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4).
func TestInferHeaders_BranchInputNothingBindsIsRefused(t *testing.T) {
	succ := defineRule("succ", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?m"))
	succ.Required = []Symbol{sym("?n")}
	rs := mustRuleSet(t, []*Rule{succ})
	readers := []struct {
		name   string
		clause Clause
	}{
		{"predicate", &Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: sym("?x")}, Right: VariableTerm{Symbol: sym("?w")}}},
		{"expression", &Expression{
			Function: ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{VariableTerm{Symbol: sym("?x")}, VariableTerm{Symbol: sym("?w")}}},
			Binding:  sym("?y"),
		}},
		{"subquery input", &SubqueryPattern{
			Query: &Query{
				Find:  []FindElement{FindVariable{Symbol: sym("?v")}},
				In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: sym("?i")}},
				Where: []Clause{scopePattern("?i", "s", "?v")},
			},
			Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: sym("?w")}},
			Binding: TupleBinding{Variables: []Symbol{sym("?y")}},
		}},
		{"required position", invoke("succ", "?w", "?y")},
	}
	forms := []struct {
		name string
		form func(branch []Clause) Clause
	}{
		{"or", func(branch []Clause) Clause {
			return &OrClause{Branches: [][]Clause{branch, {scopePattern("?e", "r", "_")}}}
		}},
		{"or-default", func(branch []Clause) Clause {
			return &OrDefaultClause{Branches: [][]Clause{branch, {scopePattern("?e", "r", "_")}}}
		}},
		{"not", func(branch []Clause) Clause {
			return &NotClause{Clauses: branch}
		}},
	}
	for _, form := range forms {
		for _, reader := range readers {
			t.Run(form.name+" branch with a "+reader.name+" reading ?w", func(t *testing.T) {
				clause := form.form([]Clause{scopePattern("?e", "q", "?x"), reader.clause})
				q := findIn([]string{"?e"}, []InputSpec{RulesInput{}}, scopePattern("?e", "p", "_"), clause)
				_, err := InferHeaders(q, rs)
				if err == nil {
					t.Fatalf("a branch reading ?w, which nothing binds, is inferred; it is refused")
				}
				for _, part := range []string{"(" + form.name, "?w"} {
					if !strings.Contains(err.Error(), part) {
						t.Errorf("the refusal %q does not carry %q", err, part)
					}
				}
			})
		}
	}

	t.Run("a form sharing nothing whose branch reads ?w is refused naming ?w", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil,
			scopePattern("?e", "p", "_"),
			&NotClause{Clauses: []Clause{&Comparison{Op: datalog.SymGT, Left: VariableTerm{Symbol: sym("?w")}, Right: ConstantTerm{Value: int64(1)}}}},
		)
		_, err := InferHeaders(q, nil)
		if err == nil {
			t.Fatalf("a not reading ?w, which nothing binds, is inferred; it is refused")
		}
		if !strings.Contains(err.Error(), "?w") {
			t.Errorf("the refusal %q does not name ?w", err)
		}
		if strings.Contains(err.Error(), "shares no variable") {
			t.Errorf("the refusal %q names the sharing, where the branch's read of ?w comes first", err)
		}
	})
}

// An or or an or-default whose variables occur nowhere else in its scope is
// refused naming the form (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4).
func TestInferHeaders_OrSharingNothingIsRefused(t *testing.T) {
	branches := [][]Clause{{scopePattern("?x", "q", "_")}, {scopePattern("?x", "r", "_")}}
	for _, form := range []Clause{&OrClause{Branches: branches}, &OrDefaultClause{Branches: branches}} {
		t.Run(form.String(), func(t *testing.T) {
			q := findIn([]string{"?e"}, nil, scopePattern("?e", "p", "_"), form)
			_, err := InferHeaders(q, nil)
			if err == nil {
				t.Fatalf("%s shares no variable with its scope; it is refused", form)
			}
			for _, part := range []string{form.String(), "shares no variable"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("the refusal %q does not carry %q", err, part)
				}
			}
		})
	}
}
