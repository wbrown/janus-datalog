package query

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Prepare runs every static check the text admits over every inner query as
// over the outer query, the :order-by rule, the inner :limit refusal and the
// inner pull refusal among them, so a hand-built query is accepted or refused
// as its rendering is at ParseQuery and at qb.Build, with one message
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md goal 5 and §6).

// callOver is the subquery clause [(q inner inputs...) binding].
func callOver(inner *Query, binding BindingForm, inputs ...PatternElement) *SubqueryPattern {
	return &SubqueryPattern{Query: inner, Inputs: inputs, Binding: binding}
}

func TestPrepare_ReachesEveryInnerQuery(t *testing.T) {
	dollar := Constant{Value: datalog.SymDollar}
	rules := Constant{Value: datalog.SymRules}
	x := Variable{Name: sym("?x")}

	t.Run("an inner query invoking a rule under an inner :in declaring no % gets the rules-input refusal", func(t *testing.T) {
		inner := findIn([]string{"?y"}, []InputSpec{ScalarInput{Symbol: sym("?x")}}, invoke("follows", "?x", "?y"))
		want := ValidateRulesInput(inner.In, inner.Where)
		if want == nil {
			t.Fatal("the fixture's inner query invokes a predicate under a :in declaring no %, which ValidateRulesInput refuses")
		}
		outer := findIn([]string{"?y"}, []InputSpec{RulesInput{}, ScalarInput{Symbol: sym("?x")}},
			callOver(inner, CollectionBinding{Variable: sym("?y")}, dollar, x))
		_, err := Prepare(outer, mustRuleSet(t, followsRules()))
		if err == nil || !strings.Contains(err.Error(), want.Error()) {
			t.Fatalf("Prepare = %v, want the inner query's rules-input refusal %v", err, want)
		}
	})

	t.Run("an inner :in declaring % twice gets the input-shape refusal", func(t *testing.T) {
		inner := findIn([]string{"?y"}, []InputSpec{RulesInput{}, RulesInput{}, ScalarInput{Symbol: sym("?x")}}, invoke("follows", "?x", "?y"))
		want := ValidateInputShapes(inner.In)
		if want == nil {
			t.Fatal("the fixture's inner query declares % twice, which ValidateInputShapes refuses")
		}
		outer := findIn([]string{"?y"}, []InputSpec{RulesInput{}, ScalarInput{Symbol: sym("?x")}},
			callOver(inner, CollectionBinding{Variable: sym("?y")}, dollar, rules, rules, x))
		_, err := Prepare(outer, mustRuleSet(t, followsRules()))
		if err == nil || !strings.Contains(err.Error(), want.Error()) {
			t.Fatalf("Prepare = %v, want the inner query's input-shape refusal %v", err, want)
		}
	})

	t.Run("an inner query declaring % once and invoking a rule is accepted", func(t *testing.T) {
		inner := findIn([]string{"?y"}, []InputSpec{RulesInput{}, ScalarInput{Symbol: sym("?x")}}, invoke("follows", "?x", "?y"))
		outer := findIn([]string{"?y"}, []InputSpec{RulesInput{}, ScalarInput{Symbol: sym("?x")}},
			callOver(inner, CollectionBinding{Variable: sym("?y")}, dollar, rules, x))
		if _, err := Prepare(outer, mustRuleSet(t, followsRules())); err != nil {
			t.Fatalf("Prepare = %v, want the inner query accepted", err)
		}
	})
}

func TestPrepare_RefusesAnInnerLimitAndAnInnerPull(t *testing.T) {
	dollar := Constant{Value: datalog.SymDollar}
	x := Variable{Name: sym("?x")}
	outerOver := func(inner *Query) *Query {
		return findIn([]string{"?y"}, []InputSpec{ScalarInput{Symbol: sym("?x")}},
			callOver(inner, CollectionBinding{Variable: sym("?y")}, dollar, x))
	}

	t.Run("an inner :limit is refused", func(t *testing.T) {
		inner := findIn([]string{"?y"}, []InputSpec{ScalarInput{Symbol: sym("?x")}}, scopePattern("?x", "page/next", "?y"))
		limit := 1
		inner.Limit = &limit
		_, err := Prepare(outerOver(inner), nil)
		if err == nil || !strings.Contains(err.Error(), ":limit is not supported inside a subquery") {
			t.Fatalf("Prepare = %v, want the refusal of the inner :limit", err)
		}
	})

	t.Run("an inner pull is refused", func(t *testing.T) {
		inner := &Query{
			Find:  []FindElement{FindPull{Variable: sym("?y"), Pattern: &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: datalog.NewKeyword(":page/kind")}}}}},
			In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: sym("?x")}},
			Where: []Clause{scopePattern("?x", "page/next", "?y")},
		}
		_, err := Prepare(outerOver(inner), nil)
		if err == nil || !strings.Contains(err.Error(), "(pull ...) is not supported inside a subquery find") {
			t.Fatalf("Prepare = %v, want the refusal of the inner pull", err)
		}
	})

	t.Run("an inner query with neither is accepted", func(t *testing.T) {
		inner := findIn([]string{"?y"}, []InputSpec{ScalarInput{Symbol: sym("?x")}}, scopePattern("?x", "page/next", "?y"))
		if _, err := Prepare(outerOver(inner), nil); err != nil {
			t.Fatalf("Prepare = %v, want the inner query accepted", err)
		}
	})
}

func TestPrepare_RunsTheOrderByRule(t *testing.T) {
	t.Run("a sort key nothing binds is refused", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil, scopePattern("?e", "page/kind", "_"))
		q.OrderBy = []OrderByClause{{Variable: sym("?z")}}
		_, err := Prepare(q, nil)
		if err == nil || !strings.Contains(err.Error(), "order-by variable ?z is not bound in the query") {
			t.Fatalf("Prepare = %v, want the refusal of a sort key nothing binds", err)
		}
	})

	t.Run("a sort key under an aggregate that is no group key is refused", func(t *testing.T) {
		q := &Query{
			Find:    []FindElement{FindAggregate{Function: datalog.SymCount, Arg: sym("?e")}},
			In:      []InputSpec{DatabaseInput{Name: datalog.SymDollar}},
			Where:   []Clause{scopePattern("?e", "page/kind", "?k")},
			OrderBy: []OrderByClause{{Variable: sym("?k")}},
		}
		_, err := Prepare(q, nil)
		if err == nil || !strings.Contains(err.Error(), "order-by variable ?k is not a group key") {
			t.Fatalf("Prepare = %v, want the refusal of a sort key that is no group key", err)
		}
	})

	t.Run("a sort key the :where binds is accepted", func(t *testing.T) {
		q := findIn([]string{"?e"}, nil, scopePattern("?e", "page/kind", "?k"))
		q.OrderBy = []OrderByClause{{Variable: sym("?k")}}
		if _, err := Prepare(q, nil); err != nil {
			t.Fatalf("Prepare = %v, want a sort key the :where binds accepted", err)
		}
	})
}
