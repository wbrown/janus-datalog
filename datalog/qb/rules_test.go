package qb

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The rule builders produce what the parser produces: Rules is the % input,
// Rule an invocation clause, and DefineRule with RuleSet a rule set equal to
// its parsed text, validated by the same NewRuleSet.

var edge = Kw(":edge")

const closureText = `[[(reach ?a ?b) [?a :edge ?b]]
                      [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]`

func TestRulesInputSpec(t *testing.T) {
	if _, ok := Rules.toInputSpec().(query.RulesInput); !ok {
		t.Fatalf("expected RulesInput, got %T", Rules.toInputSpec())
	}

	x, y := NewVar("x"), NewVar("y")
	q := Query().Find(x, y).In(DB, Rules).Where(Rule("reach", x, y)).MustBuild()
	parsed, err := parser.ParseQuery(`[:find ?x ?y :in $ % :where (reach ?x ?y)]`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.In, parsed.In) {
		t.Errorf("built :in %v differs from parsed %v", q.In, parsed.In)
	}
	if !reflect.DeepEqual(q.Where, parsed.Where) {
		t.Errorf("built :where %v differs from parsed %v", q.Where, parsed.Where)
	}
}

func TestRuleClause(t *testing.T) {
	x := NewVar("x")
	clause := Rule("reach", x, "lit", Blank()).toClause()
	parsed, err := parser.ParseQuery(`[:find ?x :in $ % :where (reach ?x "lit" _)]`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(clause, parsed.Where[0]) {
		t.Errorf("built invocation %v differs from parsed %v", clause, parsed.Where[0])
	}
}

func TestDefineRuleAndRuleSet(t *testing.T) {
	a, b, c := NewVar("a"), NewVar("b"), NewVar("c")
	built, err := RuleSet(
		DefineRule("reach", a, b).Where(Pat(a, edge, b)),
		DefineRule("reach", a, c).Where(Pat(a, edge, b), Rule("reach", b, c)),
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseRules(closureText)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(built.Predicates(), parsed.Predicates()) {
		t.Errorf("built predicates %v differ from parsed %v", built.Predicates(), parsed.Predicates())
	}
	reach := datalog.NewSymbol("reach")
	if !reflect.DeepEqual(built.Rules(reach), parsed.Rules(reach)) {
		t.Errorf("built rules %v differ from parsed %v", built.Rules(reach), parsed.Rules(reach))
	}
	if !reflect.DeepEqual(built.Strata(), parsed.Strata()) {
		t.Errorf("built strata %v differ from parsed %v", built.Strata(), parsed.Strata())
	}
}

// TestDefineRuleWithRequiredRun: Require names the head variables the
// caller binds, and the built rule equals its parsed text,
// [(succ [?n] ?m) ...].
func TestDefineRuleWithRequiredRun(t *testing.T) {
	n, m := NewVar("n"), NewVar("m")
	built, err := RuleSet(DefineRule("succ", n, m).Require(n).Where(Add(n, int64(1)).As(m)))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseRules(`[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`)
	if err != nil {
		t.Fatal(err)
	}
	succ := datalog.NewSymbol("succ")
	if !reflect.DeepEqual(built.Rules(succ), parsed.Rules(succ)) {
		t.Errorf("built rules %v differ from parsed %v", built.Rules(succ), parsed.Rules(succ))
	}
	if !reflect.DeepEqual(built.Required(succ), parsed.Required(succ)) {
		t.Errorf("built required run %v differs from parsed %v", built.Required(succ), parsed.Required(succ))
	}

	_, err = RuleSet(DefineRule("succ", n, m).Require(m).Where(Add(n, int64(1)).As(m)))
	if err == nil || !strings.Contains(err.Error(), "succ") {
		t.Errorf("a required variable that does not lead the head is refused naming the rule, got %v", err)
	}
}

func TestRuleSetValidationErrorsSurface(t *testing.T) {
	a, b, c := NewVar("a"), NewVar("b"), NewVar("c")

	unsafeRule := DefineRule("reach", a, b).Where(Pat(a, edge, c))
	_, err := RuleSet(unsafeRule)
	var unsafeErr *query.UnsafeRuleError
	if !errors.As(err, &unsafeErr) {
		t.Fatalf("expected an UnsafeRuleError, got %v", err)
	}
	if unsafeErr.Rule != datalog.NewSymbol("reach") || unsafeErr.Symbol != b.Symbol() {
		t.Errorf("error names rule %s and symbol %s, want reach and ?b", unsafeErr.Rule, unsafeErr.Symbol)
	}

	_, err = RuleSet(DefineRule("reach", a, b).Where(42))
	if err == nil || !strings.Contains(err.Error(), "cannot convert int to Clause") {
		t.Errorf("expected the body clause conversion error, got %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Error("MustRuleSet did not panic on an unsafe rule")
		}
	}()
	MustRuleSet(unsafeRule)
}
