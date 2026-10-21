package qb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A constant argument of each value type the builder accepts renders
// through FormatQuery to text the parser reads back as the same clause, and
// that clause renders to the same text: an identity, a string carrying
// escapes, a float, a bool, an int, a keyword, an instant and bytes.
func TestRuleConstantArgumentsRoundTrip(t *testing.T) {
	x := NewVar("x")
	for _, value := range []interface{}{
		datalog.NewIdentity("e1"),
		"a\"b\\c\n",
		1.5,
		true,
		int64(7),
		datalog.NewKeyword(":k/w"),
		time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
		[]byte{1, 2},
	} {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			q := Query().Find(x).In(DB, Rules).Where(Pat(x, Kw(":a"), Blank()), Rule("r", x, value)).MustBuild()
			text := parser.FormatQuery(q)
			parsed, err := parser.ParseQuery(text)
			if err != nil {
				t.Fatalf("ParseQuery(%q): %v", text, err)
			}
			if !reflect.DeepEqual(parsed.Where[1], q.Where[1]) {
				t.Errorf("built %#v, parsed %#v from %q", q.Where[1], parsed.Where[1], text)
			}
			if again := parser.FormatQuery(parsed); again != text {
				t.Errorf("the text changed on a second render:\n%s\n--\n%s", text, again)
			}
		})
	}
}

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

// A subquery whose inner query declares Rules passes % through, as the
// parsed form (q [...] $ % ?x) does, so an invocation stands in a built
// subquery as it stands in a parsed one.
func TestSubqueryPassesRulesThrough(t *testing.T) {
	x, y, n := NewVar("x"), NewVar("y"), NewVar("n")
	inner := Query().Find(Count(y)).In(DB, Rules, Scalar(x)).Where(Rule("reach", x, y))
	q := Query().Find(x, n).In(DB, Rules).Where(Pat(x, edge, Blank()), Subquery(inner, x).BindRelation(n)).MustBuild()
	parsed, err := parser.ParseQuery(`[:find ?x ?n :in $ %
	                                   :where [?x :edge _]
	                                          [(q [:find (count ?y) :in $ % ?x :where (reach ?x ?y)] $ % ?x) [[?n]]]]`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.Where, parsed.Where) {
		t.Errorf("built :where\n%v\ndiffers from parsed\n%v", q.Where, parsed.Where)
	}
}

// A subquery's inputs follow the inner query's :in in order, a source or %
// at its own position and the caller's variables at the value positions, as
// the parsed form (q [...] $ ?x %) has them. A tuple or relation input takes
// one caller per symbol, so a % after one follows every caller it takes, as
// the parsed form (q [...] $ ?a ?b %) has them. A caller variable count that
// differs from the inner query's value positions is refused at Build.
func TestSubqueryInputsFollowTheInnerDeclaration(t *testing.T) {
	x, y, n := NewVar("x"), NewVar("y"), NewVar("n")
	inner := Query().Find(Count(y)).In(DB, Scalar(x), Rules).Where(Rule("reach", x, y))
	q := Query().Find(x, n).In(DB, Rules).Where(Pat(x, edge, Blank()), Subquery(inner, x).BindRelation(n)).MustBuild()
	parsed, err := parser.ParseQuery(`[:find ?x ?n :in $ %
	                                   :where [?x :edge _]
	                                          [(q [:find (count ?y) :in $ ?x % :where (reach ?x ?y)] $ ?x %) [[?n]]]]`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.Where, parsed.Where) {
		t.Errorf("built :where\n%v\ndiffers from parsed\n%v", q.Where, parsed.Where)
	}

	a, b, c := NewVar("a"), NewVar("b"), NewVar("c")
	pairs := Query().Find(Count(c)).In(DB, Relation(a, b), Rules).Where(Rule("reach", a, c), Rule("reach", b, c))
	q = Query().Find(a, b, n).In(DB, Rules).Where(Pat(a, edge, b), Subquery(pairs, a, b).BindRelation(n)).MustBuild()
	parsed, err = parser.ParseQuery(`[:find ?a ?b ?n :in $ %
	                                  :where [?a :edge ?b]
	                                         [(q [:find (count ?c) :in $ [[?a ?b] ...] % :where (reach ?a ?c) (reach ?b ?c)] $ ?a ?b %) [[?n]]]]`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.Where, parsed.Where) {
		t.Errorf("a relation input takes both callers before the %% that follows it: built :where\n%v\ndiffers from parsed\n%v", q.Where, parsed.Where)
	}

	_, err = Query().Find(x, n).In(DB, Rules).Where(Pat(x, edge, Blank()), Subquery(inner, x, y).BindRelation(n)).Build()
	if err == nil || !strings.Contains(err.Error(), "subquery") {
		t.Errorf("two caller variables for one value position are refused naming the subquery, got %v", err)
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
