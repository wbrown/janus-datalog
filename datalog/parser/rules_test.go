package parser

import (
	"errors"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A rule set is an EDN vector of rules, each [(name ?args...) clause...],
// and ParseRules parses it into a validated query.RuleSet. In a query, % in
// :in is the rules input and a list in :where whose head names no reserved
// form is a rule invocation; an invocation needs % in its own query's :in.

const followsRulesText = `[[(follows ?a ?b) [?a :page/next ?b]]
                          [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
                          [(marked ?p ?mark) [?p :page/mark ?mark]]]`

func TestParseRules(t *testing.T) {
	rs, err := ParseRules(followsRulesText)
	if err != nil {
		t.Fatalf("ParseRules: %v", err)
	}
	preds := rs.Predicates()
	if len(preds) != 2 || preds[0] != datalog.NewSymbol("follows") || preds[1] != datalog.NewSymbol("marked") {
		t.Fatalf("Predicates = %v, want follows then marked", preds)
	}
	follows := rs.Rules(datalog.NewSymbol("follows"))
	if len(follows) != 2 {
		t.Fatalf("Rules(follows) has %d rules, want 2", len(follows))
	}
	if len(follows[0].Head) != 2 || follows[0].Head[0] != datalog.NewSymbol("?a") || follows[0].Head[1] != datalog.NewSymbol("?b") {
		t.Errorf("first follows head = %v, want ?a ?b", follows[0].Head)
	}
	if len(follows[0].Body) != 1 {
		t.Errorf("first follows body has %d clauses, want 1", len(follows[0].Body))
	}
	if _, ok := follows[0].Body[0].(*query.DataPattern); !ok {
		t.Errorf("first follows body clause is %T, want a data pattern", follows[0].Body[0])
	}
	if len(follows[1].Body) != 2 {
		t.Fatalf("second follows body has %d clauses, want 2", len(follows[1].Body))
	}
	inv, ok := follows[1].Body[1].(*query.RuleInvocation)
	if !ok {
		t.Fatalf("second follows body clause 2 is %T, want a rule invocation", follows[1].Body[1])
	}
	if inv.Name != datalog.NewSymbol("follows") || len(inv.Args) != 2 {
		t.Errorf("invocation = %s, want (follows ?b ?c)", inv)
	}
	if strata := rs.Strata(); len(strata) != 2 || !strata[0].Recursive || strata[1].Recursive {
		t.Errorf("strata = %+v, want the follows closure then marked", strata)
	}
}

func TestParseRules_RefusesMalformedRules(t *testing.T) {
	cases := []struct {
		name  string
		text  string
		error string
	}{
		{"not a vector", `{}`, "rule set must be a vector"},
		{"rule not a vector", `[(follows ?a ?b)]`, "a rule is a vector"},
		{"head not a list", `[[follows [?a :page/next ?b]]]`, "rule head must be a list"},
		{"head name a variable", `[[(?f ?a ?b) [?a :page/next ?b]]]`, "rule name must be a symbol"},
		{"required run not first", `[[(succ ?m [?n]) [(+ ?n 1) ?m]]]`, "required run"},
		{"required run empty", `[[(succ [] ?m) [(+ ?n 1) ?m]]]`, "required run"},
		{"required run holding a non-variable", `[[(succ [:n] ?m) [(+ ?n 1) ?m]]]`, "required run"},
		{"head argument not a variable", `[[(follows ?a :page/next) [?a :page/next ?b]]]`, "head argument must be a variable"},
		{"empty body", `[[(follows ?a ?b)]]`, "at least one body clause"},
		{"unsafe", `[[(follows ?a ?b) [?a :page/next _]]]`, "is unsafe"},
		{"name in the engine's namespace", `[[(delta/follows ?a ?b) [?a :page/next ?b]]]`, "delta/follows"},
		{"name in the magic namespace", `[[(magic.bf/reach ?a) [(ground 1) ?a]]]`, "magic.bf/reach"},
		{"name in the adorned namespace", `[[(adorned.bf/e ?a ?b) [?x :edge/from ?a] [?x :edge/to ?b]]]`, "adorned.bf/e"},
		{"name in the scope namespace", `[[(scope/required ?a) [(ground 1) ?a]]]`, "scope/required"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseRules(c.text)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.error)
			}
			if !contains(err.Error(), c.error) {
				t.Errorf("expected error containing %q, got %q", c.error, err.Error())
			}
		})
	}
	_, err := ParseRules(`[[(follows ?a ?b) [?a :page/next ?b]] [(follows ?a) [?a :page/next _]]]`)
	var arity *query.RuleArityError
	if !errors.As(err, &arity) {
		t.Errorf("heads disagreeing on arity: want *query.RuleArityError, got %v", err)
	}
}

// TestParseRules_RequiredBindingHead: a head whose first argument is a
// vector of variables, [(succ [?n] ?m) ...], declares those variables as the
// required run, leading the head.
func TestParseRules_RequiredBindingHead(t *testing.T) {
	rs, err := ParseRules(`[[(succ [?n] ?m) [(+ ?n 1) ?m]]
	                        [(between [?lo ?hi] ?x) [?x :page/n ?v] [(< ?lo ?v ?hi)]]]`)
	if err != nil {
		t.Fatalf("ParseRules: %v", err)
	}
	succ := rs.Rules(datalog.NewSymbol("succ"))
	if len(succ) != 1 {
		t.Fatalf("Rules(succ) has %d rules, want 1", len(succ))
	}
	if len(succ[0].Head) != 2 || succ[0].Head[0] != datalog.NewSymbol("?n") || succ[0].Head[1] != datalog.NewSymbol("?m") {
		t.Errorf("succ head = %v, want ?n ?m", succ[0].Head)
	}
	if len(succ[0].Required) != 1 || succ[0].Required[0] != datalog.NewSymbol("?n") {
		t.Errorf("succ required run = %v, want ?n", succ[0].Required)
	}
	if required := rs.Required(datalog.NewSymbol("succ")); len(required) != 2 || !required[0] || required[1] {
		t.Errorf("Required(succ) = %v, want position 0 required", required)
	}
	between := rs.Rules(datalog.NewSymbol("between"))[0]
	if len(between.Required) != 2 || len(between.Head) != 3 || between.Head[2] != datalog.NewSymbol("?x") {
		t.Errorf("between = %s, want the run ?lo ?hi leading the head ?lo ?hi ?x", between)
	}
	if got, want := succ[0].String(), "[(succ [?n] ?m) [(+ ?n 1) ?m]]"; got != want {
		t.Errorf("String() = %s, want %s", got, want)
	}
}

func TestParseQuery_RulesInputAndInvocation(t *testing.T) {
	q, err := ParseQuery(`[:find ?y
	                       :in $ % ?x
	                       :where (follows ?x ?y)
	                              (marked ?y "read" _)]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	if len(q.In) != 3 {
		t.Fatalf("In has %d specs, want 3", len(q.In))
	}
	if _, ok := q.In[1].(query.RulesInput); !ok {
		t.Errorf("In[1] is %T, want RulesInput", q.In[1])
	}
	if len(q.Where) != 2 {
		t.Fatalf("Where has %d clauses, want 2", len(q.Where))
	}
	first, ok := q.Where[0].(*query.RuleInvocation)
	if !ok {
		t.Fatalf("Where[0] is %T, want *RuleInvocation", q.Where[0])
	}
	if first.Name != datalog.NewSymbol("follows") {
		t.Errorf("Where[0] = %s, want (follows ?x ?y)", first)
	}
	if len(first.Args) != 2 || first.Args[0] != query.PatternElement(query.Variable{Name: datalog.NewSymbol("?x")}) {
		t.Errorf("Where[0] args = %v, want ?x ?y", first.Args)
	}
	second, ok := q.Where[1].(*query.RuleInvocation)
	if !ok {
		t.Fatalf("Where[1] is %T, want *RuleInvocation", q.Where[1])
	}
	if len(second.Args) != 3 {
		t.Fatalf("Where[1] has %d args, want 3", len(second.Args))
	}
	if c, ok := second.Args[1].(query.Constant); !ok || c.Value != "read" {
		t.Errorf("Where[1] arg 2 = %v, want the constant \"read\"", second.Args[1])
	}
	if !second.Args[2].IsBlank() {
		t.Errorf("Where[1] arg 3 = %v, want a blank", second.Args[2])
	}
}

func TestParseQuery_InvocationRequiresRulesInput(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"top level", `[:find ?y :in $ ?x :where (follows ?x ?y)]`},
		{"inside not-join", `[:find ?x :where [?x :page/kind _] (not-join [?x] (follows ?x _))]`},
		{"subquery with its own :in", `[:find ?z
		                               :in $ %
		                               :where [?y :page/kind _]
		                                      [(q [:find ?z :in $ ?y :where (follows ?y ?z)] $ ?y) [[?z] ...]]]`},
		{"inside an or branch", `[:find ?x :where [?x :page/kind _] (or (follows ?x _) [?x :page/next _])]`},
		{"subquery passing % the outer query lacks", `[:find ?z
		                                             :in $
		                                             :where [?y :page/kind _]
		                                                    [(q [:find ?z :in $ % ?y :where (follows ?y ?z)] $ % ?y) [[?z] ...]]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseQuery(c.text)
			if err == nil {
				t.Fatal("expected an error naming the invocation and %")
			}
			if !contains(err.Error(), "(follows") || !contains(err.Error(), "% in :in") {
				t.Errorf("error %q must name the invocation and the missing %% input", err.Error())
			}
		})
	}
}

func TestParseQuery_InvocationInsideForms(t *testing.T) {
	q, err := ParseQuery(`[:find ?x ?z
	                       :in $ %
	                       :where [?x :page/kind _]
	                              (or-join [?x ?z]
	                                (follows ?x ?z)
	                                [?x :page/next ?z])
	                              (not-join [?z] (marked ?z "skip"))
	                              [(q [:find ?w :in $ % ?z :where (follows ?z ?w)] $ % ?z) [[?w] ...]]]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	found := query.Invocations(q.Where)
	if len(found) != 3 {
		t.Fatalf("found %d invocations, want 3 (or-join branch, not-join body, subquery body): %v", len(found), found)
	}
	names := []string{"follows", "marked", "follows"}
	for i, inv := range found {
		if inv.Name != datalog.NewSymbol(names[i]) {
			t.Errorf("invocation %d is %s, want %s", i, inv, names[i])
		}
	}
	sub := q.Where[3].(*query.SubqueryPattern)
	if len(sub.Query.In) != 3 {
		t.Fatalf("subquery :in has %d specs, want 3", len(sub.Query.In))
	}
	if _, ok := sub.Query.In[1].(query.RulesInput); !ok {
		t.Errorf("subquery In[1] is %T, want RulesInput", sub.Query.In[1])
	}
	if c, ok := sub.Inputs[1].(query.Constant); !ok || c.Value != datalog.NewSymbol("%") {
		t.Errorf("subquery input 2 = %v, want the %% symbol passed through", sub.Inputs[1])
	}
}

func TestParseQuery_VariableHeadedListIsRefused(t *testing.T) {
	_, err := ParseQuery(`[:find ?e :where (?e :page/kind ?v)]`)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !contains(err.Error(), "rule invocation name must be a symbol") || !contains(err.Error(), "?e") {
		t.Errorf("error %q must say the invocation name is a variable", err.Error())
	}
}

func TestQueryString_RoundTripsInvocation(t *testing.T) {
	q, err := ParseQuery(`[:find ?y :in $ % ?x :where (follows ?x ?y) (marked ?y "read" _)]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	text := q.String()
	again, err := ParseQuery(text)
	if err != nil {
		t.Fatalf("ParseQuery(q.String()) failed on %q: %v", text, err)
	}
	if again.String() != q.String() {
		t.Errorf("round trip changed the query:\n%s\n--\n%s", q.String(), again.String())
	}
	if _, ok := again.In[1].(query.RulesInput); !ok {
		t.Errorf("round-tripped In[1] is %T, want RulesInput", again.In[1])
	}
	if _, ok := again.Where[1].(*query.RuleInvocation); !ok {
		t.Errorf("round-tripped Where[1] is %T, want *RuleInvocation", again.Where[1])
	}
}

// TestParseQuery_ReadsMintedPredicateNames: the predicates the engine mints
// are symbols in a namespace of its own, so the text of a body that invokes
// them, and a magic rule's fresh variable, is Datalog the parser reads.
func TestParseQuery_ReadsMintedPredicateNames(t *testing.T) {
	q, err := ParseQuery(`[:find ?a ?c
	                       :in $ %
	                       :where (magic.bf/follows ?a)
	                              [(ground 5) ?magic.bb/pair.1]
	                              (adorned.bf/follows ?a ?b)
	                              (delta.adorned.bf/follows ?b ?c)]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	names := map[int]string{0: "magic.bf/follows", 2: "adorned.bf/follows", 3: "delta.adorned.bf/follows"}
	for i, want := range names {
		inv, ok := q.Where[i].(*query.RuleInvocation)
		if !ok {
			t.Fatalf("Where[%d] is %T, want *RuleInvocation", i, q.Where[i])
		}
		if inv.Name != datalog.NewSymbol(want) {
			t.Errorf("Where[%d] invokes %s, want %s", i, inv.Name, want)
		}
	}
	expr, ok := q.Where[1].(*query.Expression)
	if !ok {
		t.Fatalf("Where[1] is %T, want *Expression", q.Where[1])
	}
	if bound, ok := expr.Binding.(query.Symbol); !ok || bound != datalog.NewSymbol("?magic.bb/pair.1") || !bound.IsVariable() {
		t.Errorf("Where[1] binds %v, want the variable ?magic.bb/pair.1", expr.Binding)
	}
}

func TestExtractVariablesRuleInvocation(t *testing.T) {
	q, err := ParseQuery(`[:find ?y :in $ % :where (marked ?y "read" ?y)]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	vars := ExtractVariables(q.Where)
	if len(vars) != 1 || vars[0] != datalog.NewSymbol("?y") {
		t.Errorf("ExtractVariables = %v, want ?y once", vars)
	}
}

// The strata of a set mixing recursion with negation and or-join: a chain
// of non-recursive predicates is one stratum per predicate in dependency
// order; a mutually recursive pair, a predicate negating one of them, and a
// self-recursive predicate over that are three strata in that order; and a
// negation inside an or-join that the negated predicate invokes back through
// an or closes a negative cycle, which is refused.
func TestParseRules_StrataOfMixedSets(t *testing.T) {
	stratumOf := func(t *testing.T, rs *query.RuleSet, name string) (int, bool) {
		t.Helper()
		for i, stratum := range rs.Strata() {
			if query.ContainsSymbol(stratum.Predicates, datalog.NewSymbol(name)) {
				return i, stratum.Recursive
			}
		}
		t.Fatalf("no stratum holds %s in %+v", name, rs.Strata())
		return -1, false
	}
	before := func(t *testing.T, rs *query.RuleSet, earlier, later string) {
		t.Helper()
		e, _ := stratumOf(t, rs, earlier)
		l, _ := stratumOf(t, rs, later)
		if e >= l {
			t.Errorf("%s (stratum %d) must come before %s (stratum %d)", earlier, e, later, l)
		}
	}

	chain, err := ParseRules(`[[(p ?x) (q ?x) (s ?x)] [(q ?x) (s ?x)] [(s ?x) [?x :a _]]]`)
	if err != nil {
		t.Fatalf("ParseRules(chain): %v", err)
	}
	if len(chain.Strata()) != 3 {
		t.Fatalf("the chain has %d strata, want one per predicate: %+v", len(chain.Strata()), chain.Strata())
	}
	for _, name := range []string{"p", "q", "s"} {
		if _, recursive := stratumOf(t, chain, name); recursive {
			t.Errorf("%s is marked recursive; nothing in the chain invokes itself", name)
		}
	}
	before(t, chain, "s", "q")
	before(t, chain, "q", "p")

	mixed, err := ParseRules(`[[(p ?x) (q ?x)] [(q ?x) (p ?x)] [(q ?x) [?x :a _]]
	                           [(u ?x) [?x :a _] (not (p ?x))]
	                           [(w ?x) (u ?x) (w ?x)] [(w ?x) [?x :b _]]]`)
	if err != nil {
		t.Fatalf("ParseRules(mixed): %v", err)
	}
	if len(mixed.Strata()) != 3 {
		t.Fatalf("the mixed set has %d strata, want the pair, the negation and the self-recursion: %+v", len(mixed.Strata()), mixed.Strata())
	}
	pStratum, pRecursive := stratumOf(t, mixed, "p")
	qStratum, _ := stratumOf(t, mixed, "q")
	if pStratum != qStratum || !pRecursive {
		t.Errorf("p and q invoke each other, so they share one recursive stratum; p is in %d and q in %d", pStratum, qStratum)
	}
	if _, recursive := stratumOf(t, mixed, "u"); recursive {
		t.Error("u invokes nothing in its own stratum and is marked recursive")
	}
	if _, recursive := stratumOf(t, mixed, "w"); !recursive {
		t.Error("w invokes itself and is not marked recursive")
	}
	before(t, mixed, "p", "u")
	before(t, mixed, "u", "w")

	_, err = ParseRules(`[[(p ?x) [?x :a _] (or-join [?x] (and (not (q ?x)) [?x :b _]) [?x :c _])]
	                      [(q ?x) [?x :a _] (or (p ?x) [?x :d _])]]`)
	var cycle *query.UnstratifiableError
	if !errors.As(err, &cycle) {
		t.Fatalf("p negates q inside its or-join and q invokes p inside its or: want *query.UnstratifiableError, got %v", err)
	}
	for _, name := range []string{"p", "q"} {
		if !query.ContainsSymbol(cycle.Predicates, datalog.NewSymbol(name)) {
			t.Errorf("the refusal names %v, want %s among them", cycle.Predicates, name)
		}
	}
}

// An invocation's arguments are variables, constants of every literal type,
// vector literals, blanks and entity literals, in a predicate whose name may
// carry a namespace, and the parsed query renders back to itself.
func TestParseQuery_InvocationArguments(t *testing.T) {
	q, err := ParseQuery(`[:find ?x :in $ % :where (my.ns/r ?x :k "s" 1 2.5 true _ #id "e1")]`)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	inv, ok := q.Where[0].(*query.RuleInvocation)
	if !ok {
		t.Fatalf("Where[0] is %T, want *RuleInvocation", q.Where[0])
	}
	if inv.Name != datalog.NewSymbol("my.ns/r") {
		t.Errorf("the predicate is %s, want my.ns/r", inv.Name)
	}
	want := []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?x")},
		query.Constant{Value: datalog.NewKeyword(":k")},
		query.Constant{Value: "s"},
		query.Constant{Value: int64(1)},
		query.Constant{Value: 2.5},
		query.Constant{Value: true},
		query.Blank{},
		query.Constant{Value: datalog.NewIdentity("e1")},
	}
	if len(inv.Args) != len(want) {
		t.Fatalf("the invocation has %d arguments, want %d: %v", len(inv.Args), len(want), inv.Args)
	}
	for i := range want {
		if inv.Args[i] != want[i] {
			t.Errorf("argument %d is %#v, want %#v", i, inv.Args[i], want[i])
		}
	}
	again, err := ParseQuery(q.String())
	if err != nil {
		t.Fatalf("ParseQuery(q.String()) failed on %q: %v", q.String(), err)
	}
	if again.String() != q.String() {
		t.Errorf("round trip changed the query:\n%s\n--\n%s", q.String(), again.String())
	}

	withVector, err := ParseQuery(`[:find ?x :in $ % :where (r ?x [1 2])]`)
	if err != nil {
		t.Fatalf("ParseQuery with a vector argument: %v", err)
	}
	vector, ok := withVector.Where[0].(*query.RuleInvocation).Args[1].(query.VectorConstant)
	if !ok || len(vector.Values) != 2 || vector.Values[0] != int64(1) || vector.Values[1] != int64(2) {
		t.Errorf("the vector argument parsed as %#v, want the vector constant [1 2]", withVector.Where[0].(*query.RuleInvocation).Args[1])
	}
}
