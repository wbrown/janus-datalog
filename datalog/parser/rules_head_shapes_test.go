package parser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A rule set is admitted or refused by its heads and names as
// docs/reference/RULES.md states: a head's arguments are distinct variables
// and at least one, every rule for one name has one arity and one required
// run, a name lies outside the engine's namespaces and is neither a clause
// form's nor a variable's, a rule binding not every variable it must is
// unsafe, an invocation of an undefined or misarity predicate, a cycle
// through negation and a recursion inside a fallback form are each refused as
// the error named for them, and a recursion inside a non-aggregating inner
// query is admitted.
func TestParseRules_AdmitsAndRefusesHeadsAndNamesAsStated(t *testing.T) {
	unsafe := func(err error) bool { var e *query.UnsafeRuleError; return errors.As(err, &e) }
	arity := func(err error) bool { var e *query.RuleArityError; return errors.As(err, &e) }
	unknown := func(err error) bool { var e *query.UnknownRuleError; return errors.As(err, &e) }
	unstratifiable := func(err error) bool { var e *query.UnstratifiableError; return errors.As(err, &e) }
	nested := func(err error) bool { var e *query.NestedRecursionError; return errors.As(err, &e) }
	plain := func(err error) bool { return err != nil }
	const succ = `[(succ [?n] ?m) [(+ ?n 1) ?m]]`
	cases := []struct {
		name, text string
		admitted   bool
		refused    func(error) bool
		message    string
	}{
		{"an existential under a not inside an or branch", `[[(r ?e) [?e :a _] (or (and [?e :b _] (not [?e :c ?z])) [?e :d _])]]`, true, nil, ""},
		{"a head variable only a not mentions", `[[(r ?x ?y) [?x :a _] (not [?x :b ?y])]]`, false, unsafe, "?y"},
		{"a predicate's input nothing binds", `[[(r ?x) [?x :a ?v] [(> ?v ?w)]]]`, false, unsafe, "?w"},
		{"a head variable one or branch binds and another does not", `[[(r ?x ?y) (or [?x :a ?y] [?x :b _])]]`, false, unsafe, "?y"},
		{"two rules of one name at two arities", `[[(r ?x) [?x :a _]] [(r ?x ?y) [?x :a ?y]]]`, false, arity, ""},
		{"an invocation of a predicate the set does not define", `[[(r ?x) (s ?x)]]`, false, unknown, "s"},
		{"two predicates negating each other", `[[(p ?x) [?x :a _] (not (q ?x))] [(q ?x) [?x :a _] (not (p ?x))]]`, false, unstratifiable, ""},
		{"a cycle through a not-join holding an or", `[[(p ?x) [?x :a _] (not-join [?x] (or (q ?x) [?x :b _]))] [(q ?x) [?x :a _] (p ?x)]]`, false, unstratifiable, ""},
		{"a predicate negating itself", `[[(p ?x) [?x :a _] (not (p ?x))]]`, false, unstratifiable, ""},
		{"a recursion inside an or-default", `[[(p ?x) [?x :a _]] [(p ?x) [?x :b _] (or-default (p ?x) [?x :c _])]]`, false, nested, ""},
		{"a recursion inside a non-aggregating subquery", `[[(p ?x) [?x :a _]] [(p ?x) [?y :b ?x] [(q [:find ?x :in $ % ?y :where (p ?y)] $ % ?y) [[?x]]]]]`, true, nil, ""},
		{"a name in the magic namespace", `[[(magic.bf/r ?x) [?x :a _]]]`, false, plain, "the engine's"},
		{"a name in the delta namespace", `[[(delta/r ?x) [?x :a _]]]`, false, plain, "the engine's"},
		{"a name in the adorned namespace", `[[(adorned/r ?x) [?x :a _]]]`, false, plain, "the engine's"},
		{"a required run", `[[(r [?x] ?y) [(+ ?x 1) ?y]]]`, true, nil, ""},
		{"an expression's input the head does not require", `[[(r ?x ?y) [(+ ?x 1) ?y]]]`, false, unsafe, "?x"},
		{"two rules of one name with two required runs", `[[(r [?x] ?y) [(+ ?x 1) ?y]] [(r ?x ?y) [?x :a ?y]]]`, false, plain, ""},
		{"a head requiring every variable", `[[(r [?x ?y]) [(> ?x ?y)]]]`, true, nil, ""},
		{"a head naming no variable", `[[(r) [?x :a _]]]`, false, plain, "a head names at least one variable"},
		{"a head naming no variable over a ground body", `[[(always) [(ground 1) ?one]]]`, false, plain, "a head names at least one variable"},
		{"a head naming no variable in a recursion", `[[(p) [?y :a 1]] [(p) (p) [?y :a 2]]]`, false, plain, "a head names at least one variable"},
		{"a body invocation whose required position nothing binds", `[` + succ + ` [(r ?y) [?y :a _] (s ?z ?y)] [(s [?x] ?y) [(+ ?x 1) ?y]]]`, false, unsafe, "?z"},
		{"a required position consumed under a not that nothing binds", `[` + succ + ` [(r ?v) [?x :a ?v] (not (succ ?z ?v))]]`, false, unsafe, "?z"},
		{"a required position consumed under an or-default that nothing binds", `[` + succ + ` [(r ?v) [?x :a ?v] (or-default (succ ?z ?v) [?x :a ?v])]]`, false, unsafe, "?z"},
		{"a predicate's input under a not that nothing binds", `[[(r ?x) [?x :a _] (not [?x :b ?v] [(> ?v ?w)])]]`, false, unsafe, "?w"},
		{"a predicate's input under an or-default that nothing binds", `[[(r ?x) [?x :a _] (or-default (and [?x :b ?v] [(> ?v ?w)]) [?x :a _])]]`, false, unsafe, "?w"},
		{"an expression's input under a not that nothing binds", `[[(r ?x) [?x :a _] (not [(+ ?w 1) ?v] [?x :b ?v])]]`, false, unsafe, "?w"},
		{"a subquery's input under an or-default that nothing binds", `[[(r ?x) [?x :a _] (or-default (and [?x :b ?v] [(q [:find ?z :in $ ?w :where [?z :a ?w]] $ ?w) [[?z]]]) [?x :a _])]]`, false, unsafe, "?w"},
		{"a head holding a constant", `[[(r 1) [?x :a _]]]`, false, plain, ""},
		{"a head naming one variable twice", `[[(r ?x ?x) [?x :a _]]]`, false, plain, ""},
		{"a name that is a variable", `[[(?r ?x) [?x :a _]]]`, false, plain, "a predicate's name is not a variable"},
		{"a name that is not's", `[[(not ?x) [?x :a _]]]`, false, plain, "heads a clause form"},
		{"a name that is or's", `[[(or ?x) [?x :a _]]]`, false, plain, "heads a clause form"},
		{"a name that is and's", `[[(and ?x) [?x :a _]]]`, false, plain, "heads a clause form"},
		{"a second required vector after the first argument", `[[(r [?x] [?y] ?z) [(+ ?x ?y) ?z]]]`, false, plain, "the required run [?x ...] leads the head"},
		{"an empty required run", `[[(r [] ?y) [?y :a _]]]`, false, plain, "the required run names at least one variable"},
		{"a name that is the subquery call's", `[[(q ?x) [?x :a _]]]`, true, nil, ""},
		{"a name that is a function's", `[[(ground ?x) [?x :a _]]]`, true, nil, ""},
		{"a name that is the default source's", `[[($ ?x) [?x :a _]]]`, true, nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rs, err := ParseRules(c.text)
			if c.admitted {
				require.NoError(t, err)
				require.NotNil(t, rs)
				return
			}
			require.Error(t, err)
			require.True(t, c.refused(err), "the refusal is the error named for it: %v", err)
			if c.message != "" {
				require.Contains(t, err.Error(), c.message)
			}
		})
	}
}
