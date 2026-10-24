package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A query over a rule set answers or is refused by the shape of its
// invocations and its own clauses, on both planner paths
// (docs/reference/RULES.md, docs/proposals/NESTED_SCOPES_AS_QUERIES.md
// §2.4): a branch reading a variable nothing binds is refused as a query and
// as a rule body, an invocation at the wrong arity or of an undefined
// predicate, an engine-namespace name among them, is refused as the error
// named for it, a required position is bound by a constant and refused under
// a blank, a free variable or a variable repeated at both positions, a query
// declaring % twice is refused, and a subquery passing % under a :in
// declaring none is refused.
func TestRules_InvocationShapesAtAQueryAnswerOrAreRefused(t *testing.T) {
	const succ = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`
	const reachOverA = `[[(reach ?a ?b) [?a :a ?b]]]`
	arity := func(err error) bool { var e *query.RuleArityError; return errors.As(err, &e) }
	unknown := func(err error) bool { var e *query.UnknownRuleError; return errors.As(err, &e) }
	unsafe := func(err error) bool { var e *query.UnsafeRuleError; return errors.As(err, &e) }
	plain := func(err error) bool { return err != nil }
	branchBodies := []struct{ name, body string }{
		{"a predicate's input under a not", `[?x :a _] (not [?x :b ?v] [(> ?v ?w)])`},
		{"a predicate's input under an or-default", `[?x :a _] (or-default (and [?x :b ?v] [(> ?v ?w)]) [?x :a _])`},
		{"an expression's input under a not", `[?x :a _] (not [(+ ?w 1) ?v] [?x :b ?v])`},
		{"a subquery's input under an or-default", `[?x :a _] (or-default (and [?x :b ?v] [(q [:find ?z :in $ ?w :where [?z :a ?w]] $ ?w) [[?z]]]) [?x :a _])`},
	}
	cases := []struct {
		name, text string
		inputs     []interface{}
		refused    func(error) bool
		message    string
		want       []string
	}{
		{"an invocation at the wrong arity", `[:find ?x :in $ % :where (reach ?x)]`, []interface{}{reachOverA}, arity, "", nil},
		{"an invocation of a predicate the set does not define", `[:find ?x :in $ % :where (nope ?x)]`, []interface{}{reachOverA}, unknown, "", nil},
		{"an invocation of the delta namespace", `[:find ?x ?y :in $ % :where (delta/reach ?x ?y)]`, []interface{}{reachOverA}, unknown, "", nil},
		{"an invocation of the magic namespace", `[:find ?y :in $ % :where (magic.bf/reach ?y)]`, []interface{}{reachOverA}, unknown, "", nil},
		{"a blank at a required position", `[:find ?m :in $ % :where (succ _ ?m)]`, []interface{}{succ}, plain, "", nil},
		{"a constant at a required position", `[:find ?m :in $ % :where (succ 4 ?m)]`, []interface{}{succ}, nil, "", []string{"5"}},
		{"a free variable at a required position", `[:find ?m :in $ % :where (succ ?n ?m)]`, []interface{}{succ}, plain, "?n", nil},
		{"one free variable at both positions", `[:find ?n :in $ % :where (succ ?n ?n)]`, []interface{}{succ}, plain, "?n", nil},
		{"a query declaring % twice", `[:find ?x ?y :in $ % % :where (reach ?x ?y)]`, []interface{}{reachOverA, reachOverA}, plain, "at most once", nil},
		{"a query declaring % twice given a non-rule-set second", `[:find ?x ?y :in $ % % :where (reach ?x ?y)]`, []interface{}{reachOverA, int64(42)}, plain, "at most once", nil},
		{"a subquery passing % under a :in declaring none", `[:find ?y ?n :in $ :where [?y :a _] [(q [:find (count ?z) :in $ % ?y :where (reach ?y ?z)] $ % ?y) [[?n]]]]`, nil, plain, "requires % in :in", nil},
	}
	for _, b := range branchBodies {
		cases = append(cases,
			struct {
				name, text string
				inputs     []interface{}
				refused    func(error) bool
				message    string
				want       []string
			}{b.name + " nothing binds, as a query", `[:find ?x :where ` + b.body + `]`, nil, plain, "?w", nil},
			struct {
				name, text string
				inputs     []interface{}
				refused    func(error) bool
				message    string
				want       []string
			}{b.name + " nothing binds, as a rule body", `[:find ?x :in $ % :where (r ?x)]`, []interface{}{`[[(r ?x) ` + b.body + `]]`}, unsafe, "?w", nil},
		)
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := map[datalog.Identity]string{}
			tx := d.NewTransaction()
			for i := int64(1); i <= 3; i++ {
				id := datalog.NewIdentity("e" + string(rune('0'+i)))
				names[id] = "e" + string(rune('0'+i))
				require.NoError(t, tx.Add(id, kw(":a"), i))
				require.NoError(t, tx.Add(id, kw(":b"), i))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					if c.refused != nil {
						_, err := executor.CollectTuples(d.Query(c.text, c.inputs...))
						require.Error(t, err)
						require.True(t, c.refused(err), "the refusal is the error named for it: %v", err)
						if c.message != "" {
							require.Contains(t, err.Error(), c.message)
						}
						return
					}
					rel, err := d.Query(c.text, c.inputs...)
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, renderedRows(t, rel, names))
				})
			}
		})
	}
}
