package parser

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The records' examples at the text boundaries hold as stated: RULES.md's
// refusals of an engine namespace, of recursion under an aggregating inner
// query or a fallback form, and of a clause using the rule set with no %
// input; and NESTED_SCOPES_AS_QUERIES.md's refusal of a one-branch fallback
// and of a rule whose branch reads a variable nothing binds.

// refusedBeforePlanning parses src and prepares it under no rule set,
// returning the first refusal.
func refusedBeforePlanning(src string) error {
	q, err := ParseQuery(src)
	if err != nil {
		return err
	}
	_, err = query.Prepare(q, nil)
	return err
}

// preparedText parses src and prepares it under no rule set, returning the
// prepared query's rendering.
func preparedText(t *testing.T, src string) string {
	t.Helper()
	q, err := ParseQuery(src)
	require.NoError(t, err)
	p, err := query.Prepare(q, nil)
	require.NoError(t, err)
	return p.String()
}

// A one-branch or-join is that branch over its header, a one-branch or
// infers to it, the worked examples of NESTED_SCOPES_AS_QUERIES.md §2.3
// prepare to the headers it states, and a relation binding renders with its
// trailing dots on an expression and on a subquery.
func TestPrepare_ShapesTheFormsAsTheDesignStates(t *testing.T) {
	require.Contains(t, preparedText(t, `[:find ?x :where [?x :a _] (or-join [?x] [?x :b 1])]`), "(or-join [?x]", "an or-join of one branch is that branch over the header")
	require.Contains(t, preparedText(t, `[:find ?x :where [?x :a _] (or [?x :b 1])]`), "(or-join [?x]", "an or of one branch infers to the one-branch or-join")
	require.Contains(t, preparedText(t, `[:find ?category ?count :where [?c :category/name ?category] (or-default [(q [:find (count ?p) :in $ ?c :where [?p :product/category ?c]] $ ?c) [?count ...]] [(ground 0) ?count])]`), "(or-default-join [[?c] ?count]", "the zero-default count's header")
	require.Contains(t, preparedText(t, `[:find ?e :where [?w :p ?e] [?y :q ?e] (or-default [?e :p ?m] (and [?m :p ?e] [?y ?a ?m]))]`), "(or-default-join [[?y] ?e]", "the joined output's header")
	require.Contains(t, preparedText(t, `[:find ?e :where [?e :p _] (not [?e :q ?x])]`), "(not-join [?e]", "the existential's header")
	require.Contains(t, preparedText(t, `[:find ?a ?b :where [(ground [[1 2]]) [[?a ?b]]]]`), "[[?a ?b] ...]", "an expression's relation binding renders with its trailing dots")
	require.Contains(t, preparedText(t, `[:find ?a :where [(q [:find ?x :where [?x :p _]] $) [[?a]]]]`), "[[?a] ...]", "a subquery's relation binding renders with its trailing dots")
}

// The engine mints predicates in four namespaces, adorned, magic, delta and
// scope (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.4), a rule set naming
// a predicate in any of them is refused, and the refusal names all four.
func TestParseRules_RefusesAPredicateInAnEngineNamespaceNamingEveryEngineNamespace(t *testing.T) {
	for _, head := range []string{"adorned/p", "magic/p", "delta/p", "scope/p", "scope/required"} {
		t.Run(head, func(t *testing.T) {
			_, err := ParseRules(fmt.Sprintf(`[[(%s ?a) [?a :edge _]]]`, head))
			require.Error(t, err, "a predicate in an engine namespace is refused")
			for _, namespace := range []string{"adorned", "magic", "delta", "scope"} {
				require.ErrorContains(t, err, namespace, "the refusal names every namespace the engine mints predicates in")
			}
		})
	}
}

func TestParseRules_AdmitsRecursionInsideASubqueryOnlyWhenItDoesNotAggregate(t *testing.T) {
	_, err := ParseRules(`[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) [?a :edge ?b] [(q [:find ?c :in $ % ?b :where (r ?b ?c)] $ % ?b) [[?c]]]]]`)
	require.NoError(t, err, "recursion inside a subquery whose inner query does not aggregate is admitted")
	_, err = ParseRules(`[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) [?a :edge ?b] [(q [:find (count ?c) :in $ % ?b :where (r ?b ?c)] $ % ?b) [[?c]]]]]`)
	require.Error(t, err, "recursion inside an aggregating inner query is refused")
	_, err = ParseRules(`[[(r ?a ?b) [?a :edge ?b]] [(r ?a ?c) [?a :edge ?b] (or-default (r ?b ?c) [(ground 0) ?c])]]`)
	require.Error(t, err, "recursion inside a fallback form is refused")
}

func TestParseQuery_RefusesAClauseUsingTheRuleSetWithNoRulesInput(t *testing.T) {
	for _, src := range []string{
		`[:find ?y :in $ ?s :where (reach ?s ?y)]`,
		`[:find ?x ?n :in $ :where [?x :edge _] [(q [:find (count ?y) :in $ % ?x :where (reach ?x ?y)] $ % ?x) [[?n]]]]`,
		`[:find ?x :in $ :where [?x :edge _] (not (reach ?x ?x))]`,
	} {
		_, err := ParseQuery(src)
		require.ErrorContains(t, err, "%", "a clause using the rule set requires % in :in: %s", src)
	}
}

func TestPrepare_RefusesAOneBranchFallbackAndAnUnsafeRule(t *testing.T) {
	require.Error(t, refusedBeforePlanning(`[:find ?x :where [?x :a _] (or-default-join [?x] [?x :b 1])]`), "an or-default-join needs two branches")
	require.Error(t, refusedBeforePlanning(`[:find ?x :where [?x :a _] (or-default [?x :b 1])]`), "an or-default needs two branches")

	_, err := ParseRules(`[[(r ?a) [?a :n _] (or (and [?a :n ?v] [(> ?v ?w)]) [?a :m _])]]`)
	var unsafe *query.UnsafeRuleError
	require.True(t, errors.As(err, &unsafe), "a rule whose branch reads a variable nothing binds is the unsafe rule, got %v", err)
}
