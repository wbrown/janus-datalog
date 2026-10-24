package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A refusing source refuses a query whose text names, in the attribute
// position of a data pattern on that source at any depth, inner queries and
// rule bodies included, in a pull pattern, or in a database function, an
// attribute the source's schema does not declare, the default source and a
// named source alike; an inner query's pattern names its attribute on the
// source the call passes at the inner source's place, so a pattern on the
// inner $ given a named source is checked by that source, and the default
// source declaring the schema without the option refuses nothing of it. The
// same route carrying a declared keyword answers (docs/reference/SCHEMA.md,
// "Unknown attributes are refused on request").
func TestQueryRefusesUnknownAttributeReachingThroughEveryRoute(t *testing.T) {
	routes := []struct {
		name, text, rules string
		declaringDefault  bool
	}{
		{"a constant on the default source", `[:find ?v :where [?e :t/KEY ?v]]`, "", false},
		{"a constant on a named source", `[:find ?v :in $ $other :where [$other ?e :t/KEY ?v]]`, "", false},
		{"a constant inside a subquery body on the default source", `[:find ?v :where [(q [:find ?v :in $ :where [?e :t/KEY ?v]] $) [[?v]]]]`, "", false},
		{"a constant inside a subquery body on a named inner source given a named source", `[:find ?v :in $ $other :where [(q [:find ?v :in $t :where [$t ?e :t/KEY ?v]] $other) [[?v]]]]`, "", false},
		{"a constant inside a subquery body on the inner default source given a named source", `[:find ?v :in $ $other :where [(q [:find ?v :in $ :where [?e :t/KEY ?v]] $other) [[?v]]]]`, "", true},
		{"a constant inside a subquery body under an omitted inner :in given a named source", `[:find ?v :in $ $other :where [(q [:find ?v :where [?e :t/KEY ?v]] $other) [[?v]]]]`, "", true},
		{"a constant inside a nested call's body given a named source passed on", `[:find ?v :in $ $other :where [(q [:find ?v :in $t :where [(q [:find ?w :in $u :where [$u ?e :t/KEY ?w]] $t) [[?v]]]] $other) [[?v]]]]`, "", false},
		{"a constant in a rule body on the default source", `[:find ?v :in $ % :where (rn ?v)]`, `[[(rn ?v) [?e :t/KEY ?v]]]`, false},
		{"a constant in a rule body on a named source", `[:find ?v :in $ $other % :where (rn ?v)]`, `[[(rn ?v) [$other ?e :t/KEY ?v]]]`, false},
		{"a constant in a rule body's subquery body", `[:find ?v :in $ % :where (rn ?v)]`, `[[(rn ?v) [(q [:find ?v :in $ :where [?e :t/KEY ?v]] $) [[?v]]]]]`, false},
		{"a pull pattern in :find", `[:find (pull ?e [:t/KEY]) :where [?e :t/string _]]`, "", false},
		{"a database function", `[:find ?v :where [?e :t/string _] [(get-else $ ?e :t/KEY "none") ?v]]`, "", false},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			other, _ := refusingDB(t, mode)
			declaring := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			for _, route := range routes {
				t.Run(route.name, func(t *testing.T) {
					on := d
					if route.declaringDefault {
						on = declaring
					}
					run := func(attr datalog.Keyword) error {
						text := strings.ReplaceAll(route.text, ":t/KEY", attr.String())
						var inputs []interface{}
						if route.rules != "" {
							inputs = append(inputs, mustRuleSet(t, strings.ReplaceAll(route.rules, ":t/KEY", attr.String())))
						}
						if strings.Contains(text, "$other") {
							inputs = append(inputs, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$other"): other}))
						}
						rel, err := on.Query(text, inputs...)
						if err != nil {
							return err
						}
						_, err = executor.CollectTuples(rel, nil)
						return err
					}
					requireUnknown(t, run(kw(":t/strnig")), kw(":t/strnig"))
					require.NoError(t, run(kw(":t/string")), "the declared keyword through the same route answers")
				})
			}
		})
	}
}

// A keyword that reaches a pattern's attribute position through a variable is
// a value, wherever it comes from, and a refusing source refuses nothing of
// it: the pattern evaluates over the store, answering no tuple for a keyword
// no datom carries and the holder's value for a declared one, through every
// binder the language admits, on the default source and on a named source
// (docs/reference/SCHEMA.md, "Unknown attributes are refused on request").
func TestQueryRunsAKeywordReachingAnAttributePositionAsAValue(t *testing.T) {
	routes := []struct {
		name, text, rules string
		input             bool
	}{
		{"an input on the default source", `[:find ?v :in $ ?a :where [?e ?a ?v]]`, "", true},
		{"an input on a named source", `[:find ?v :in $ $other ?a :where [$other ?e ?a ?v]]`, "", true},
		{"a ground on the default source", `[:find ?v :where [(ground :t/KEY) ?a] [?e ?a ?v]]`, "", false},
		{"a ground on a named source", `[:find ?v :in $ $other :where [(ground :t/KEY) ?a] [$other ?e ?a ?v]]`, "", false},
		{"an identity alias of an input on the default source", `[:find ?v :in $ ?b :where [(identity ?b) ?a] [?e ?a ?v]]`, "", true},
		{"an identity alias of an input on a named source", `[:find ?v :in $ $other ?b :where [(identity ?b) ?a] [$other ?e ?a ?v]]`, "", true},
		{"a subquery call's constant argument", `[:find ?v :where [(q [:find ?v :in $ ?attr :where [?e ?attr ?v]] $ :t/KEY) [[?v]]]]`, "", false},
		{"an or-join output on the default source", `[:find ?v :where (or-join [?a] [(ground :t/KEY) ?a] [(ground :t/long) ?a]) [?e ?a ?v]]`, "", false},
		{"an or-join output on a named source", `[:find ?v :in $ $other :where (or-join [?a] [(ground :t/KEY) ?a] [(ground :t/long) ?a]) [$other ?e ?a ?v]]`, "", false},
		{"an or-default output on the default source", `[:find ?v :where (or-default [(ground :t/KEY) ?a] [(ground :t/long) ?a]) [?e ?a ?v]]`, "", false},
		{"a subquery's relation binding", `[:find ?v :where [(q [:find ?a :in $ :where [(ground :t/KEY) ?a]] $) [[?a]]] [?e ?a ?v]]`, "", false},
		{"a subquery's collection binding", `[:find ?v :where [(q [:find ?a :in $ :where [(ground :t/KEY) ?a]] $) [?a ...]] [?e ?a ?v]]`, "", false},
		{"a form's output inside a subquery's find", `[:find ?v :where [(q [:find ?a :in $ :where (or-join [?a] [(ground :t/KEY) ?a] [(ground :t/long) ?a])] $) [[?a]]] [?e ?a ?v]]`, "", false},
		{"a rule's head", `[:find ?v :in $ % :where (attr ?a) [?e ?a ?v]]`, `[[(attr ?a) [(ground :t/KEY) ?a]]]`, false},
		{"a rule's head through another rule", `[:find ?v :in $ % :where (attr2 ?a) [?e ?a ?v]]`, `[[(attr ?a) [(ground :t/KEY) ?a]] [(attr2 ?b) (attr ?b)]]`, false},
		{"a rule's head on a named source", `[:find ?v :in $ $other % :where (attr ?a) [$other ?e ?a ?v]]`, `[[(attr ?a) [(ground :t/KEY) ?a]]]`, false},
		{"a constant passed to a rule's head", `[:find ?v :in $ % :where (has :t/KEY ?v)]`, `[[(has ?a ?v) [?e ?a ?v]]]`, false},
		{"a rule body's head variable on a named source", `[:find ?v :in $ $other % :where (rn :t/KEY ?v)]`, `[[(rn [?a] ?v) [$other ?e ?a ?v]]]`, false},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			other, _ := refusingDB(t, mode)
			for _, route := range routes {
				t.Run(route.name, func(t *testing.T) {
					run := func(attr datalog.Keyword) ([][]interface{}, error) {
						text := strings.ReplaceAll(route.text, ":t/KEY", attr.String())
						var inputs []interface{}
						if route.rules != "" {
							inputs = append(inputs, mustRuleSet(t, strings.ReplaceAll(route.rules, ":t/KEY", attr.String())))
						}
						if route.input {
							inputs = append(inputs, attr)
						}
						if strings.Contains(text, "$other") {
							inputs = append(inputs, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$other"): other}))
						}
						return executor.CollectTuples(d.Query(text, inputs...))
					}
					tuples, err := run(kw(":t/strnig"))
					require.NoError(t, err, "a keyword reaching the attribute position through a variable is a value")
					require.Empty(t, tuples, "no datom carries the keyword")
					tuples, err = run(kw(":t/string"))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"held"}}, tuples, "the declared keyword through the same route answers the holder's value")
				})
			}
		})
	}
}
