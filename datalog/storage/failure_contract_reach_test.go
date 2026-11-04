package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A correlated subquery whose aggregate cannot fold one group's value fails
// the query where every other clause holds, through every reader: a :limit, the
// existence verdict, QueryOneInto, a not-join body and an or-join branch, under
// sum and avg, on both optimizer paths. Sixteen groups hold numbers, and one
// holds a string beside a number, so whichever group a reader takes first, the
// failing one is among those left (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md
// §1, the failure contract).
func TestAnAggregatingSubqueryWhoseAggregateFailsOnAValueFailsTheQueryThroughEveryReader(t *testing.T) {
	name := datalog.NewKeyword(":item/name")
	groupName := datalog.NewKeyword(":grp/name")
	memberOf := datalog.NewKeyword(":mem/grp")
	value := datalog.NewKeyword(":mem/v")
	subquery := `[(q [:find (%s ?v) :in $ ?g :where [?m :mem/grp ?g] [?m :mem/v ?v]] $ ?g) [[?s]]]`
	cases := []struct {
		name  string
		query string
	}{
		{"a limit over the subquery's group",
			`[:find ?g ?s :where [?g :grp/name _] ` + subquery + ` :limit 1]`},
		{"the existence verdict over the subquery's group",
			`[:find ?p :where [?p :item/name _] [?g :grp/name _] ` + subquery + `]`},
		{"a not-join body over a group the find does not read",
			`[:find ?p :where [?p :item/name _] [?g :grp/name _] (not-join [?g] ` + subquery + ` [(< ?s 0)])]`},
		{"an or-join branch over a group the find does not read",
			`[:find ?p :where [?p :item/name _] [?g :grp/name _] (or-join [[?g]] (and ` + subquery + ` [(> ?s -1)]))]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("aggregate-reach:alice"), name, "Alice"))
			for i := 0; i < 16; i++ {
				g := datalog.NewIdentity(fmt.Sprintf("aggregate-reach:g%d", i))
				require.NoError(t, tx.Add(g, groupName, fmt.Sprintf("g%d", i)))
				for j := 0; j < 2; j++ {
					m := datalog.NewIdentity(fmt.Sprintf("aggregate-reach:g%d-m%d", i, j))
					require.NoError(t, tx.Add(m, memberOf, g))
					require.NoError(t, tx.Add(m, value, int64(i+j)))
				}
			}
			stringy := datalog.NewIdentity("aggregate-reach:stringy")
			require.NoError(t, tx.Add(stringy, groupName, "stringy"))
			for j, v := range []interface{}{"x", int64(1)} {
				m := datalog.NewIdentity(fmt.Sprintf("aggregate-reach:stringy-m%d", j))
				require.NoError(t, tx.Add(m, memberOf, stringy))
				require.NoError(t, tx.Add(m, value, v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, aggregate := range []string{"sum", "avg"} {
				for _, c := range cases {
					text := fmt.Sprintf(c.query, aggregate)
					t.Run(aggregate+"/"+c.name, func(t *testing.T) {
						got, err := executor.CollectTuples(d.Query(text))
						require.ErrorContains(t, err, "sum/avg input", "the query answered %d tuples with no error", len(got))
					})
				}
				t.Run(aggregate+"/QueryOneInto through the existence verdict", func(t *testing.T) {
					var one struct {
						P datalog.Identity `datalog:"?p"`
					}
					found, err := d.QueryOneInto(&one, fmt.Sprintf(cases[1].query, aggregate))
					require.ErrorContains(t, err, "sum/avg input", "QueryOneInto answered found %v with no error", found)
				})
			}
		})
	}
}

// A windowed form whose subject is another form's output decides the failed
// tuple its subject answers at the tuple's key, as a form over a failing
// clause does. The failing expression stands in the first form's subject. The
// first form holds at the failing tuple, a not-join, a subquery, an or-join or
// an or-default-join, and the second form, a subquery or a not-join, holds at
// no assignment extending it, so no failure is raised and the query answers
// the numeric items. Where the second form holds at the failing tuple, the
// failure is the query's error (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md
// §1, the failure contract, and T13).
func TestAFormOverAnotherFormsOutputDecidesAFailedTupleAtItsKey(t *testing.T) {
	v := datalog.NewKeyword(":item/v")
	kind := datalog.NewKeyword(":item/kind")
	tag := datalog.NewKeyword(":item/tag")
	firsts := []struct{ name, clause string }{
		{"a not-join", `(not-join [?q] [?q :item/banned true])`},
		{"a subquery", `[(q [:find ?k :in $ ?q :where [?q :item/kind ?k]] $ ?q) [[?k]]]`},
		{"an or-join", `(or-join [[?q]] [?q :item/kind "str"] [?q :item/tag _])`},
		{"an or-default-join", `(or-default-join [[?q] ?d] [?q :item/kind ?d] [(ground "none") ?d])`},
		{"a pattern", `[?q :item/kind ?kk]`},
	}
	seconds := []struct{ name, clause string }{
		{"a subquery holding nowhere at the failing tuple", `[(q [:find ?t :in $ ?q :where [?q :item/tag ?t]] $ ?q) [[?t]]]`},
		{"a not-join holding nowhere at the failing tuple", `(not-join [?q] [?q :item/kind "str"])`},
		{"a pattern holding nowhere at the failing tuple", `[?q :item/tag ?tt]`},
	}
	holding := []struct{ name, clause string }{
		{"a subquery holding at the failing tuple", `[(q [:find ?k2 :in $ ?q :where [?q :item/kind ?k2]] $ ?q) [[?k2]]]`},
		{"a not-join holding at the failing tuple", `(not-join [?q] [?q :item/banned true])`},
		{"a pattern holding at the failing tuple", `[?q :item/kind ?k3]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for i := 0; i < 16; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("form-chain:n%d", i))
				require.NoError(t, tx.Add(e, v, int64(i)))
				require.NoError(t, tx.Add(e, kind, "num"))
				require.NoError(t, tx.Add(e, tag, "t"))
			}
			stringy := datalog.NewIdentity("form-chain:stringy")
			require.NoError(t, tx.Add(stringy, v, "x"))
			require.NoError(t, tx.Add(stringy, kind, "str"))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, first := range firsts {
				for _, second := range seconds {
					text := `[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] ` + first.clause + ` ` + second.clause + `]`
					t.Run(first.name+" then "+second.name, func(t *testing.T) {
						got, err := executor.CollectTuples(d.Query(text))
						require.NoError(t, err, "the second form holds at no assignment extending the failing tuple, so its failure is not raised")
						require.Len(t, got, 16, "each numeric item is answered")
					})
				}
				for _, second := range holding {
					text := `[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] ` + first.clause + ` ` + second.clause + `]`
					t.Run(first.name+" then "+second.name, func(t *testing.T) {
						got, err := executor.CollectTuples(d.Query(text))
						require.ErrorContains(t, err, "is not a number", "the query answered %d tuples with no error", len(got))
					})
				}
			}
		})
	}
}

// A correlated subquery whose aggregate cannot fold one group's value fails
// the query through a limit of zero, on both optimizer paths, where an outer
// tuple reaches the failing group. A limit of zero reads a relation that
// evaluates to its end, as every limit does. A failing group no outer tuple
// reaches is the reproducer of
// BUG_OPTIMIZER_PATHS_ANSWER_DIFFERENTLY_WHEN_A_CORRELATED_SUBQUERY_FAILS_ON_A_VALUE.md
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1, the failure contract, and
// T11).
func TestACorrelatedAggregateFailingAtOneGroupAnswersByTheFailureContractOnBothOptimizerPaths(t *testing.T) {
	groupName := datalog.NewKeyword(":grp/name")
	memberOf := datalog.NewKeyword(":mem/grp")
	value := datalog.NewKeyword(":mem/v")
	subquery := `[(q [:find (%s ?v) :in $ ?g :where [?m :mem/grp ?g] [?m :mem/v ?v]] $ ?g) [[?s]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for i := 0; i < 16; i++ {
				g := datalog.NewIdentity(fmt.Sprintf("aggregate-contract:g%d", i))
				require.NoError(t, tx.Add(g, groupName, fmt.Sprintf("g%d", i)))
				for j := 0; j < 2; j++ {
					m := datalog.NewIdentity(fmt.Sprintf("aggregate-contract:g%d-m%d", i, j))
					require.NoError(t, tx.Add(m, memberOf, g))
					require.NoError(t, tx.Add(m, value, int64(i+j)))
				}
			}
			stringy := datalog.NewIdentity("aggregate-contract:stringy")
			require.NoError(t, tx.Add(stringy, groupName, "stringy"))
			for j, v := range []interface{}{"x", int64(1)} {
				m := datalog.NewIdentity(fmt.Sprintf("aggregate-contract:stringy-m%d", j))
				require.NoError(t, tx.Add(m, memberOf, stringy))
				require.NoError(t, tx.Add(m, value, v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, aggregate := range []string{"sum", "avg"} {
				t.Run(aggregate+"/a limit of zero over a reached failing group fails the query", func(t *testing.T) {
					text := fmt.Sprintf(`[:find ?g ?s :where [?g :grp/name _] `+subquery+` :limit 0]`, aggregate)
					got, err := executor.CollectTuples(d.Query(text))
					require.ErrorContains(t, err, "sum/avg input", "the query answered %d tuples with no error", len(got))
				})
			}
		})
	}
}

// A correlated subquery whose body cannot evaluate at one key fails the query,
// on both optimizer paths, where an outer tuple reaches that key: read whole,
// or through a limit of zero, which reads a relation that evaluates to its end
// as every limit does. The fixture is built so the body fails at one item
// alone, whose :item/v is a string. A failing key no outer tuple reaches is
// the reproducer of
// BUG_OPTIMIZER_PATHS_ANSWER_DIFFERENTLY_WHEN_A_CORRELATED_SUBQUERY_FAILS_ON_A_VALUE.md
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1, the failure contract, and
// T11).
func TestACorrelatedSubqueryWhoseBodyFailsAtOneKeyAnswersByTheFailureContractOnBothOptimizerPaths(t *testing.T) {
	v := datalog.NewKeyword(":item/v")
	subquery := `[(q [:find ?b :in $ ?q :where [?q :item/v ?a] [(+ ?a 1) ?b]] $ ?q) [[?b]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for i := 0; i < 16; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("body-contract:n%d", i))
				require.NoError(t, tx.Add(e, v, int64(i)))
			}
			require.NoError(t, tx.Add(datalog.NewIdentity("body-contract:stringy"), v, "x"))
			_, err := tx.Commit()
			require.NoError(t, err)
			t.Run("a failing key an outer tuple reaches fails the query", func(t *testing.T) {
				got, err := executor.CollectTuples(d.Query(`[:find ?q ?b :where [?q :item/v _] ` + subquery + `]`))
				require.ErrorContains(t, err, "is not a number", "the query answered %d tuples with no error", len(got))
			})
			t.Run("a limit of zero over a reached failing key fails the query", func(t *testing.T) {
				got, err := executor.CollectTuples(d.Query(`[:find ?q ?b :where [?q :item/v _] ` + subquery + ` :limit 0]`))
				require.ErrorContains(t, err, "is not a number", "the query answered %d tuples with no error", len(got))
			})
		})
	}
}
