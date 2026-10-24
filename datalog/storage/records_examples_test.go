package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The examples the records state run as stated: DATOMIC_COMPATIBILITY.md's
// fallback, inference, refusal and input examples, QUERY_BUILDER.md's or-join
// forms, RULES.md's refusal of a rule body's undeclared attribute, the
// expression binding forms, the three subquery binding spellings, the
// planner's ordering of a pattern before the expression reading it, the
// instant a transaction is opened at, and the fork on each backend.

// prepared parses src and prepares it under no rule set.
func prepared(t *testing.T, src string) *query.Query {
	t.Helper()
	q, err := parser.ParseQuery(src)
	require.NoError(t, err)
	p, err := query.Prepare(q, nil)
	require.NoError(t, err)
	return p
}

// refusedAtAnyBoundary parses src and prepares it, and returns the first
// refusal.
func refusedAtAnyBoundary(src string) error {
	q, err := parser.ParseQuery(src)
	if err != nil {
		return err
	}
	_, err = query.Prepare(q, nil)
	return err
}

func TestDatomicCompatibilityExamples_RunAsStated(t *testing.T) {
	p1, q1, q2 := datalog.NewIdentity("p1"), datalog.NewIdentity("q1"), datalog.NewIdentity("q2")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(p1, kw(":person/name"), "Ann"))
			require.NoError(t, tx.Add(datalog.NewIdentity("p2"), kw(":person/age"), int64(3)))
			require.NoError(t, tx.Add(datalog.NewIdentity("t1"), kw(":task/done"), true))
			require.NoError(t, tx.Add(datalog.NewIdentity("t2"), kw(":task/done"), false))
			require.NoError(t, tx.Add(q1, kw(":t/n"), int64(1)))
			require.NoError(t, tx.Add(q1, kw(":t/tag"), kw(":t/none")))
			require.NoError(t, tx.Add(q2, kw(":t/n"), int64(2)))
			_, err := tx.Commit()
			require.NoError(t, err)
			run := func(src string, inputs ...interface{}) [][]interface{} {
				t.Helper()
				tuples, err := executor.CollectTuples(d.Query(src, inputs...))
				require.NoError(t, err)
				return tuples
			}

			require.Equal(t, [][]interface{}{{"Ann"}}, run(`[:find ?name :where (or-default [?e :person/name ?name] [(ground "Unknown") ?name])]`), "the first branch answers, so the default never fires")
			require.Equal(t, [][]interface{}{{int64(1)}}, run(`[:find ?count :where (or-default [(q [:find (count ?t) :where [?t :task/done true]] $) [[?count]]] [(ground 0) ?count])]`), "the zero-default count")

			orExample := `[:find ?q :where [?q :t/n _] (or [?q ?a ?v] [?q :t/tag :t/none]) [(ground :t/tag) ?a]]`
			require.Contains(t, prepared(t, orExample).String(), "(or-join [[?a] ?q]", "the shared variable one branch does not bind is required")
			require.Equal(t, [][]interface{}{{q1}}, run(orExample))

			require.Error(t, refusedAtAnyBoundary(`[:find ?x :where [?x :t/n _] (or [?y :t/n 1] [?y :t/n 2])]`), "a form sharing nothing is refused")
			require.Error(t, refusedAtAnyBoundary(`[:find ?x :where [?x :t/n ?n] [?x :t/tag ?g] (not-join [?x ?g] [?x :t/n 5])]`), "a not-join header variable the body neither binds nor reads is refused")
			branch := refusedAtAnyBoundary(`[:find ?x :where [?x :t/n ?n] (or-join [?x ?y] [?x :t/tag ?y] [?x :t/n 2])]`)
			require.ErrorContains(t, branch, "branch 2", "an or-join output a branch does not bind names the branch")

			for _, src := range []string{
				`[:find ?c :where [(q [:find (count ?t) :where [?t :task/done _]] :limit 1] $) [[?c]]]]`,
				`[:find ?c :where [(q [:find ?t :where [?t :task/done _] :limit 1] $) [[?c]]]]`,
			} {
				_, err := d.Query(src)
				require.Error(t, err, "a :limit inside a subquery is refused")
			}

			require.Equal(t, [][]interface{}{{"Ann"}}, run(`[:find ?n . :where [?e :person/name ?n]]`), "a scalar find returns the one value")
			require.Equal(t, [][]interface{}{{q1}}, run(`[:find ?e :in $ [?n ?v] :where [?e :t/n ?n]]`, []interface{}{int64(1), int64(9)}), "a tuple input")
			require.ElementsMatch(t, [][]interface{}{{q1}, {q2}}, run(`[:find ?e :in $ [[?n]] :where [?e :t/n ?n]]`, [][]interface{}{{int64(1)}, {int64(2)}}), "a relation input")
			require.ElementsMatch(t, [][]interface{}{{q1}, {q2}}, run(`[:find ?e :in $ [[?n] ...] :where [?e :t/n ?n]]`, [][]interface{}{{int64(1)}, {int64(2)}}), "the relation input's other spelling")

			rendered := prepared(t, `[:find ?e :in $ [[?n]] [?a ?b] :where [?e :t/n ?n]]`).String()
			require.Contains(t, rendered, "[[?n] ...]", "a relation input renders with its trailing dots")
			require.Contains(t, rendered, "[?a ?b]", "a tuple input renders as the tuple")

			for _, src := range []string{
				`[:find ?e :in $ [?n 3] :where [?e :t/n ?n]]`,
				`[:find ?e :in $ [[?n :k]] :where [?e :t/n ?n]]`,
				`[:find ?e :in $ [[?n :k] ...] :where [?e :t/n ?n]]`,
				`[:find ?e :in $ [:k ...] :where [?e :t/n ?n]]`,
			} {
				require.Error(t, refusedAtAnyBoundary(src), "an :in element that is not a variable is refused: %s", src)
			}
		})
	}
}

// The planner orders a pattern before the expression reading its vector, so
// enumerate written before the pattern answers as enumerate written after it
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1).
func TestEnumerate_AnswersAlikeWithThePatternWrittenAfterIt(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":character/skills").Type(schema.TypeString).Vector().Add().Build()
	require.NoError(t, err)
	want := [][]interface{}{{int64(0), "stealth"}, {int64(1), "archery"}}
	for _, mode := range optimizerModes {
		if mode.backend.Name == "memory" {
			continue
		}
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			tx := d.NewTransaction()
			for _, v := range []string{"stealth", "archery"} {
				require.NoError(t, tx.Add(datalog.NewIdentity("alice"), kw(":character/skills"), v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, src := range []string{
				`[:find ?i ?t :where [?e :character/skills ?vec] [(enumerate ?vec) [[?i ?t] ...]]]`,
				`[:find ?i ?t :where [(enumerate ?vec) [[?i ?t] ...]] [?e :character/skills ?vec]]`,
			} {
				tuples, err := executor.CollectTuples(d.Query(src))
				require.NoError(t, err, src)
				require.ElementsMatch(t, want, tuples, src)
			}
		})
	}
}

// An expression's binding reads its function's value under each of the four
// forms, a value of another shape is refused, a flat-header fallback decides
// once, a tuple input and a one-tuple relation input answer alike, and a
// subquery's scalar binding binds its one value
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7). The flat-header
// case's tuples are logged for the assertion that follows the observation.
func TestExpressionBindingForms_AnswerAsTheRecordsState(t *testing.T) {
	s1, s2 := datalog.NewIdentity("s1"), datalog.NewIdentity("s2")
	for _, mode := range optimizerModes {
		if mode.backend.Name == "memory" {
			continue
		}
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(s1, kw(":s/status"), kw(":on")))
			require.NoError(t, tx.Add(s2, kw(":s/kind"), kw(":x")))
			_, err := tx.Commit()
			require.NoError(t, err)
			run := func(src string, inputs ...interface{}) ([][]interface{}, error) {
				return executor.CollectTuples(d.Query(src, inputs...))
			}
			answers := []struct {
				name, src string
				inputs    []interface{}
				want      [][]interface{}
			}{
				{"the tuple form", `[:find ?n ?s :where [(ground [1 "x"]) [?n ?s]]]`, nil, [][]interface{}{{int64(1), "x"}}},
				{"the collection form", `[:find ?n :where [(ground [1 2 3]) [?n ...]]]`, nil, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
				{"the relation form", `[:find ?n ?s :where [(ground [[1 "x"] [2 "y"]]) [[?n ?s] ...]]]`, nil, [][]interface{}{{int64(1), "x"}, {int64(2), "y"}}},
				{"enumerate under the tuple form, each variable one pair", `[:find ?i ?t :where [(ground [:a :b]) ?vec] [(enumerate ?vec) [?i ?t]]]`, nil, [][]interface{}{{[]interface{}{int64(0), kw(":a")}, []interface{}{int64(1), kw(":b")}}}},
				{"enumerate written before the ground it reads", `[:find ?i ?t :where [(enumerate ?vec) [[?i ?t] ...]] [(ground [:a :b]) ?vec]]`, nil, [][]interface{}{{int64(0), kw(":a")}, {int64(1), kw(":b")}}},
				{"a tuple input", `[:find ?e :in $ [?a ?v] :where [?e ?a ?v]]`, []interface{}{[]interface{}{kw(":s/status"), kw(":on")}}, [][]interface{}{{s1}}},
				{"a relation input of one tuple", `[:find ?e :in $ [[?a ?v]] :where [?e ?a ?v]]`, []interface{}{[][]interface{}{{kw(":s/status"), kw(":on")}}}, [][]interface{}{{s1}}},
				{"a subquery's scalar binding", `[:find ?e ?c :where [?e :s/kind _] [(q [:find (count ?x) :in $ :where [?x :s/status _]] $) ?c]]`, nil, [][]interface{}{{s2, int64(1)}}},
			}
			for _, a := range answers {
				t.Run(a.name, func(t *testing.T) {
					tuples, err := run(a.src, a.inputs...)
					require.NoError(t, err)
					require.ElementsMatch(t, a.want, tuples)
				})
			}
			t.Run("a flat-header fallback decides once", func(t *testing.T) {
				tuples, err := run(`[:find ?e ?st :where [?e :s/kind _] (or-default-join [?st] [?x :s/status ?st] [(ground :unknown) ?st])]`)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{s2, kw(":on")}}, tuples, "the first branch answers once over the unit relation, and the find reads its output beside the entity :s/kind binds")
			})
			for _, src := range []string{
				`[:find ?n ?s :where [(ground [1 2 3]) [?n ?s]]]`,
				`[:find ?i ?t :where [(ground [:a :b :c]) ?vec] [(enumerate ?vec) [?i ?t]]]`,
			} {
				_, err := run(src)
				require.Error(t, err, "a value of another shape than the binding is refused: %s", src)
			}
		})
	}
}

// A subquery binding answers the relation over its symbol under every
// spelling, the scalar, the collection and the relation form alike, whatever
// the inner result's size (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7).
func TestSubqueryBinding_EverySpellingAnswersTheRelation(t *testing.T) {
	o := datalog.NewIdentity("o")
	want := [][]interface{}{{o, int64(0)}, {o, int64(1)}, {o, int64(2)}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for i, n := range []string{"a", "b", "c"} {
				require.NoError(t, tx.Add(datalog.NewIdentity(n), kw(":t/n"), int64(i)))
			}
			require.NoError(t, tx.Add(o, kw(":t/o"), true))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, src := range []string{
				`[:find ?o ?v :where [?o :t/o true] [(q [:find ?n :in $ :where [_ :t/n ?n]] $) ?v]]`,
				`[:find ?o ?v :where [?o :t/o true] [(q [:find ?n :in $ :where [_ :t/n ?n]] $) [?v ...]]]`,
				`[:find ?o ?v :where [?o :t/o true] [(q [:find ?n :in $ :where [_ :t/n ?n]] $) [[?v]]]]`,
			} {
				tuples, err := executor.CollectTuples(d.Query(src))
				require.NoError(t, err, src)
				require.ElementsMatch(t, want, tuples, src)
			}
		})
	}
}

// A commit's :db/txInstant is the time the transaction was opened at when
// the caller gave one.
func TestTxInstant_IsTheTimeTheTransactionWasOpenedAt(t *testing.T) {
	at := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransactionAt(at)
			require.NoError(t, tx.Add(datalog.NewIdentity("a"), kw(":t/n"), int64(1)))
			_, err := tx.Commit()
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(d.Query(`[:find ?t :where [?tx :db/txInstant ?t]]`))
			require.NoError(t, err)
			require.Len(t, tuples, 1, "one commit, one mark")
			stamped, ok := tuples[0][0].(time.Time)
			require.True(t, ok, "the mark's value is an instant, got %T", tuples[0][0])
			require.True(t, at.Equal(stamped), "the mark is the time the transaction was opened at, got %v", stamped)
		})
	}
}

// A memory-trees store forks a branch and rewinds; every other store this
// build has, the badger store among them, has no way to start a branch and
// refuses to fork, naming the store
// (docs/proposals/RULES_WITNESSES_AND_SPECULATIVE_VALUES.md §0).
func TestFork_MemoryTreesForksAndABadgerStoreRefuses(t *testing.T) {
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			d := backendDB(t, backend, DatabaseOptions{})
			defer d.Close()
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("a"), kw(":t/n"), int64(1)))
			_, err := tx.Commit()
			require.NoError(t, err)
			_, err = d.Snapshot("s1")
			require.NoError(t, err, "a snapshot is taken on every backend")
			b, err := d.Fork("s1")
			if backend.Name != "memory-trees" {
				require.ErrorContains(t, err, "cannot fork", "a store with no way to start a branch refuses loudly")
				require.Nil(t, b)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, b)
			require.NoError(t, d.TruncateTo("s1"))
		})
	}
}

// QUERY_BUILDER.md's or-join forms build to the written header forms and
// answer: a flat header of outputs, and a required run of a scalar input.
func TestQueryBuilderOrJoinExamples_BuildAndAnswer(t *testing.T) {
	ann, bob, cy := datalog.NewIdentity("ann"), datalog.NewIdentity("bob"), datalog.NewIdentity("cy")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(ann, kw(":person/city"), "NYC"))
			require.NoError(t, tx.Add(bob, kw(":person/active"), true))
			require.NoError(t, tx.Add(cy, kw(":person/city"), "LA"))
			_, err := tx.Commit()
			require.NoError(t, err)

			personCity, personActive := qb.Kw(":person/city"), qb.Kw(":person/active")
			e, city := qb.NewVar("e"), qb.NewVar("city")
			form1 := qb.OrJoin(e, city).
				Branch(qb.Pat(e, personCity, city), qb.Eq(city, "NYC")).
				Branch(qb.Pat(e, personCity, city), qb.Eq(city, "LA"))
			form2 := qb.OrJoin(e).Require(city).
				Branch(qb.Pat(e, personCity, city)).
				Branch(qb.Pat(e, personActive, qb.V(true)))

			q1, err := qb.Query().Find(e, city).Where(form1).Build()
			require.NoError(t, err)
			require.Contains(t, q1.String(), "(or-join [?e ?city]")
			tuples, err := executor.CollectTuples(d.Query(q1))
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{ann, "NYC"}, {cy, "LA"}}, tuples)

			q2, err := qb.Query().Find(e).In(qb.DB, qb.Scalar(city)).Where(form2).Build()
			require.NoError(t, err)
			require.Contains(t, q2.String(), "(or-join [[?city] ?e]")
			tuples, err = executor.CollectTuples(d.Query(q2, "NYC"))
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{ann}, {bob}}, tuples)
		})
	}
}

// A refusing database refuses a rule body's undeclared attribute under a free
// invocation and under a bound one (docs/reference/RULES.md).
func TestRefusingDatabase_RefusesARuleBodysUndeclaredAttributeUnderFreeAndBoundInvocation(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":edge").Type(schema.TypeRef).Many().Add().Build()
	require.NoError(t, err)
	a, b := datalog.NewIdentity("a"), datalog.NewIdentity("b")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, RefuseUnknownAttributes: true})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(a, kw(":edge"), b))
			_, err := tx.Commit()
			require.NoError(t, err)
			rs := mustRuleSet(t, `[[(reach ?a ?b) [?a :edge ?b]] [(reach ?a ?b) [?a :nope ?b]]]`)
			_, err = executor.CollectTuples(d.Query(`[:find ?x ?y :in $ % :where (reach ?x ?y)]`, rs))
			requireUnknown(t, err, kw(":nope"))
			_, err = executor.CollectTuples(d.Query(`[:find ?y :in $ % ?x :where (reach ?x ?y)]`, rs, a))
			requireUnknown(t, err, kw(":nope"))
		})
	}
}
