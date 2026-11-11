package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// openFormScopeDB opens the transaction-typing database under mode and adds
// first's :item/next second and second's :item/one "y", so a form reading
// :item/next decides something: second is first's next, and first is no
// entity's next.
func openFormScopeDB(t *testing.T, mode optimizerMode) *Database {
	t.Helper()
	db, _ := openTransactionTypingDB(t, mode, false)
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("item:first"), datalog.NewKeyword(":item/next"), datalog.NewIdentity("item:second")))
	require.NoError(t, tx.Add(datalog.NewIdentity("item:second"), datalog.NewKeyword(":item/one"), "y"))
	_, err := tx.Commit()
	require.NoError(t, err)
	return db
}

// A variable a header form's branch holds as its own, one the header does
// not name, binds into no position of the enclosing scope, whatever its name.
// So an :in input that reaches a typed position only through such a variable
// of the head variable's name, in a rule's body or in the query's own :where,
// is not refused at the query's entry: it answers what the ground-bound
// argument answers. An input reaching the position through a variable the
// header names is refused, as one reaching it directly is.
func TestAnInputReachingAFormLocalThroughARuleArgumentIsNotRefused(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	second := datalog.NewIdentity("item:second")
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := openFormScopeDB(t, mode)

			for _, c := range []struct {
				name   string
				query  string
				ground string
				rules  string
				want   map[string][][]interface{}
			}{
				{
					"a not-join local, through a rule argument",
					`[:find ?b :in $ % ?a :where (r ?a ?b)]`,
					`[:find ?b :in $ %% :where [(ground %q) ?a] (r ?a ?b)]`,
					`[[(r ?a ?b) [?b :item/one ?a] (not-join [?b] [?a :item/next ?b])]]`,
					map[string][][]interface{}{"x": {{first}}, "y": {}},
				},
				{
					"an or-join local, through a rule argument",
					`[:find ?b :in $ % ?a :where (r ?a ?b)]`,
					`[:find ?b :in $ %% :where [(ground %q) ?a] (r ?a ?b)]`,
					`[[(r ?a ?b) [?b :item/one ?a] (or-join [?b] [?a :item/next ?b] [?b :item/count _])]]`,
					map[string][][]interface{}{"x": {{first}}, "y": {{second}}},
				},
				{
					"an or-default-join local, through a rule argument",
					`[:find ?b ?v :in $ % ?a :where (r ?a ?b ?v)]`,
					`[:find ?b ?v :in $ %% :where [(ground %q) ?a] (r ?a ?b ?v)]`,
					`[[(r ?a ?b ?v) [?b :item/one ?a] (or-default-join [[?b] ?v] (and [?a :item/next ?b] [(ground 1) ?v]) [(ground 0) ?v])]]`,
					map[string][][]interface{}{"x": {{first, int64(0)}}, "y": {{second, int64(1)}}},
				},
				{
					"a not-join local, directly",
					`[:find ?b :in $ ?a :where [?b :item/one ?a] (not-join [?b] [?a :item/next ?b])]`,
					`[:find ?b :where [(ground %q) ?a] [?b :item/one ?a] (not-join [?b] [?a :item/next ?b])]`,
					"",
					map[string][][]interface{}{"x": {{first}}, "y": {}},
				},
				{
					"an or-join local, directly",
					`[:find ?b :in $ ?a :where [?b :item/one ?a] (or-join [?b] [?a :item/next ?b] [?b :item/count _])]`,
					`[:find ?b :where [(ground %q) ?a] [?b :item/one ?a] (or-join [?b] [?a :item/next ?b] [?b :item/count _])]`,
					"",
					map[string][][]interface{}{"x": {{first}}, "y": {{second}}},
				},
			} {
				for value, want := range c.want {
					var inputs, groundInputs []interface{}
					if c.rules != "" {
						inputs = append(inputs, c.rules)
						groundInputs = append(groundInputs, c.rules)
					}
					inputs = append(inputs, value)
					answered, err := executor.CollectTuples(db.Query(c.query, inputs...))
					require.NoError(t, err, "%s, %q: the input reaches no typed position, so it is not refused", c.name, value)
					ground, err := executor.CollectTuples(db.Query(fmt.Sprintf(c.ground, value), groundInputs...))
					require.NoError(t, err, "%s, %q: the ground-bound argument", c.name, value)
					assert.ElementsMatch(t, want, answered, "%s, %q: the input answers the form's own reading of its local", c.name, value)
					assert.ElementsMatch(t, ground, answered, "%s, %q: the input answers what the ground-bound argument answers", c.name, value)
				}
			}

			for _, c := range []struct {
				name  string
				query string
				rules string
			}{
				{"a not-join header variable, through a rule argument", `[:find ?b :in $ % ?a :where (r ?a ?b)]`, `[[(r ?a ?b) [?b :item/one ?a] (not-join [?a ?b] [?a :item/next ?b])]]`},
				{"an or-join header variable, through a rule argument", `[:find ?b :in $ % ?a :where (r ?a ?b)]`, `[[(r ?a ?b) [?b :item/one ?a] (or-join [[?a ?b]] [?a :item/next ?b] [?b :item/count _])]]`},
				{"a not-join header variable, directly", `[:find ?b :in $ ?a :where [?b :item/one ?a] (not-join [?a ?b] [?a :item/next ?b])]`, ""},
			} {
				var inputs []interface{}
				if c.rules != "" {
					inputs = append(inputs, c.rules)
				}
				inputs = append(inputs, "x")
				_, err := executor.CollectTuples(db.Query(c.query, inputs...))
				require.Error(t, err, "%s: a string reaching the entity position through the header is refused", c.name)
				assert.ErrorContains(t, err, "entity position", c.name)
			}
		})
	}
}

// A constant written as a rule invocation's argument at a head position a
// pattern of the predicate's bodies occupies, or as a subquery's argument at
// an inner typed parameter, binds into that position, so a constant of
// another type is refused at the query's entry as the same constant written
// in the pattern is refused at the match's entry, in every view. A constant
// at a value-only position is not refused, and an ElementID constant at the
// transaction position answers the assertion it names.
func TestAConstantArgumentOfAnotherTypeAtATypedPositionIsRefused(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db, at := openTransactionTypingDB(t, mode, false)
			eid := fmt.Sprintf("#eid [%d %d]", at.Lamport, at.ReplicaID)

			for _, view := range []struct {
				name string
				db   *Database
			}{{"latest", db}, {"History", db.History()}, {"as-of", db.AsOf(at)}} {
				for _, c := range []struct {
					name     string
					query    string
					rules    string
					position string
				}{
					{"a string at the transaction position", `[:find ?e :in $ % :where (at ?e "x")]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"an integer at the transaction position", fmt.Sprintf(`[:find ?e :in $ %% :where (at ?e %d)]`, at.Lamport), `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the entity position", `[:find ?v :in $ % :where (one "item:first" ?v)]`, `[[(one ?e ?v) [?e :item/one ?v]]]`, "entity position"},
					{"an identity at the attribute position", `[:find ?v :in $ % :where (has #id "item:first" ?v)]`, `[[(has ?a ?v) [?e ?a ?v]]]`, "attribute position"},
					{"a string at the transaction position of a body's own invocation", `[:find ?e :in $ % :where (r ?e)]`, `[[(r ?e) (at ?e "x")] [(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position through a subquery's argument", `[:find ?e :in $ % :where [(q [:find ?x :in $ ?tx :where [?x :item/one "x" ?tx]] $ "x") [[?e]]]]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position of a predicate reached through a chain of invocations", `[:find ?e :in $ % :where (a ?e)]`, `[[(a ?e) (wr ?e)] [(wr ?e) (at ?e "x")] [(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position of an invocation under a not-join", `[:find ?e :in $ % :where [?e :item/count _] (not-join [?e] (at ?e "x"))]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position of an invocation under a subquery's own %", `[:find ?e :in $ % :where [(q [:find ?x :in $ % :where (at ?x "x")] $ %) [[?e]]]]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position of a predicate with a required position", `[:find ?e :in $ % :where (at "x" ?e)]`, `[[(at [?t] ?e) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position through a subquery's tuple parameter", `[:find ?e :in $ % :where [(q [:find ?x :in $ [?tx ?d] :where [?x :item/one ?d ?tx]] $ "x" 1) [[?e]]]]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position through a subquery's relation parameter", `[:find ?e :in $ % :where [(q [:find ?x :in $ [[?tx ?d]] :where [?x :item/one ?d ?tx]] $ "x" 1) [[?e]]]]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position through a subquery's scalar parameter declared after a tuple parameter", `[:find ?e :in $ % :where [(q [:find ?x :in $ [?a ?d] ?tx :where [?x :item/one ?a ?tx] [?x :item/count ?d]] $ "x" 5 "x") [[?e]]]]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position through a subquery's tuple parameter declared after its rules marker", `[:find ?e :in $ % :where [(q [:find ?x :in $ % [?tx ?d] :where [?x :item/one ?d ?tx]] $ % "x" 1) [[?e]]]]`, `[[(at ?e ?t) [?e :item/one "x" ?t]]]`, "transaction position"},
					{"a string at the transaction position through a subquery's tuple parameter in a rule body", `[:find ?e :in $ % :where (r ?e)]`, `[[(r ?e) [(q [:find ?x :in $ [?tx ?d] :where [?x :item/one ?d ?tx]] $ "x" 1) [[?e]]]]]`, "transaction position"},
				} {
					_, err := executor.CollectTuples(view.db.Query(c.query, c.rules))
					require.Error(t, err, "%s, %s: a constant of another type at a typed position is refused at the query's entry", view.name, c.name)
					assert.ErrorContains(t, err, c.position, "%s, %s", view.name, c.name)
				}
			}

			valued, err := executor.CollectTuples(db.Query(`[:find ?e :in $ % :where (one ?e 5)]`, `[[(one ?e ?v) [?e :item/one ?v]]]`))
			require.NoError(t, err, "a constant at a value-only position is not refused")
			assert.Empty(t, valued, "the integer matches no :item/one value")

			unreached := `[[(g ?e) [?e :item/count 5]] [(wr ?e) (at ?e "x")] [(at ?e ?t) [?e :item/one "x" ?t]]]`
			for _, c := range []struct {
				name  string
				query string
			}{
				{"a query invoking another predicate", `[:find ?e :in $ % :where (g ?e)]`},
				{"a query invoking no predicate", `[:find ?e :in $ % :where [?e :item/count 5]]`},
			} {
				answered, err := executor.CollectTuples(db.Query(c.query, unreached))
				require.NoError(t, err, "%s: a constant in a predicate the query does not invoke is not read", c.name)
				require.Equal(t, [][]interface{}{{first}}, answered, "%s: the query answers the facts", c.name)
			}

			answered, err := executor.CollectTuples(db.Query(fmt.Sprintf(`[:find ?e :in $ %% :where (at ?e %s)]`, eid), `[[(at ?e ?t) [?e :item/one "x" ?t]]]`))
			require.NoError(t, err, "an ElementID constant at the transaction position through a rule argument")
			require.Equal(t, [][]interface{}{{first}}, answered, "the #eid argument answers the assertion it names")

			answered, err = executor.CollectTuples(db.Query(fmt.Sprintf(`[:find ?e :in $ %% :where [(q [:find ?x :in $ [?tx ?d] :where [?x :item/one ?d ?tx]] $ %s "x") [[?e]]]]`, eid), `[[(at ?e ?t) [?e :item/one "x" ?t]]]`))
			require.NoError(t, err, "an ElementID constant at the transaction position through a subquery's tuple parameter")
			require.Equal(t, [][]interface{}{{first}}, answered, "the #eid argument through the tuple parameter answers the assertion it names")
		})
	}
}

// A subquery's collection parameter takes a vector. A call passing a constant
// of another shape there is refused at the query's entry, naming the
// argument and the parameter, whatever position the parameter's symbol
// occupies in the inner query, in every view. A variable argument bound to a
// value of another shape is refused when the call's binding runs.
func TestANonVectorConstantAtASubquerysCollectionParameterIsRefused(t *testing.T) {
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db, at := openTransactionTypingDB(t, mode, false)
			for _, view := range []struct {
				name string
				db   *Database
			}{{"latest", db}, {"History", db.History()}, {"as-of", db.AsOf(at)}} {
				for _, c := range []struct {
					name  string
					query string
				}{
					{"the symbol at a value position", `[:find ?e :where [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ "x") [[?e]]]]`},
					{"the symbol at the transaction position", `[:find ?e :where [(q [:find ?x :in $ [?tx ...] :where [?x :item/one "x" ?tx]] $ "x") [[?e]]]]`},
				} {
					_, err := executor.CollectTuples(view.db.Query(c.query))
					require.Error(t, err, "%s, %s: a constant of another shape than a vector at a collection parameter is refused", view.name, c.name)
					assert.ErrorContains(t, err, `argument 2 is "x", and the collection parameter`, "%s, %s: the refusal names the argument", view.name, c.name)
					assert.ErrorContains(t, err, "takes a vector", "%s, %s: the refusal names the parameter's shape", view.name, c.name)
				}

				_, err := executor.CollectTuples(view.db.Query(`[:find ?e :in $ ?w :where [(q [:find ?x :in $ [?v ...] :where [?x :item/one ?v]] $ ?w) [[?e]]]]`, "x"))
				require.Error(t, err, "%s: a variable bound to a string at a collection parameter is refused when the binding runs", view.name)
				assert.ErrorContains(t, err, "collection binding [?v ...] requires a vector result, got string", view.name)
			}
		})
	}
}
