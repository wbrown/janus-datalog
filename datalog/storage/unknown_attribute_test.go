package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A database opened with RefuseUnknownAttributes refuses a query or a pull
// naming an attribute its schema does not declare, whether or not a datom
// carries it. A declared attribute with no datoms, a pattern on a named
// source, and a database opened without the option are not refused.

// refusingDB opens a database on mode that refuses unknown attributes,
// carrying the fixture declarations and one entity holding a :t/string value,
// so a pattern on a declared attribute has a tuple to return.
func refusingDB(t *testing.T, mode optimizerMode) (*Database, datalog.Identity) {
	t.Helper()
	d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t), RefuseUnknownAttributes: true})
	entity := datalog.NewIdentity("holder")
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(entity, kw(":t/string"), "held"))
	_, err := tx.Commit()
	require.NoError(t, err)
	return d, entity
}

// requireUnknown asserts that err refuses attr.
func requireUnknown(t *testing.T, err error, attr datalog.Keyword) {
	t.Helper()
	var unknown *schema.UnknownAttributeError
	require.ErrorAs(t, err, &unknown, "expected a refusal of %s, got %v", attr, err)
	require.Same(t, attr, unknown.Attribute)
}

func TestQueryRefusesUnknownAttribute(t *testing.T) {
	const typo = `[:find ?v :where [?e :t/strnig ?v]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			attr := kw(":t/strnig")

			_, err := d.Query(typo)
			requireUnknown(t, err, attr)

			q, err := parser.ParseQuery(typo)
			require.NoError(t, err)
			_, err = d.NewExecutor().Execute(q)
			requireUnknown(t, err, attr)

			var values []string
			requireUnknown(t, d.QueryInto(&values, typo), attr)

			_, err = d.Analyze(typo)
			requireUnknown(t, err, attr)
		})
	}
}

// A branch of a refusing database refuses as its parent does.
func TestForkRefusesUnknownAttributesAsItsParentDoes(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			d, entity := refusingDB(t, mode)
			_, err := d.Snapshot("s")
			require.NoError(t, err)
			branch := branchOf(t, d, "s")
			attr := kw(":t/strnig")

			_, err = branch.Query(`[:find ?v :where [?e :t/strnig ?v]]`)
			requireUnknown(t, err, attr)

			_, err = branch.Pull(entity, `[:t/strnig]`)
			requireUnknown(t, err, attr)
		})
	}
}

func TestQueryRefusesUnknownAttributeInNestedClauses(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"not", `[:find ?e :where [?e :t/string ?v] (not [?e :t/strnig _])]`},
		{"not-join", `[:find ?e :where [?e :t/string ?v] (not-join [?e] [?e :t/strnig _])]`},
		{"or-join", `[:find ?e :where (or-join [?e] [?e :t/strnig _] [?e :t/string _])]`},
		{"or-default-join", `[:find ?e ?x :where [?e :t/string ?v] (or-default-join [[?e] ?x] [?e :t/strnig ?x] [(ground "none") ?x])]`},
		{"subquery", `[:find ?e ?n :where [?e :t/string ?v] [(q [:find (count ?x) :in $ ?e :where [?e :t/strnig ?x]] $ ?e) [[?n]]]]`},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d, _ := refusingDB(t, mode)
				_, err := d.Query(c.text)
				requireUnknown(t, err, kw(":t/strnig"))
			})
		}
	}
}

func TestQueryRefusesUnknownAttributeInDatabaseFunctions(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"get-else", `[:find ?v :where [?e :t/string _] [(get-else $ ?e :t/strnig "none") ?v]]`},
		{"get-some", `[:find ?v :where [?e :t/string _] [(get-some $ ?e :t/long :t/strnig) ?v]]`},
		{"missing? expression", `[:find ?m :where [?e :t/string _] [(missing? $ ?e :t/strnig) ?m]]`},
		{"missing? predicate", `[:find ?e :where [?e :t/string _] [(missing? $ ?e :t/strnig)]]`},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d, _ := refusingDB(t, mode)
				_, err := d.Query(c.text)
				requireUnknown(t, err, kw(":t/strnig"))
			})
		}
	}
}

// A keyword an input binds to a variable that reaches a pattern's attribute
// position is a value, and a refusing database refuses nothing of it: bound
// through a scalar input, a collection input, a subquery's input, whose call
// passes the query's input to the inner :in symbol standing in the attribute
// position, an identity expression aliasing the input into the position, and
// an invocation passing the input at a position the body reads as an
// attribute, the pattern evaluates over the store and answers no tuple for
// a keyword no datom carries.
func TestQueryRunsAKeywordBoundThroughAnInput(t *testing.T) {
	texts := []struct {
		name, text string
		inputs     []interface{}
	}{
		{"a scalar input", `[:find ?v :in $ ?a :where [?e ?a ?v]]`, []interface{}{kw(":t/strnig")}},
		{"a collection input", `[:find ?v :in $ [?a ...] :where [?e ?a ?v]]`, []interface{}{[]interface{}{kw(":t/strnig")}}},
		{"a subquery's input", `[:find ?v :in $ ?a :where [(q [:find ?v :in $ ?attr :where [?e ?attr ?v]] $ ?a) [[?v] ...]]]`, []interface{}{kw(":t/strnig")}},
		{"an identity alias of the input", `[:find ?v :in $ ?in :where [(identity ?in) ?a] [?e ?a ?v]]`, []interface{}{kw(":t/strnig")}},
		{"an invocation passing the input", `[:find ?e :in $ % ?a :where (has ?e ?a)]`, []interface{}{`[[(has ?e ?a) [?e ?a _]]]`, kw(":t/strnig")}},
	}
	for _, mode := range optimizerModes {
		for _, c := range texts {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t), RefuseUnknownAttributes: true})
				tuples, err := executor.CollectTuples(d.Query(c.text, c.inputs...))
				require.NoError(t, err, "a keyword bound through an input is a value")
				require.Empty(t, tuples, "no datom carries the keyword")
			})
		}
	}
}

// An input sharing the name of a variable a branch reads in the attribute
// position outside the form's header is not that variable: the branch's is
// its own, the input reaches no attribute position, and the query answers
// every entity the branches admit.
func TestQueryRunsAnInputSharingABranchLocalsName(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, tagged, untagged := taggedPair(t, mode)
			requireSameEntities(t, []datalog.Identity{tagged, untagged},
				queriedEntities(t, d, `[:find ?e :in $ ?a :where [?e :t/n _] (or-join [?e] [?e ?a _] [?e :t/tag :t/none])]`, kw(":t/strnig")),
				"the branch's ?a is its own, so the input reaches no attribute position and the branch reads every attribute")
		})
	}
}

// A keyword that reaches an attribute position through a subquery's input is
// evaluated as data: on a refusing database the entity whose :t/tag names
// :t/n is answered its :t/n, and the entity whose :t/tag names an attribute
// no definition declares yields no datom, which the fallback answers with
// zero. The refusal of an attribute the query names is decided at entry.
func TestKeywordReachingAnAttributePositionThroughASubqueryInputIsData(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":t/tag").Type(schema.TypeKeyword).One().Add().
				Attribute(":t/n").Type(schema.TypeLong).One().Add().
				Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, RefuseUnknownAttributes: true})
			declared, undeclared := datalog.NewIdentity("declared"), datalog.NewIdentity("undeclared")
			names := map[datalog.Identity]string{declared: "declared", undeclared: "undeclared"}
			tx := d.NewTransaction()
			require.NoError(t, tx.Set(declared, kw(":t/tag"), kw(":t/n")))
			require.NoError(t, tx.Set(declared, kw(":t/n"), int64(5)))
			require.NoError(t, tx.Set(undeclared, kw(":t/tag"), kw(":t/strnig")))
			require.NoError(t, tx.Set(undeclared, kw(":t/n"), int64(50)))
			_, err = tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(`[:find ?e ?v :where [?e :t/tag ?a] (or-default [(q [:find ?v :in $ ?e ?attr :where [?e ?attr ?v]] $ ?e ?a) [[?v] ...]] [(ground 0) ?v])]`)
			require.NoError(t, err, "a keyword the data binds into an attribute position is evaluated, never checked against the vocabulary")
			require.ElementsMatch(t, []string{"declared 5", "undeclared 0"}, renderedTuples(t, rel, names))
		})
	}
}

// A variable a nested form's branch binds outside the form's interface is
// the branch's own: a ground in the enclosing query binding a variable of the
// same name reaches the enclosing query's patterns and not the branch's.
func TestQueryRunsALocalAttributeVariableBesideAGround(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":t/tag").Type(schema.TypeKeyword).One().Add().
				Attribute(":t/other").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, RefuseUnknownAttributes: true})
			tagged, both := datalog.NewIdentity("tagged"), datalog.NewIdentity("both")
			tx := d.NewTransaction()
			require.NoError(t, tx.Set(tagged, kw(":t/tag"), kw(":t/kind")))
			require.NoError(t, tx.Set(both, kw(":t/tag"), kw(":t/kind")))
			require.NoError(t, tx.Set(both, kw(":t/other"), "x"))
			_, err = tx.Commit()
			require.NoError(t, err)

			tuples, err := executor.CollectTuples(d.Query(`[:find ?q :where [(ground :t/kind) ?a] [?q :t/tag ?a] (or-join [?q] [?q ?a ?v] [?q :t/other _])]`))
			require.NoError(t, err, "the branch's ?a is its own, so :t/kind reaches no attribute position")
			var got []datalog.Identity
			for _, tuple := range tuples {
				got = append(got, tuple[0].(datalog.Identity))
			}
			require.ElementsMatch(t, []datalog.Identity{tagged, both}, got, "the branch's pattern reads every attribute of each tagged entity")
		})
	}
}

// taggedPair opens a refusing database on mode declaring :t/tag keyword and
// :t/n long, holding one entity tagged :t/kind with :t/n 5 and one entity
// with :t/n 50 alone.
func taggedPair(t *testing.T, mode optimizerMode) (*Database, datalog.Identity, datalog.Identity) {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":t/tag").Type(schema.TypeKeyword).One().Add().
		Attribute(":t/n").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, RefuseUnknownAttributes: true})
	tagged, untagged := datalog.NewIdentity("tagged"), datalog.NewIdentity("untagged")
	tx := d.NewTransaction()
	require.NoError(t, tx.Set(tagged, kw(":t/tag"), kw(":t/kind")))
	require.NoError(t, tx.Set(tagged, kw(":t/n"), int64(5)))
	require.NoError(t, tx.Set(untagged, kw(":t/n"), int64(50)))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d, tagged, untagged
}

// queriedEntities runs text on d and returns the identities its tuples carry.
func queriedEntities(t *testing.T, d *Database, text string, inputs ...interface{}) []datalog.Identity {
	t.Helper()
	tuples, err := executor.CollectTuples(d.Query(text, inputs...))
	require.NoError(t, err)
	var got []datalog.Identity
	for _, tuple := range tuples {
		got = append(got, tuple[0].(datalog.Identity))
	}
	return got
}

// requireSameEntities asserts that got holds the entities want holds and no
// other, in any order, naming each identity by its rendering on failure.
func requireSameEntities(t *testing.T, want, got []datalog.Identity, msg string) {
	t.Helper()
	wanted := make(map[datalog.Identity]bool, len(want))
	for _, id := range want {
		wanted[id] = true
	}
	seen := make(map[datalog.Identity]bool, len(got))
	same := len(got) == len(wanted)
	for _, id := range got {
		if !wanted[id] || seen[id] {
			same = false
		}
		seen[id] = true
	}
	if same {
		return
	}
	var wantNames, gotNames []string
	for _, id := range want {
		wantNames = append(wantNames, id.String())
	}
	for _, id := range got {
		gotNames = append(gotNames, id.String())
	}
	t.Fatalf("%s: got %v, want %v", msg, gotNames, wantNames)
}

// A variable at a predicate's required position that the or-join's required
// run names is bound by the enclosing query inside the branch as outside it,
// so the predicate reads the attribute a ground binds it to: a declared
// keyword answers the entity carrying it, and a keyword no datom carries
// answers nothing. Under a header that does not name it, the variable is the
// branch's own and a free predicate derives whole.
func TestQueryBindsARequiredPositionInsideABranchFromTheHeader(t *testing.T) {
	const required = `[[(has [?a] ?e) [?e ?a _]]]`
	const free = `[[(has ?a ?e) [?e ?a _]]]`
	const typo = `[:find ?e :in $ % :where [(ground :t/strnig) ?a] [?e :t/n _] (or-join [?e] (has ?a ?e) [?e :t/tag :t/none])]`
	const declared = `[:find ?e :in $ % :where [(ground :t/tag) ?a] [?e :t/n _] (or-join [?e] (has ?a ?e) [?e :t/tag :t/none])]`
	const typoThroughHeader = `[:find ?e :in $ % :where [(ground :t/strnig) ?a] [?e :t/n _] (or-join [[?a] ?e] (has ?a ?e) [?e :t/tag :t/none])]`
	const declaredThroughHeader = `[:find ?e :in $ % :where [(ground :t/tag) ?a] [?e :t/n _] (or-join [[?a] ?e] (has ?a ?e) [?e :t/tag :t/none])]`
	for _, mode := range optimizerModes {
		t.Run(mode.name+"/required, typo", func(t *testing.T) {
			d, _, _ := taggedPair(t, mode)
			requireSameEntities(t, nil, queriedEntities(t, d, typoThroughHeader, mustRuleSet(t, required)),
				"the required run carries ?a from the ground, so the predicate reads an attribute no datom carries and no entity is tagged :t/none")
		})
		t.Run(mode.name+"/required, declared keyword", func(t *testing.T) {
			d, tagged, _ := taggedPair(t, mode)
			requireSameEntities(t, []datalog.Identity{tagged}, queriedEntities(t, d, declaredThroughHeader, mustRuleSet(t, required)),
				"the required run carries ?a from the ground, so the predicate reads :t/tag")
		})
		t.Run(mode.name+"/free, typo", func(t *testing.T) {
			d, tagged, untagged := taggedPair(t, mode)
			requireSameEntities(t, []datalog.Identity{tagged, untagged}, queriedEntities(t, d, typo, mustRuleSet(t, free)),
				"a free position is the branch's own, so the predicate derives whole and the ground's keyword reaches no attribute position")
		})
		t.Run(mode.name+"/free, declared keyword", func(t *testing.T) {
			d, tagged, untagged := taggedPair(t, mode)
			requireSameEntities(t, []datalog.Identity{tagged, untagged}, queriedEntities(t, d, declared, mustRuleSet(t, free)),
				"a free position is the branch's own, so the predicate derives whole")
		})
	}
}

// A keyword passed at a predicate's position reaches no attribute position
// when the body reads the head symbol at that position as a value and a
// branch reads a variable of the same name in the attribute position outside
// the form's header: the branch's variable is its own, so the keyword is a
// value, and the invocation answers the entities carrying it.
func TestQueryRunsAKeywordPassedToABranchLocalsPosition(t *testing.T) {
	const rules = `[[(r ?e ?a) [?e :t/tag ?a] (or-join [?e] [?e ?a _] [?e :t/n _])]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, tagged, _ := taggedPair(t, mode)
			requireSameEntities(t, nil, queriedEntities(t, d, `[:find ?e :in $ % :where (r ?e :t/strnig)]`, mustRuleSet(t, rules)),
				"no entity's :t/tag is :t/strnig, and the keyword reaches no attribute position")
			requireSameEntities(t, []datalog.Identity{tagged}, queriedEntities(t, d, `[:find ?e :in $ % :where (r ?e :t/kind)]`, mustRuleSet(t, rules)),
				"the tagged entity's :t/tag is :t/kind")
		})
	}
}

// An or's branches unify with the enclosing query on every variable they
// share with it, so a ground outside the form binds a branch's attribute
// variable: a declared keyword reads the one entity carrying it, and a
// keyword no datom carries answers nothing, on the union path, on the
// correlated path, and under or-default alike.
func TestQueryBindsAOneBranchVariableOfAnOrFromTheEnclosingQuery(t *testing.T) {
	cases := []struct {
		name     string
		declared string
		typo     string
		inputs   []interface{}
	}{
		{
			"union path",
			`[:find ?q :where [(ground :t/tag) ?a] [?q :t/n _] (or [?q ?a ?v] [?q :t/tag :t/none])]`,
			`[:find ?q :where [(ground :t/strnig) ?a] [?q :t/n _] (or [?q ?a ?v] [?q :t/tag :t/none])]`,
			nil,
		},
		{
			"correlated path",
			`[:find ?q :in $ ?lim :where [(ground :t/tag) ?a] [?q :t/n ?n] (or (and [?q ?a ?v] [(< ?n ?lim)]) [?q :t/tag :t/none])]`,
			`[:find ?q :in $ ?lim :where [(ground :t/strnig) ?a] [?q :t/n ?n] (or (and [?q ?a ?v] [(< ?n ?lim)]) [?q :t/tag :t/none])]`,
			[]interface{}{int64(100)},
		},
		{
			"or-default",
			`[:find ?q :where [(ground :t/tag) ?a] [?q :t/n _] (or-default [?q ?a ?v] [?q :t/tag :t/none])]`,
			`[:find ?q :where [(ground :t/strnig) ?a] [?q :t/n _] (or-default [?q ?a ?v] [?q :t/tag :t/none])]`,
			nil,
		},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name+", declared keyword", func(t *testing.T) {
				d, tagged, _ := taggedPair(t, mode)
				requireSameEntities(t, []datalog.Identity{tagged}, queriedEntities(t, d, c.declared, c.inputs...),
					"the branch's ?a is bound from the ground, so its pattern reads :t/tag")
			})
			t.Run(mode.name+"/"+c.name+", typo", func(t *testing.T) {
				d, _, _ := taggedPair(t, mode)
				requireSameEntities(t, nil, queriedEntities(t, d, c.typo, c.inputs...),
					"the branch's ?a is bound from the ground to an attribute no datom carries, and no entity is tagged :t/none")
			})
		}
	}
}

// A literal keyword that reaches a pattern's attribute position through a
// variable the clause text binds is a value: a ground or identity expression
// in the query, a constant argument of a subquery call whose inner input
// stands in the position, a constant argument of an invocation at a position
// the body reads as an attribute, directly, through a body invocation or
// through a body's subquery call whose inner input stands in the position, a
// ground bound to a variable passed at such a position, or a ground inside a
// rule body. Each runs on a refusing database, where the same literal written
// in a pattern is refused.
func TestQueryRunsAKeywordReachingThroughABinding(t *testing.T) {
	// The rule sets write no keyword constant in an attribute position, so
	// every keyword a case carries reaches the position through a variable.
	const reading = `[[(has ?e ?a) [?e ?a _]]
	                  [(has2 ?e ?a) (has ?e ?a)]
	                  [(viacall [?a] ?e) [?e :t/string _] [(q [:find ?v :in $ ?x ?y :where [?x ?y ?v]] $ ?e ?a) [[?v] ...]]]]`
	const grounding = `[[(typo ?e) [(ground :t/strnig) ?a] [?e ?a _]]]`
	cases := []struct {
		name  string
		text  string
		rules string
	}{
		{"ground", `[:find ?v :where [(ground :t/strnig) ?a] [?e ?a ?v]]`, ""},
		{"ground vector", `[:find ?e :where [(ground [:t/strnig :t/x]) [?a ?b]] [?e ?a ?b]]`, ""},
		{"identity", `[:find ?v :where [(identity :t/strnig) ?a] [?e ?a ?v]]`, ""},
		{"subquery call constant", `[:find ?v :where [?e :t/string _] [(q [:find ?v :in $ ?attr :where [?x ?attr ?v]] $ :t/strnig) [[?v] ...]]]`, ""},
		{"invocation constant", `[:find ?e :in $ % :where (has ?e :t/strnig)]`, reading},
		{"invocation constant through a body invocation", `[:find ?e :in $ % :where (has2 ?e :t/strnig)]`, reading},
		{"invocation constant through a body's subquery call", `[:find ?e :in $ % :where (viacall :t/strnig ?e)]`, reading},
		{"ground into an invocation", `[:find ?e :in $ % :where [(ground :t/strnig) ?a] (has ?e ?a)]`, reading},
		{"ground inside a rule body", `[:find ?e :in $ % :where (typo ?e)]`, grounding},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d, _ := refusingDB(t, mode)
				var inputs []interface{}
				if c.rules != "" {
					inputs = append(inputs, mustRuleSet(t, c.rules))
				}
				_, err := executor.CollectTuples(d.Query(c.text, inputs...))
				require.NoError(t, err, "a keyword reaching the attribute position through a binding is a value")
			})
		}
	}
}

// typedAccessor is one exported read of Database naming an attribute in Go,
// called on d for e and a with its value dropped and its error returned.
type typedAccessor struct {
	name string
	get  func(d *Database, e datalog.Identity, a datalog.Keyword) error
}

// typedAccessors is every exported read of Database that names an attribute
// in Go: the six value readers, the two vector readers and the entity
// resolver.
func typedAccessors() []typedAccessor {
	return []typedAccessor{
		{"GetString", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, _, err := d.GetString(e, a)
			return err
		}},
		{"GetInt", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, _, err := d.GetInt(e, a)
			return err
		}},
		{"GetFloat", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, _, err := d.GetFloat(e, a)
			return err
		}},
		{"GetBool", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, _, err := d.GetBool(e, a)
			return err
		}},
		{"GetRef", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, _, err := d.GetRef(e, a)
			return err
		}},
		{"GetStrings", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, err := d.GetStrings(e, a)
			return err
		}},
		{"GetVectorNth", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, err := d.GetVectorNth(e, a, 0)
			return err
		}},
		{"GetVectorLength", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, err := d.GetVectorLength(e, a)
			return err
		}},
		{"ResolveEntityAttributes", func(d *Database, e datalog.Identity, a datalog.Keyword) error {
			_, err := d.ResolveEntityAttributes(e, []datalog.Keyword{a})
			return err
		}},
	}
}

func TestTypedAccessorsRefuseUnknownAttribute(t *testing.T) {
	for _, mode := range optimizerModes {
		for _, accessor := range typedAccessors() {
			t.Run(mode.name+"/"+accessor.name, func(t *testing.T) {
				d, entity := refusingDB(t, mode)
				attr := kw(":t/strnig")
				requireUnknown(t, accessor.get(d, entity, attr), attr)
			})
		}
	}
}

// ResolveEntityAttributes asked several undeclared attributes reports each,
// joined in order of appearance, as a query naming several is reported.
func TestResolveEntityAttributesReportsEveryUnknownAttribute(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, entity := refusingDB(t, mode)
			_, err := d.ResolveEntityAttributes(entity, []datalog.Keyword{kw(":t/string"), kw(":t/strnig"), kw(":t/lnog")})

			var joined interface{ Unwrap() []error }
			require.ErrorAs(t, err, &joined, "two unknown attributes are reported together, got %v", err)
			unknowns := joined.Unwrap()
			require.Len(t, unknowns, 2)
			requireUnknown(t, unknowns[0], kw(":t/strnig"))
			requireUnknown(t, unknowns[1], kw(":t/lnog"))
		})
	}
}

// A declared attribute no datom carries is not refused: every accessor
// answers it on a refusing database with no error.
func TestTypedAccessorsAnswerADeclaredAttributeWithNoDatoms(t *testing.T) {
	for _, mode := range optimizerModes {
		for _, accessor := range typedAccessors() {
			t.Run(mode.name+"/"+accessor.name, func(t *testing.T) {
				d, entity := refusingDB(t, mode)
				require.NoError(t, accessor.get(d, entity, kw(":t/double")), "a declared attribute with no datoms answers")
			})
		}
	}
}

// A database opened without the option refuses nothing: every accessor
// answers an undeclared attribute with no error.
func TestTypedAccessorsWithoutRefusalOptionAreNotChecked(t *testing.T) {
	for _, mode := range optimizerModes {
		for _, accessor := range typedAccessors() {
			t.Run(mode.name+"/"+accessor.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
				require.NoError(t, accessor.get(d, datalog.NewIdentity("holder"), kw(":t/strnig")), "a database opened without the option refuses nothing")
			})
		}
	}
}

// pullTypo pulls through a tag naming an attribute no definition declares.
type pullTypo struct {
	ID     datalog.Identity `datalog:"-,id"`
	Strnig string           `datalog:"t/strnig"`
}

func TestPullRefusesUnknownAttribute(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, entity := refusingDB(t, mode)
			attr := kw(":t/strnig")

			_, err := d.Query(`[:find (pull ?e [:t/strnig]) :where [?e :t/string _]]`)
			requireUnknown(t, err, attr)

			_, err = d.Query(`[:find (pull ?e [{:t/ref [:t/strnig]}]) :where [?e :t/string _]]`)
			requireUnknown(t, err, attr)

			_, err = d.Pull(entity, `[:t/strnig]`)
			requireUnknown(t, err, attr)

			_, err = d.PullMany([]datalog.Identity{entity}, `[:t/strnig]`)
			requireUnknown(t, err, attr)

			var one pullTypo
			requireUnknown(t, d.PullInto(entity, &one), attr)

			var many []pullTypo
			requireUnknown(t, d.PullIntoMany([]datalog.Identity{entity}, &many), attr)
		})
	}
}

func TestQueryReportsEveryUnknownAttribute(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			_, err := d.Query(`[:find ?v ?w :where [?e :t/strnig ?v] [?e :t/lnog ?w]]`)

			var joined interface{ Unwrap() []error }
			require.ErrorAs(t, err, &joined, "two unknown attributes are reported together, got %v", err)
			unknowns := joined.Unwrap()
			require.Len(t, unknowns, 2)
			requireUnknown(t, unknowns[0], kw(":t/strnig"))
			requireUnknown(t, unknowns[1], kw(":t/lnog"))
		})
	}
}

// A refusing database always has a given schema to check against, and a
// refused declaration of none leaves the given schema as it was.
func TestRefusalRequiresSchema(t *testing.T) {
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			_, err := NewDatabaseWithOptions(backendOptions(t, backend, DatabaseOptions{RefuseUnknownAttributes: true}))
			require.Error(t, err, "the option names no schema to check against")

			d := openBackendDB(t, backend, DatabaseOptions{Schema: schemaFactsFixture(t), RefuseUnknownAttributes: true})
			_, err = d.Snapshot("s")
			require.NoError(t, err)
			require.Error(t, d.SetSchema(nil), "the option names no schema to check against")

			require.NoError(t, d.TruncateTo("s"), "a refused declaration leaves the given schema in place, so the rewind installs it")
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/string")), "the given schema is still the one the database runs with")
		})
	}
}

func TestQueryAcceptsDeclaredAttributeWithNoDatoms(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			tuples, err := executor.CollectTuples(d.Query(`[:find ?v :where [?e :t/double ?v]]`))
			require.NoError(t, err, "a declared attribute with no datoms runs")
			require.Empty(t, tuples)
		})
	}
}

func TestQueryRefusesUndeclaredAttributeWithDatoms(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, entity := refusingDB(t, mode)
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(entity, kw(":t/loose"), "b"))
			_, err := tx.Commit()
			require.NoError(t, err)

			_, err = d.Query(`[:find ?v :where [?e :t/loose ?v]]`)
			requireUnknown(t, err, kw(":t/loose"))
		})
	}
}

func TestQueryNamedSourcePatternIsNotChecked(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			cache := executor.NewMemoryPatternMatcher([]datalog.Datom{
				{E: datalog.NewIdentity("cache:1"), A: kw(":cache/score"), V: int64(95)},
			})

			var scores []int64
			require.NoError(t, d.QueryInto(&scores,
				`[:find ?s :in $ $cache :where [$cache ?c :cache/score ?s]]`,
				WithSources(map[datalog.Symbol]executor.PatternMatcher{datalog.NewSymbol("$cache"): cache})),
				"a pattern on a named source reads that source's attributes")
			require.Equal(t, []int64{95}, scores)
		})
	}
}

// A refusing database passed as a named source refuses as it does as $.
func TestNamedSourceDatabaseRefusesUnknownAttribute(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			refusing, _ := refusingDB(t, mode)
			plain := createOptimizerModeDB(t, mode, DatabaseOptions{})
			attr := kw(":t/strnig")

			_, err := plain.Query(`[:find ?v :in $ $other :where [$other ?e :t/strnig ?v]]`,
				WithSources(map[datalog.Symbol]executor.PatternMatcher{datalog.NewSymbol("$other"): refusing}))
			requireUnknown(t, err, attr)
		})
	}
}

func TestQueryWithoutRefusalOptionIsNotChecked(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("holder"), kw(":t/string"), "held"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tuples, err := executor.CollectTuples(d.Query(`[:find ?v :where [?e :t/strnig ?v]]`))
			require.NoError(t, err, "a database opened without the option refuses nothing")
			require.Empty(t, tuples)
		})
	}
}

// A database opened without the option collects no roster: the plan it makes
// carries none, where the plan a refusing database makes for the same text
// carries the attributes the text names.
func TestQueryWithoutRefusalOptionCollectsNoRoster(t *testing.T) {
	const text = `[:find ?n :where [?e :t/string ?n] (not [?e :t/long _]) [(get-else $ ?e :t/double 0.0) ?w]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			plain := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			plan, err := plain.Explain(text)
			require.NoError(t, err)
			require.Nil(t, plan.Rosters, "a database that refuses nothing collects no roster")

			refusing, _ := refusingDB(t, mode)
			plan, err = refusing.Explain(text)
			require.NoError(t, err)
			require.NotNil(t, plan.Rosters, "a refusing database's plan carries the roster the check reads")
			require.Equal(t, []datalog.Keyword{kw(":t/string"), kw(":t/long"), kw(":t/double")}, plan.Rosters.SourceAttributes(datalog.SymDollar))
		})
	}
}

// queryExecutionAllocs returns the allocations one execution of text makes on
// d with the inputs given, each run on a query parsed ahead of the
// measurement. AllocsPerRun runs once unmeasured first, so every measured run
// executes a plan the cache holds.
func queryExecutionAllocs(t *testing.T, d *Database, text string, inputs ...interface{}) float64 {
	t.Helper()
	const runs = 20
	queries := make([]*query.Query, 0, runs+1)
	for i := 0; i <= runs; i++ {
		q, err := parser.ParseQuery(text)
		require.NoError(t, err)
		queries = append(queries, q)
	}
	next := 0
	return testing.AllocsPerRun(runs, func() {
		q := queries[next]
		next++
		rel, err := d.Query(q, inputs...)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executor.CollectTuples(rel, nil); err != nil {
			t.Fatal(err)
		}
	})
}

// A refusing database collects the rosters a query names once, with the
// query's plan, so an execution on a plan the cache holds reads them and
// allocates what the same execution allocates on a database without the
// option: for a query naming attributes on the default source, one naming
// them on a named source, one naming them in a subquery body, and one holding
// a headerless form, whose preparation rewrites it on every execution.
func TestQueryWithRefusalOptionCollectsRostersOnce(t *testing.T) {
	for _, algebra := range []struct {
		name string
		on   bool
	}{{"algebra_on", true}, {"algebra_off", false}} {
		t.Run(algebra.name, func(t *testing.T) {
			popts := DefaultPlannerOptions()
			popts.EnableAlgebraOptimizer = algebra.on
			open := func(refuse bool) *Database {
				d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", PlannerOptions: &popts, Schema: schemaFactsFixture(t), RefuseUnknownAttributes: refuse})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, d.Close()) })
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(datalog.NewIdentity("holder"), kw(":t/string"), "held"))
				_, err = tx.Commit()
				require.NoError(t, err)
				return d
			}
			refusing, refusingOther := open(true), open(true)
			plain, plainOther := open(false), open(false)
			other := datalog.NewSymbol("$other")
			for _, shape := range []struct {
				name string
				text string
			}{
				{"default source", `[:find ?v :where [?e :t/string ?v]]`},
				{"named source", `[:find ?v :in $ $other :where [$other ?e :t/string ?v]]`},
				{"subquery body", `[:find ?e ?n :where [?e :t/string ?v] [(q [:find (count ?x) :in $ ?e :where [?e :t/long ?x]] $ ?e) [[?n]]]]`},
				{"headerless form", `[:find ?v :where [?e :t/string ?v] (not [?e :t/long _])]`},
			} {
				t.Run(shape.name, func(t *testing.T) {
					collected := queryExecutionAllocs(t, refusing, shape.text,
						WithSources(map[datalog.Symbol]executor.PatternMatcher{other: refusingOther}))
					baseline := queryExecutionAllocs(t, plain, shape.text,
						WithSources(map[datalog.Symbol]executor.PatternMatcher{other: plainOther}))
					require.Equal(t, baseline, collected, "on a plan the cache holds, an execution on a refusing database reads its rosters and collects nothing")
				})
			}
		})
	}
}

func TestAsOfHandleRefusesBySchema(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, entity := refusingDB(t, mode)
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(entity, kw(":t/double"), 1.5))
			point, err := tx.Commit()
			require.NoError(t, err)

			tx = d.NewTransaction()
			require.NoError(t, tx.Add(entity, kw(":t/long"), int64(1)))
			require.NoError(t, tx.Add(entity, kw(":t/loose"), "later"))
			_, err = tx.Commit()
			require.NoError(t, err)

			handle := d.AsOf(point)
			tuples, err := executor.CollectTuples(handle.Query(`[:find ?v :where [?e :t/long ?v]]`))
			require.NoError(t, err, "a declared attribute runs on every handle")
			require.Empty(t, tuples, "the handle's point precedes the attribute's first datom")

			_, err = handle.Query(`[:find ?v :where [?e :t/loose ?v]]`)
			requireUnknown(t, err, kw(":t/loose"))
		})
	}
}

func TestEngineQueriesRunUnderRefusal(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)

			_, err := d.Snapshot("s")
			require.NoError(t, err, "Snapshot looks its name up before any marker exists")
			snaps, err := d.Snapshots()
			require.NoError(t, err)
			require.Len(t, snaps, 1)

			asOf, err := d.AsOfSnapshot("s")
			require.NoError(t, err)
			var values []string
			require.NoError(t, asOf.QueryInto(&values, `[:find ?v :where [?e :t/string ?v]]`))
			require.Equal(t, []string{"held"}, values)

			require.NoError(t, d.TruncateTo("s"), "TruncateTo looks for branches before any fork exists")
			require.NoError(t, d.DeleteSnapshot("s"))
			snaps, err = d.Snapshots()
			require.NoError(t, err, "Snapshots runs after the marker datoms are gone")
			require.Empty(t, snaps)
		})
	}

	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name+"/fork", func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			_, err := d.Snapshot("s")
			require.NoError(t, err)
			branch := branchOf(t, d, "s")
			var values []string
			require.NoError(t, branch.QueryInto(&values, `[:find ?v :where [?e :t/string ?v]]`))
			require.Equal(t, []string{"held"}, values)
		})
	}
}
