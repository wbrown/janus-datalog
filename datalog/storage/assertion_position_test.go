package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// assertionFixture is the store the tests of a data pattern's fifth position
// read, with the point of each of its three commits. One entity, doc, carries
// a cardinality-one :w/one, a cardinality-many :w/many and a vector :w/list.
// The first commit asserts one a, many x and y, and list p then q. The second
// withdraws one a, many x and list p, and asserts one b. The third asserts
// one c, which supersedes b and withdraws nothing.
type assertionFixture struct {
	db                   *Database
	doc                  datalog.Identity
	names                map[datalog.Identity]string
	first, second, third datalog.ElementID
}

func openAssertionFixture(t *testing.T, mode optimizerMode, disableCache bool) assertionFixture {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":w/name").Type(schema.TypeString).Add().
		Attribute(":w/one").Type(schema.TypeString).Add().
		Attribute(":w/many").Type(schema.TypeString).Many().Add().
		Attribute(":w/list").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	f := assertionFixture{
		db:  createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: disableCache}),
		doc: datalog.NewIdentity("assertion:doc"),
	}
	f.names = map[datalog.Identity]string{f.doc: "doc"}
	name, one, many, list := kw(":w/name"), kw(":w/one"), kw(":w/many"), kw(":w/list")

	tx := f.db.NewTransaction()
	require.NoError(t, tx.Add(f.doc, name, "doc"))
	require.NoError(t, tx.Add(f.doc, one, "a"))
	require.NoError(t, tx.Add(f.doc, many, "x"))
	require.NoError(t, tx.Add(f.doc, many, "y"))
	require.NoError(t, tx.Add(f.doc, list, "p"))
	require.NoError(t, tx.Add(f.doc, list, "q"))
	f.first, err = tx.Commit()
	require.NoError(t, err)

	tx = f.db.NewTransaction()
	require.NoError(t, tx.Remove(f.doc, one, "a"))
	require.NoError(t, tx.Remove(f.doc, many, "x"))
	require.NoError(t, tx.Remove(f.doc, list, "p"))
	require.NoError(t, tx.Add(f.doc, one, "b"))
	f.second, err = tx.Commit()
	require.NoError(t, err)

	tx = f.db.NewTransaction()
	require.NoError(t, tx.Add(f.doc, one, "c"))
	f.third, err = tx.Commit()
	require.NoError(t, err)
	return f
}

// answers runs text on view and renders its tuples, sorted.
func (f assertionFixture) answers(t *testing.T, view *Database, text string, inputs ...interface{}) []string {
	t.Helper()
	rel, err := view.Query(text, inputs...)
	require.NoError(t, err, text)
	return renderedTuples(t, rel, f.names)
}

// A withdrawal's false tuple in History binds the value the Remove call
// named. A cardinality-one Remove withdraws whatever value the attribute
// holds, so the value it names need not be one the attribute held: after
// Add a and Remove zzz the latest view holds no value, and History's false
// tuple binds zzz.
func TestAWithdrawalBindsTheValueTheRemoveNamed(t *testing.T) {
	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":w/one").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			doc := datalog.NewIdentity("withdrawal:doc")
			names := map[datalog.Identity]string{doc: "doc"}
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(doc, kw(":w/one"), "a"))
			_, err = tx.Commit()
			require.NoError(t, err)
			tx = d.NewTransaction()
			require.NoError(t, tx.Remove(doc, kw(":w/one"), "zzz"))
			_, err = tx.Commit()
			require.NoError(t, err)

			latest, err := d.Query(`[:find ?v :where [?e :w/one ?v]]`)
			require.NoError(t, err)
			require.Empty(t, renderedTuples(t, latest, names), "the withdrawal leaves the attribute with no value in the latest view")
			withdrawn, err := d.History().Query(`[:find ?v :where [?e :w/one ?v _ false]]`)
			require.NoError(t, err)
			require.Equal(t, []string{"zzz"}, renderedTuples(t, withdrawn, names), "History's false tuple binds the value the Remove call named")
		})
	}
}

// TestHistoryBindsWhetherEachDatomAssertsOrWithdraws pins the fifth position
// of a data pattern on a History handle. It binds true for a datom that
// asserts a value and false for one that withdraws it, and a withdrawal binds
// the value the Remove call named. That holds for a cardinality-one
// attribute, a cardinality-many attribute and a vector. A value a later one
// supersedes is withdrawn by no datom, so it binds true alone. A constant at
// the position answers the datoms whose operation it names, and so does a
// value an earlier clause binds to the position's variable. The answers are
// the same with the pattern's entity free, bound by an earlier clause, and
// given as an input.
func TestHistoryBindsWhetherEachDatomAssertsOrWithdraws(t *testing.T) {
	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			f := openAssertionFixture(t, mode, false)
			history := f.db.History()
			for _, c := range []struct {
				attr                       string
				every, withdrawn, asserted []string
			}{
				{":w/one", []string{"a false", "a true", "b true", "c true"}, []string{"a"}, []string{"a", "b", "c"}},
				{":w/many", []string{"x false", "x true", "y true"}, []string{"x"}, []string{"x", "y"}},
				{":w/list", []string{"p false", "p true", "q true"}, []string{"p"}, []string{"p", "q"}},
			} {
				t.Run(c.attr, func(t *testing.T) {
					assert.Equal(t, c.every,
						f.answers(t, history, `[:find ?v ?added :where [?e `+c.attr+` ?v _ ?added]]`),
						"the entity free")
					assert.Equal(t, c.every,
						f.answers(t, history, `[:find ?v ?added :where [?e :w/name "doc"] [?e `+c.attr+` ?v _ ?added]]`),
						"the entity bound by an earlier clause")
					assert.Equal(t, c.every,
						f.answers(t, history, `[:find ?v ?added :in $ ?e :where [?e `+c.attr+` ?v _ ?added]]`, f.doc),
						"the entity an input")
					assert.Equal(t, c.withdrawn,
						f.answers(t, history, `[:find ?v :where [?e `+c.attr+` ?v _ false]]`),
						"the constant false answers the withdrawals")
					assert.Equal(t, c.asserted,
						f.answers(t, history, `[:find ?v :where [?e `+c.attr+` ?v _ true]]`),
						"the constant true answers the assertions")
					assert.Equal(t, c.withdrawn,
						f.answers(t, history, `[:find ?v :where [(ground false) ?added] [?e `+c.attr+` ?v _ ?added]]`),
						"false bound by an earlier clause answers the withdrawals")
					assert.Equal(t, c.asserted,
						f.answers(t, history, `[:find ?v :where [(ground true) ?added] [?e `+c.attr+` ?v _ ?added]]`),
						"true bound by an earlier clause answers the assertions")
				})
			}
		})
	}
}

// TestTheLatestAndAsOfViewsBindEveryTupleAsAnAssertion pins the fifth
// position in a resolved view, the latest view and a view as of a commit.
// Every tuple a pattern answers there is a resolved value, an assertion, so
// the position binds true. That holds with the pattern's entity free, bound
// by an earlier clause, and given as an input, for a cardinality-one
// attribute, a cardinality-many attribute and a vector. The constant false,
// and false bound by an earlier clause, answer nothing. The constant true,
// and true bound by an earlier clause, answer what the pattern of four
// elements answers. Each holds with the cache enabled and disabled.
func TestTheLatestAndAsOfViewsBindEveryTupleAsAnAssertion(t *testing.T) {
	for _, mode := range legsFor(optimizerModes, sharingOption) {
		for _, cache := range []struct {
			name    string
			disable bool
		}{{"cache_enabled", false}, {"cache_disabled", true}} {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				f := openAssertionFixture(t, mode, cache.disable)
				views := []struct {
					name   string
					db     *Database
					values map[string][]string
				}{
					{"latest", f.db, map[string][]string{":w/one": {"c"}, ":w/many": {"y"}, ":w/list": {"[q]"}}},
					{"as of the first commit", f.db.AsOf(f.first), map[string][]string{":w/one": {"a"}, ":w/many": {"x", "y"}, ":w/list": {"[p q]"}}},
					{"as of the second commit", f.db.AsOf(f.second), map[string][]string{":w/one": {"b"}, ":w/many": {"y"}, ":w/list": {"[q]"}}},
				}
				for _, view := range views {
					for _, attr := range []string{":w/one", ":w/many", ":w/list"} {
						t.Run(view.name+"/"+attr, func(t *testing.T) {
							values := view.values[attr]
							withTrue := make([]string, len(values))
							for i, v := range values {
								withTrue[i] = v + " true"
							}
							require.Equal(t, values,
								f.answers(t, view.db, `[:find ?v :where [?e `+attr+` ?v]]`),
								"the pattern of four elements answers the view's values")
							assert.Equal(t, withTrue,
								f.answers(t, view.db, `[:find ?v ?added :where [?e `+attr+` ?v _ ?added]]`),
								"the entity free")
							assert.Equal(t, withTrue,
								f.answers(t, view.db, `[:find ?v ?added :where [?e :w/name "doc"] [?e `+attr+` ?v _ ?added]]`),
								"the entity bound by an earlier clause")
							assert.Equal(t, withTrue,
								f.answers(t, view.db, `[:find ?v ?added :in $ ?e :where [?e `+attr+` ?v _ ?added]]`, f.doc),
								"the entity an input")
							assert.Empty(t,
								f.answers(t, view.db, `[:find ?v :where [?e `+attr+` ?v _ false]]`),
								"the constant false answers nothing")
							assert.Equal(t, values,
								f.answers(t, view.db, `[:find ?v :where [?e `+attr+` ?v _ true]]`),
								"the constant true answers the view's values")
							assert.Empty(t,
								f.answers(t, view.db, `[:find ?v :where [(ground false) ?added] [?e `+attr+` ?v _ ?added]]`),
								"false bound by an earlier clause answers nothing")
							assert.Equal(t, values,
								f.answers(t, view.db, `[:find ?v :where [(ground true) ?added] [?e `+attr+` ?v _ ?added]]`),
								"true bound by an earlier clause answers the view's values")
						})
					}
				}
			})
		}
	}
}
