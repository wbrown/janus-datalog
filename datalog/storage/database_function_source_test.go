package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A database function reads the source its first argument names, as
// Datomic's do (NESTED_SCOPES_AS_QUERIES.md goal 12), and get-some binds the
// attribute it found together with the value.

// twoSourceFixture opens a database for $ and one for $src. Both hold the
// entities named "one" and "two". $ holds :t/a for both, and $src holds :t/a
// for "one" alone, with a value that differs from $'s.
func twoSourceFixture(t *testing.T, mode optimizerMode, opts DatabaseOptions) (*Database, *Database) {
	t.Helper()
	d := createOptimizerModeDB(t, mode, opts)
	src := createOptimizerModeDB(t, mode, DatabaseOptions{})
	one, two := datalog.NewIdentity("fn-source:one"), datalog.NewIdentity("fn-source:two")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
	require.NoError(t, tx.Add(one, kw(":t/a"), "from-default"))
	require.NoError(t, tx.Add(two, kw(":t/name"), "two"))
	require.NoError(t, tx.Add(two, kw(":t/a"), "two-default"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx = src.NewTransaction()
	require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
	require.NoError(t, tx.Add(one, kw(":t/a"), "from-src"))
	require.NoError(t, tx.Add(two, kw(":t/name"), "two"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d, src
}

// sourceNamed passes m as the named source sym.
func sourceNamed(sym string, m executor.PatternMatcher) QueryOption {
	return WithSources(map[datalog.Symbol]executor.PatternMatcher{datalog.NewSymbol(sym): m})
}

// valuesByName runs text and returns each tuple's second value under its
// first, the entity's name.
func valuesByName(t *testing.T, d *Database, text string, inputs ...interface{}) map[string]interface{} {
	t.Helper()
	tuples, err := executor.CollectTuples(d.Query(text, inputs...))
	require.NoError(t, err, "query: %s", text)
	byName := make(map[string]interface{}, len(tuples))
	for _, tuple := range tuples {
		require.Len(t, tuple, 2, "query: %s", text)
		byName[tuple[0].(string)] = tuple[1]
	}
	return byName
}

func TestDatabaseFunction_ReadsTheSourceItNames(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, src := twoSourceFixture(t, mode, DatabaseOptions{})
			srcOpt := sourceNamed("$src", src)

			require.Equal(t,
				map[string]interface{}{"one": "from-src", "two": "none"},
				valuesByName(t, d, `[:find ?n ?v :in $ $src :where [$src ?e :t/name ?n] [(get-else $src ?e :t/a "none") ?v]]`, srcOpt),
				"get-else on $src reads $src")
			require.Equal(t,
				map[string]interface{}{"one": "from-default", "two": "two-default"},
				valuesByName(t, d, `[:find ?n ?v :in $ $src :where [$src ?e :t/name ?n] [(get-else $ ?e :t/a "none") ?v]]`, srcOpt),
				"get-else on $ reads $")

			require.Equal(t,
				map[string]interface{}{"one": false, "two": true},
				valuesByName(t, d, `[:find ?n ?m :in $ $src :where [$src ?e :t/name ?n] [(missing? $src ?e :t/a) ?m]]`, srcOpt),
				"missing? bound on $src reads $src")
			require.Equal(t,
				map[string]interface{}{"one": false, "two": false},
				valuesByName(t, d, `[:find ?n ?m :in $ $src :where [$src ?e :t/name ?n] [(missing? $ ?e :t/a) ?m]]`, srcOpt),
				"missing? bound on $ reads $")

			missing, err := executor.CollectTuples(d.Query(`[:find ?n :in $ $src :where [$src ?e :t/name ?n] [(missing? $src ?e :t/a)]]`, srcOpt))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"two"}}, missing, "missing? as a predicate on $src reads $src")

			require.Equal(t,
				map[string]interface{}{"one": "from-src", "two": "two"},
				valuesByName(t, d, `[:find ?n ?v :in $ $src :where [$src ?e :t/name ?n] [(get-some $src ?e :t/a :t/name) [?attr ?v]]]`, srcOpt),
				"get-some on $src reads $src")
		})
	}
}

// Two texts that differ only in a function's source are two queries, so the
// plan the first makes is never served for the second.
func TestDatabaseFunction_TextsDifferingInTheSourceAreTwoQueries(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, src := twoSourceFixture(t, mode, DatabaseOptions{})
			srcOpt := sourceNamed("$src", src)
			const onDefault = `[:find ?n ?v :in $ $src :where [$src ?e :t/name ?n] [(get-else $ ?e :t/a "none") ?v]]`
			const onSrc = `[:find ?n ?v :in $ $src :where [$src ?e :t/name ?n] [(get-else $src ?e :t/a "none") ?v]]`
			for range 2 {
				require.Equal(t, "from-default", valuesByName(t, d, onDefault, srcOpt)["one"])
				require.Equal(t, "from-src", valuesByName(t, d, onSrc, srcOpt)["one"])
			}
		})
	}
}

// get-else's default takes the type the schema of the source it reads gives
// the attribute, with and without an annotation handler on the database the
// query runs on.
func TestDatabaseFunction_GetElseDefaultIsTypedByItsSource(t *testing.T) {
	srcSchema, err := schema.NewBuilder().
		Attribute(":t/name").Type(schema.TypeString).Add().
		Attribute(":t/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	handlers := []struct {
		name    string
		handler annotations.Handler
	}{
		{"no handler", nil},
		{"annotation handler", func(annotations.Event) {}},
	}
	for _, mode := range optimizerModes {
		for _, h := range handlers {
			t.Run(mode.name+"/"+h.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: h.handler})
				src := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: srcSchema})
				one, two := datalog.NewIdentity("fn-typed:one"), datalog.NewIdentity("fn-typed:two")
				tx := src.NewTransaction()
				require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
				require.NoError(t, tx.Add(one, kw(":t/skills"), "stealth"))
				require.NoError(t, tx.Add(two, kw(":t/name"), "two"))
				_, err := tx.Commit()
				require.NoError(t, err)

				got := valuesByName(t, d,
					`[:find ?n ?s :in $ $src :where [$src ?e :t/name ?n] [(get-else $src ?e :t/skills []) ?s]]`,
					sourceNamed("$src", src))
				require.Equal(t, []string{"stealth"}, got["one"])
				require.Equal(t, []string{}, got["two"], "the default is a []string, the type $src's schema gives :t/skills")
			})
		}
	}
}

// A subquery's inner query reads $ as the source its call passes there, for a
// database function as for a data pattern.
func TestDatabaseFunction_InASubqueryReadsTheSourceTheCallPasses(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, src := twoSourceFixture(t, mode, DatabaseOptions{})
			require.Equal(t,
				map[string]interface{}{"one": "from-src", "two": "none"},
				valuesByName(t, d,
					`[:find ?n ?v
					  :in $ $src
					  :where [$src ?e :t/name ?n]
					         [(q [:find ?x :in $ ?e :where [(get-else $ ?e :t/a "none") ?x]] $src ?e) [[?v]]]]`,
					sourceNamed("$src", src)),
				"the inner get-else reads $src, the source the call passes as the inner $")
		})
	}
}

// A database function on a source that cannot look up an entity's attribute
// is an error, never an answer read from another source.
func TestDatabaseFunction_OnASourceThatCannotLookUpIsAnError(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := twoSourceFixture(t, mode, DatabaseOptions{})
			cache := executor.NewSliceSource([]string{"one"}, executor.AttributeSchema[string]{
				kw(":t/name"): func(name string) any { return name },
			})
			_, err := executor.CollectTuples(d.Query(
				`[:find ?n ?m
				  :in $ $cache
				  :where [$cache ?e :t/name ?n]
				         [(q [:find ?x :in $ ?e :where [(missing? $ ?e :t/a) ?x]] $cache ?e) [[?m]]]]`,
				sourceNamed("$cache", cache)))
			require.Error(t, err, "the inner missing? reads $cache, which cannot look up an attribute")
			require.Contains(t, err.Error(), "$cache")
		})
	}
}

// The vocabulary check reads a database function's attributes on the source
// the function names.
func TestDatabaseFunction_AttributesAreCheckedOnTheSourceItNames(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			refusing, _ := refusingDB(t, mode)
			plain := createOptimizerModeDB(t, mode, DatabaseOptions{})
			attr := kw(":t/strnig")

			_, err := plain.Query(`[:find ?v :in $ $other :where [$other ?e :t/string _] [(get-else $other ?e :t/strnig "x") ?v]]`,
				sourceNamed("$other", refusing))
			requireUnknown(t, err, attr)

			_, err = executor.CollectTuples(refusing.Query(`[:find ?v :in $ $other :where [?e :t/string _] [(get-else $other ?e :t/strnig "x") ?v]]`,
				sourceNamed("$other", plain)))
			require.NoError(t, err, "a function on $other is checked by $other, which refuses nothing")
		})
	}
}

// get-some binds the first attribute the entity carries together with its
// value: under the tuple form, the attribute and the value; under a single
// symbol, the pair.
func TestGetSome_BindsTheAttributeItFoundWithTheValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			both := datalog.NewIdentity("get-some:both")
			second := datalog.NewIdentity("get-some:second")
			neither := datalog.NewIdentity("get-some:neither")
			empty := datalog.NewIdentity("get-some:empty")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(both, kw(":t/name"), "both"))
			require.NoError(t, tx.Add(both, kw(":t/a"), "a-value"))
			require.NoError(t, tx.Add(both, kw(":t/b"), "b-value"))
			require.NoError(t, tx.Add(second, kw(":t/name"), "second"))
			require.NoError(t, tx.Add(second, kw(":t/b"), "b-only"))
			require.NoError(t, tx.Add(neither, kw(":t/name"), "neither"))
			require.NoError(t, tx.Add(neither, kw(":t/c"), "c-only"))
			require.NoError(t, tx.Add(empty, kw(":t/name"), "empty"))
			require.NoError(t, tx.Add(empty, kw(":t/a"), ""))
			_, err := tx.Commit()
			require.NoError(t, err)

			tuples, err := executor.CollectTuples(d.Query(
				`[:find ?n ?attr ?v :where [?e :t/name ?n] [(get-some $ ?e :t/a :t/b) [?attr ?v]]]`))
			require.NoError(t, err)
			found := map[string][2]interface{}{}
			for _, tuple := range tuples {
				found[tuple[0].(string)] = [2]interface{}{tuple[1], tuple[2]}
			}
			require.Equal(t, map[string][2]interface{}{
				"both":   {kw(":t/a"), "a-value"},
				"second": {kw(":t/b"), "b-only"},
				"empty":  {kw(":t/a"), ""},
			}, found, "an entity carrying neither attribute is dropped")

			require.Equal(t,
				map[string]interface{}{
					"both":   []interface{}{kw(":t/a"), "a-value"},
					"second": []interface{}{kw(":t/b"), "b-only"},
					"empty":  []interface{}{kw(":t/a"), ""},
				},
				valuesByName(t, d, `[:find ?n ?p :where [?e :t/name ?n] [(get-some $ ?e :t/a :t/b) ?p]]`),
				"a single symbol binds the pair")
		})
	}
}
