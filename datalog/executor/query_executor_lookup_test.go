package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// valueDatabase opens a database holding :t/name "one" and :t/a value for
// the entity lookup:one.
func valueDatabase(t *testing.T, value string) *storage.Database {
	t.Helper()
	d, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{Path: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	one := datalog.NewIdentity("lookup:one")
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(one, datalog.NewKeyword(":t/name"), "one"))
	require.NoError(t, tx.Add(one, datalog.NewKeyword(":t/a"), value))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d
}

// executeOne runs text on exec and returns the one value of its one tuple.
func executeOne(t *testing.T, exec *executor.Executor, text string) interface{} {
	t.Helper()
	q, err := parser.ParseQuery(text)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(exec.Execute(q))
	require.NoError(t, err, "query: %s", text)
	require.Len(t, tuples, 1, "query: %s", text)
	return tuples[0][0]
}

// A database function evaluates through the executor's matcher, reading the
// source the function names.
func TestExecutor_DatabaseFunctionReadsTheSourceItNames(t *testing.T) {
	router := executor.NewSourceRouter(map[query.Symbol]executor.PatternMatcher{
		datalog.SymDollar:         valueDatabase(t, "from-default"),
		datalog.NewSymbol("$src"): valueDatabase(t, "from-src"),
	})
	exec := executor.NewExecutor(router, nil)
	require.Equal(t, "from-src", executeOne(t, exec,
		`[:find ?v :in $ $src :where [$src ?e :t/name "one"] [(get-else $src ?e :t/a "none") ?v]]`))
	require.Equal(t, "from-default", executeOne(t, exec,
		`[:find ?v :in $ $src :where [$src ?e :t/name "one"] [(get-else $ ?e :t/a "none") ?v]]`))
}

// A database function over a slice source answers from the slice's facts:
// the slice's entity holds no :t/a, so missing? holds of it.
func TestExecutor_DatabaseFunctionOverASliceSourceAnswersItsFacts(t *testing.T) {
	exec := executor.NewExecutor(sliceDatabase(t, []string{"one"}, storage.AttributeSchema[string]{
		datalog.NewKeyword(":t/name"): func(name string) any { return name },
	}), nil)
	require.Equal(t, true, executeOne(t, exec, `[:find ?m :where [?e :t/name "one"] [(missing? $ ?e :t/a) ?m]]`))
}

// customSource is a source a caller writes, as docs/reference/MULTI_SOURCE.md
// describes under "Implementing Custom Sources": a PatternMatcher and nothing
// else, matching each pattern against the database it reads. It looks no
// attribute up.
type customSource struct{ d *storage.Database }

func (s customSource) Match(q *query.Query, bindings executor.Relations) (executor.Relation, error) {
	return s.d.Match(q, bindings)
}

// An executor whose matcher cannot look an entity's attribute up refuses each
// database function, naming it, and never answers it as absent. The source's
// database holds :t/a for the entity, so an answer of absence would differ
// from the refusal.
func TestExecutor_DatabaseFunctionOverAMatcherThatCannotLookUpIsAnError(t *testing.T) {
	exec := executor.NewExecutor(customSource{valueDatabase(t, "held")}, nil)
	for _, c := range []struct{ function, text string }{
		{"missing?", `[:find ?e :where [?e :t/name "one"] [(missing? $ ?e :t/a)]]`},
		{"get-else", `[:find ?v :where [?e :t/name "one"] [(get-else $ ?e :t/a "none") ?v]]`},
		{"get-some", `[:find ?v :where [?e :t/name "one"] [(get-some $ ?e :t/a) ?v]]`},
	} {
		t.Run(c.function, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(exec.Execute(q))
			require.ErrorContains(t, err, c.function, "the query answered %v with no error", tuples)
		})
	}
}
