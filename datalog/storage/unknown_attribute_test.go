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

func TestQueryRefusesUnknownAttributeBoundThroughInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, _ := refusingDB(t, mode)
			attr := kw(":t/strnig")

			_, err := d.Query(`[:find ?v :in $ ?a :where [?e ?a ?v]]`, attr)
			requireUnknown(t, err, attr)

			_, err = d.Query(`[:find ?v :in $ [?a ...] :where [?e ?a ?v]]`, []interface{}{attr})
			requireUnknown(t, err, attr)
		})
	}
}

func TestTypedAccessorsRefuseUnknownAttribute(t *testing.T) {
	accessors := []struct {
		name string
		get  func(d *Database, e datalog.Identity, a datalog.Keyword) error
	}{
		{"GetString", func(d *Database, e datalog.Identity, a datalog.Keyword) error { _, _, err := d.GetString(e, a); return err }},
		{"GetInt", func(d *Database, e datalog.Identity, a datalog.Keyword) error { _, _, err := d.GetInt(e, a); return err }},
		{"GetFloat", func(d *Database, e datalog.Identity, a datalog.Keyword) error { _, _, err := d.GetFloat(e, a); return err }},
		{"GetBool", func(d *Database, e datalog.Identity, a datalog.Keyword) error { _, _, err := d.GetBool(e, a); return err }},
		{"GetRef", func(d *Database, e datalog.Identity, a datalog.Keyword) error { _, _, err := d.GetRef(e, a); return err }},
		{"GetStrings", func(d *Database, e datalog.Identity, a datalog.Keyword) error { _, err := d.GetStrings(e, a); return err }},
	}
	for _, mode := range optimizerModes {
		for _, accessor := range accessors {
			t.Run(mode.name+"/"+accessor.name, func(t *testing.T) {
				d, entity := refusingDB(t, mode)
				attr := kw(":t/strnig")
				requireUnknown(t, accessor.get(d, entity, attr), attr)
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

// A database opened without the option collects no roster, so a fresh parsed
// query allocates what one whose roster was collected beforehand allocates.
func TestQueryWithoutRefusalOptionCollectsNoRoster(t *testing.T) {
	const text = `[:find ?n :where [?e :p/name ?n] [?e :p/age ?a] (not [?e :p/x _]) [(get-else $ ?e :p/y "z") ?w]]`
	for _, algebra := range []struct {
		name string
		on   bool
	}{{"algebra_on", true}, {"algebra_off", false}} {
		t.Run(algebra.name, func(t *testing.T) {
			popts := DefaultPlannerOptions()
			popts.EnableAlgebraOptimizer = algebra.on
			d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", PlannerOptions: &popts})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			person := datalog.NewIdentity("p:1")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(person, kw(":p/name"), "alice"))
			require.NoError(t, tx.Add(person, kw(":p/age"), int64(30)))
			_, err = tx.Commit()
			require.NoError(t, err)

			fresh := queryExecutionAllocs(t, d, text, false)
			collected := queryExecutionAllocs(t, d, text, true)
			require.Equal(t, collected, fresh, "with the option off, executing a query collects no roster")
		})
	}
}

// queryExecutionAllocs returns the allocations one execution of text makes on
// d, each run on a query parsed ahead of the measurement, its roster collected
// ahead of it too when collected is set.
func queryExecutionAllocs(t *testing.T, d *Database, text string, collected bool) float64 {
	t.Helper()
	const runs = 20
	queries := make([]*query.Query, 0, runs+1)
	for i := 0; i <= runs; i++ {
		q, err := parser.ParseQuery(text)
		require.NoError(t, err)
		if collected {
			q.DefaultSourceAttributes()
		}
		queries = append(queries, q)
	}
	next := 0
	return testing.AllocsPerRun(runs, func() {
		q := queries[next]
		next++
		rel, err := d.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executor.CollectTuples(rel, nil); err != nil {
			t.Fatal(err)
		}
	})
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
