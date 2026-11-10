package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/db"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestAttributeFetchBundleTraversesInputOnce(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	kind := datalog.NewSymbol("?kind")
	code := datalog.NewSymbol("?code")
	name := datalog.NewSymbol("?name")
	codeAttr := datalog.NewKeyword(":place/code")
	nameAttr := datalog.NewKeyword(":place/name")
	e1 := datalog.NewIdentity("bundle:e1")
	e2 := datalog.NewIdentity("bundle:e2")

	matcher := factsDatabase(t, []datalog.Datom{
		{E: e1, A: codeAttr, V: "R1"},
		{E: e1, A: nameAttr, V: "N1"},
		{E: e2, A: codeAttr, V: "R2"},
		{E: e2, A: nameAttr, V: "N2"},
	}).Matcher()
	clauses := []query.Clause{
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: codeAttr},
			query.Variable{Name: code},
		}},
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: nameAttr},
			query.Variable{Name: name},
		}},
	}
	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return m.Fusion() }) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewQueryExecutor(matcher, nil, ExecutorOptionsFromPlanner(mode.PlannerOptions()))
			inputProperties := RelationProperties{Keys: [][]query.Symbol{{entity}}}
			input := NewIteratorCountingRelation(NewMaterializedRelationWithProperties(
				[]query.Symbol{entity, kind},
				[]Tuple{{e1, "room"}, {e2, "room"}},
				ExecutorOptions{},
				inputProperties,
			))

			groups, consumed, err := exec.TryFuseAttributeFetchBundle(
				NewContext(),
				clauses,
				[]Relation{input},
			)
			require.NoError(t, err)
			require.Equal(t, 2, consumed)
			require.Equal(t, 1, input.IteratorCalls(), "bundle must traverse the input relation once")
			require.Len(t, groups, 1)
			require.Equal(t, RelationProperties{Keys: inputProperties.Keys, Identities: []query.Symbol{entity}}, groups[0].Properties(),
				"the bundle carries the input's keys, and its entity variable holds identities, the only entities it answers")

			got, err := CollectTuples(groups[0], nil)
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{
				{e1, "room", "R1", "N1"},
				{e2, "room", "R2", "N2"},
			}, got)
		})
	}
}

// After the pattern binding an entity, a run of patterns fetching that
// entity's attributes fuses on the legs with attribute-fetch fusion: one
// fetch is reported per attribute, over every entity. On the other legs
// nothing is fused. Every leg answers the same tuples.
func TestASameEntityAttributeRunFusesAndAnswersAsUnfused(t *testing.T) {
	kind, code, name := datalog.NewKeyword(":place/kind"), datalog.NewKeyword(":place/code"), datalog.NewKeyword(":place/name")
	e1, e2 := datalog.NewIdentity("bundle:e1"), datalog.NewIdentity("bundle:e2")
	s, err := schema.NewBuilder().
		Attribute(":place/kind").Type(schema.TypeString).One().Add().
		Attribute(":place/code").Type(schema.TypeString).One().Add().
		Attribute(":place/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	const text = `[:find ?e ?code ?name :where [?e :place/kind "room"] [?e :place/code ?code] [?e :place/name ?name]]`
	want := [][]interface{}{{e1, "R1", "N1"}, {e2, "R2", "N2"}}
	type fetch struct {
		attr    datalog.Keyword
		in, out int
	}
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var fetches []fetch
			d, err := db.OpenMemory(db.WithSchema(s), db.WithPlannerOptions(mode.PlannerOptions()), db.WithAnnotationHandler(mode.Recording(func(e annotations.Event) {
				if e.Name == annotations.PatternFusedFetch {
					fetches = append(fetches, fetch{attr: e.Data["attr"].(datalog.Keyword), in: e.Data["in"].(int), out: e.Data["out"].(int)})
				}
			})))
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			tx := d.NewTransaction()
			for _, f := range []struct {
				e datalog.Identity
				a datalog.Keyword
				v string
			}{
				{e1, kind, "room"}, {e1, code, "R1"}, {e1, name, "N1"},
				{e2, kind, "room"}, {e2, code, "R2"}, {e2, name, "N2"},
			} {
				require.NoError(t, tx.Add(f.e, f.a, f.v))
			}
			_, err = tx.Commit()
			require.NoError(t, err)

			got, err := CollectTuples(d.Query(text))
			require.NoError(t, err)
			require.ElementsMatch(t, want, got)
			if mode.Fusion() {
				require.ElementsMatch(t, []fetch{{code, 2, 2}, {name, 2, 2}}, fetches, "each attribute after the binding pattern is fetched over both entities")
			} else {
				require.Empty(t, fetches, "nothing is fused with the option off")
			}
		})
	}
}
