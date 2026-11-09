package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A data pattern's answer carries the pattern's entity variable among its
// identity symbols, whichever arm of the matcher answers it: every datom's
// entity is an identity, and a binding at the entity position is one
// (typedPositionBindingCheck). The binding-driven strategies are driven
// directly, as TestBindingDrivenStrategiesReportTheirFunnel drives them,
// since which one a query reaches depends on the binding set's size.
func TestAPatternsAnswerCarriesItsEntityVariableAmongItsIdentities(t *testing.T) {
	for _, mode := range legsWhere(backendModes, func(m optimizerMode) bool { return !m.algebra }) {
		t.Run(mode.name, func(t *testing.T) {
			testAPatternsAnswerCarriesItsEntityVariableAmongItsIdentities(t, mode)
		})
	}
}

func testAPatternsAnswerCarriesItsEntityVariableAmongItsIdentities(t *testing.T, mode optimizerMode) {
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})

	name := datalog.NewKeyword(":person/name")
	tags := datalog.NewKeyword(":person/tags")
	skills := datalog.NewKeyword(":person/skills")
	entities := make([]datalog.Identity, 3)
	tx := db.NewTransaction()
	for i := range entities {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("person:%d", i))
		require.NoError(t, tx.Set(entities[i], name, fmt.Sprintf("person %d", i)))
		require.NoError(t, tx.Add(entities[i], tags, "red"))
		require.NoError(t, tx.Add(entities[i], tags, "blue"))
		require.NoError(t, tx.Set(entities[i], skills, []interface{}{"go", "sql"}))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	e := datalog.NewSymbol("?e")
	entity := query.Variable{Name: e}
	variable := func(name string) query.Variable { return query.Variable{Name: datalog.NewSymbol(name)} }
	pattern := func(elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elements}
	}
	nameOf := pattern(entity, query.Constant{Value: name}, variable("?n"))
	skillsOf := pattern(entity, query.Constant{Value: skills}, variable("?s"))
	carryingAnother := func() executor.Relation {
		tuples := make([]executor.Tuple, len(entities))
		for i, who := range entities {
			tuples[i] = executor.Tuple{who, int64(i)}
		}
		return executor.NewMaterializedRelation([]query.Symbol{e, datalog.NewSymbol("?i")}, tuples)
	}
	m := db.Matcher().(*PatternMatcher)
	match := func(p *query.DataPattern, bindings ...executor.Relation) func() (executor.Relation, error) {
		return func() (executor.Relation, error) {
			return m.Match(query.PatternQuery(p), executor.Relations(bindings))
		}
	}

	for _, tc := range []struct {
		name   string
		answer func() (executor.Relation, error)
	}{
		{"a scan of a cardinality-one attribute", match(nameOf)},
		{"a scan for a constant value of a cardinality-one attribute",
			match(pattern(entity, query.Constant{Value: name}, query.Constant{Value: "person 1"}))},
		{"a scan of a cardinality-many attribute", match(pattern(entity, query.Constant{Value: tags}, variable("?t")))},
		{"a scan of a cardinality-many attribute dropping its value",
			match(pattern(entity, query.Constant{Value: tags}, query.Blank{}))},
		{"a scan for a value of a cardinality-many attribute",
			match(pattern(entity, query.Constant{Value: tags}, query.Constant{Value: "red"}))},
		{"a scan of a cardinality-vector attribute", match(skillsOf)},
		{"a scan for a vector under an open attribute",
			match(pattern(entity, variable("?a"), query.Constant{Value: []interface{}{"go", "sql"}}))},
		{"the cache over entity bindings", match(nameOf, entityBindings(entities))},
		{"the cache over bindings carrying another symbol", match(nameOf, carryingAnother())},
		{"a cardinality-vector attribute over entity bindings", match(skillsOf, entityBindings(entities))},
		{"the hash join", func() (executor.Relation, error) {
			return m.matchWithHashJoin(nameOf, entityBindings(entities), nameOf.Symbols(), 0, AETV, nil)
		}},
		{"the merge join", func() (executor.Relation, error) {
			return m.matchWithMergeJoin(nameOf, entityBindings(entities), nameOf.Symbols(), 0, AETV, nil)
		}},
		{"a scan per binding", func() (executor.Relation, error) {
			return m.matchWithoutIteratorReuse(nameOf, entityBindings(entities), nameOf.Symbols(), nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rel, err := tc.answer()
			require.NoError(t, err)
			require.Contains(t, rel.Properties().Identities, e)
			require.Positive(t, drainRelation(t, rel), "the arm answers the fixture's facts")
		})
	}
}
