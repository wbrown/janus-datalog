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

// The strategies of docs/proposals/BOUND_VALUE_SEEK_PROOF.md §6: a binding
// touching V is decided by the cardinality the constant attribute resolves
// under. With V the one bound position, validation is requested exactly when
// the attribute resolves as cardinality one. With the entity and the value
// bound and more distinct values than entities, V is the seek dimension for
// a declared many alone, and the entity is sought otherwise.

// refSchemas are the four schema settings the contracts range over: the ref
// declared cardinality one, declared many, undeclared under a schema
// declaring another attribute, and no schema at all.
func refSchemas(t *testing.T) []struct {
	name string
	s    *schema.Schema
	many bool
} {
	t.Helper()
	one, err := schema.NewBuilder().Attribute(":t/ref").Type(schema.TypeRef).Add().Build()
	require.NoError(t, err)
	many, err := schema.NewBuilder().Attribute(":t/ref").Type(schema.TypeRef).Many().Add().Build()
	require.NoError(t, err)
	other, err := schema.NewBuilder().Attribute(":t/other").Type(schema.TypeRef).Add().Build()
	require.NoError(t, err)
	return []struct {
		name string
		s    *schema.Schema
		many bool
	}{
		{"declared one", one, false},
		{"declared many", many, true},
		{"undeclared under a given schema", other, false},
		{"no schema", nil, false},
	}
}

func TestReuseStrategy_ASingleBoundValueIsValidatedWhenTheAttributeResolvesAsOne(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?a :in $ ?d :where [?a :t/ref ?d]]`)
	require.NoError(t, err)
	pattern := q.Where[0].(*query.DataPattern)
	d1, d2 := datalog.NewIdentity("d1"), datalog.NewIdentity("d2")
	for _, sc := range refSchemas(t) {
		for _, mode := range optimizerModes {
			t.Run(sc.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sc.s, DisableCache: true})
				keys := executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?d")}, []executor.Tuple{{d1}, {d2}})
				strategy, _ := d.Matcher().(*PatternMatcher).analyzeReuseStrategy(pattern, keys)
				require.Equal(t, SinglePositionReuse, strategy.Type)
				require.Equal(t, 2, strategy.Position, "the value is the seek dimension")
				require.Equal(t, !sc.many, strategy.NeedsValidation, "validation is requested exactly when the attribute resolves as cardinality one")
			})
		}
	}
}

func TestReuseStrategy_SeveralBoundPositionsSeekOnTheValueForADeclaredManyAlone(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?a :in $ ?d :where [?a :t/ref ?d]]`)
	require.NoError(t, err)
	pattern := q.Where[0].(*query.DataPattern)
	a, d1, d2 := datalog.NewIdentity("a"), datalog.NewIdentity("d1"), datalog.NewIdentity("d2")
	for _, sc := range refSchemas(t) {
		for _, mode := range optimizerModes {
			t.Run(sc.name+"/"+mode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sc.s, DisableCache: true})
				keys := executor.NewMaterializedRelation(
					[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?d")},
					[]executor.Tuple{{a, d1}, {a, d2}})
				strategy, _ := d.Matcher().(*PatternMatcher).analyzeReuseStrategy(pattern, keys)
				require.Equal(t, SinglePositionReuse, strategy.Type)
				if sc.many {
					require.Equal(t, 2, strategy.Position, "a declared many is sought on the value, the position with more distinct values")
				} else {
					require.Equal(t, 0, strategy.Position, "an attribute resolving as cardinality one is sought on the entity, whose run holds the whole group")
				}
			})
		}
	}
}

// A declared many under a binding over the entity and the value answers its
// members, sought on the value: a's two referents, and nothing for the pair
// b does not carry.
func TestDeclaredManyAttribute_TheMatcherAnswersItsMembersSeekingOnTheValue(t *testing.T) {
	many, err := schema.NewBuilder().Attribute(":t/ref").Type(schema.TypeRef).Many().Add().Build()
	require.NoError(t, err)
	q, err := parser.ParseQuery(`[:find ?a :in $ ?d :where [?a :t/ref ?d]]`)
	require.NoError(t, err)
	pattern := q.Where[0].(*query.DataPattern)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: many, DisableCache: true})
			a, b := datalog.NewIdentity("a"), datalog.NewIdentity("b")
			d1, d2 := datalog.NewIdentity("d1"), datalog.NewIdentity("d2")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(a, kw(":t/ref"), d1))
			require.NoError(t, tx.Add(a, kw(":t/ref"), d2))
			require.NoError(t, tx.Add(b, kw(":t/ref"), d1))
			_, err := tx.Commit()
			require.NoError(t, err)
			keys := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?d")},
				[]executor.Tuple{{a, d1}, {a, d2}, {b, d2}})
			rel, err := d.Matcher().Match(query.PatternQuery(pattern), executor.Relations{keys})
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{a, d1}, {a, d2}}, tuples)
		})
	}
}

// A declared vector's value names the whole collection, so a bound value
// never narrows a run: with the entity bound each entity's vector is
// resolved and compared, and with the value alone bound every entity's
// vector is resolved and the entities carrying the bound one answer.
func TestDeclaredVectorAttribute_ABoundValueAnswersTheEntitiesCarryingIt(t *testing.T) {
	vec, err := schema.NewBuilder().Attribute(":t/vec").Type(schema.TypeString).Vector().Add().Build()
	require.NoError(t, err)
	for _, mode := range optimizerModes {
		for _, disableCache := range []bool{false, true} {
			name := mode.name
			if disableCache {
				name += "/cache off"
			}
			t.Run(name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: vec, DisableCache: disableCache})
				a, b := datalog.NewIdentity("a"), datalog.NewIdentity("b")
				tx := d.NewTransaction()
				require.NoError(t, tx.Set(a, kw(":t/vec"), []string{"x", "y"}))
				require.NoError(t, tx.Set(b, kw(":t/vec"), []string{"x"}))
				_, err := tx.Commit()
				require.NoError(t, err)
				both, err := executor.CollectTuples(d.Query(`[:find ?e :in $ ?v :where [?e :t/vec ?v]]`, []string{"x", "y"}))
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{a}}, both, "the value alone bound answers the entity carrying that vector")
				withEntity, err := executor.CollectTuples(d.Query(`[:find ?e :in $ ?e ?v :where [?e :t/vec ?v]]`, b, []string{"x"}))
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{b}}, withEntity, "the entity and the value bound answer the entity when its vector is the bound one")
				none, err := executor.CollectTuples(d.Query(`[:find ?e :in $ ?e ?v :where [?e :t/vec ?v]]`, b, []string{"x", "y"}))
				require.NoError(t, err)
				require.Empty(t, none, "the entity and the value bound answer nothing when its vector is another")
			})
		}
	}
}
