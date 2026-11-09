package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// In the latest and as-of views, [?e ?a V] answers an entity and attribute
// exactly where [?e a V], with that attribute constant, answers the entity:
// the value as a constant or a binding, over a cardinality-one, a
// cardinality-many, a vector and an undeclared attribute, with the EA cache on
// and off.
func TestAValueWithAFreeAttributeAnswersWhatTheAttributeBoundPatternAnswers(t *testing.T) {
	one := datalog.NewKeyword(":item/one")
	many := datalog.NewKeyword(":item/many")
	vec := datalog.NewKeyword(":item/vec")
	loose := datalog.NewKeyword(":item/loose")
	attrs := []datalog.Keyword{one, many, vec, loose}

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: one, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: many, ValueType: schema.TypeString, Cardinality: schema.CardinalityMany})
	s.Add(&schema.AttributeDefinition{Ident: vec, ValueType: schema.TypeString, Cardinality: schema.CardinalityVector})

	oneSuperseded, oneHeld := datalog.NewIdentity("item:one-superseded"), datalog.NewIdentity("item:one-held")
	manyHeld, manyWithdrawn := datalog.NewIdentity("item:many-held"), datalog.NewIdentity("item:many-withdrawn")
	vecLong, vecShort := datalog.NewIdentity("item:vec-long"), datalog.NewIdentity("item:vec-short")
	looseSuperseded, looseHeld := datalog.NewIdentity("item:loose-superseded"), datalog.NewIdentity("item:loose-held")

	forms := []struct {
		name  string
		query string
		args  []interface{}
	}{
		{"constant", `[:find ?e ?a :where [?e ?a "x"]]`, nil},
		{"binding", `[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`, []interface{}{"x"}},
	}

	for _, mode := range optimizerModes {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(oneSuperseded, one, "x"))
				require.NoError(t, tx.Add(oneHeld, one, "x"))
				require.NoError(t, tx.Add(manyHeld, many, "x"))
				require.NoError(t, tx.Add(manyHeld, many, "z"))
				require.NoError(t, tx.Add(manyWithdrawn, many, "x"))
				require.NoError(t, tx.Add(vecLong, vec, "x"))
				require.NoError(t, tx.Add(vecLong, vec, "w"))
				require.NoError(t, tx.Add(vecShort, vec, "x"))
				require.NoError(t, tx.Add(looseSuperseded, loose, "x"))
				require.NoError(t, tx.Add(looseHeld, loose, "x"))
				first, err := tx.Commit()
				require.NoError(t, err)
				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(oneSuperseded, one, "y"))
				require.NoError(t, tx2.Remove(manyWithdrawn, many, "x"))
				require.NoError(t, tx2.Add(looseSuperseded, loose, "q"))
				_, err = tx2.Commit()
				require.NoError(t, err)

				views := []struct {
					name string
					db   *Database
					want map[datalog.Keyword][]datalog.Identity
				}{
					{"latest", db, map[datalog.Keyword][]datalog.Identity{
						one:   {oneHeld},
						many:  {manyHeld},
						loose: {looseHeld},
					}},
					{"as of the first commit", db.AsOf(first), map[datalog.Keyword][]datalog.Identity{
						one:   {oneSuperseded, oneHeld},
						many:  {manyHeld, manyWithdrawn},
						loose: {looseSuperseded, looseHeld},
					}},
				}
				for _, view := range views {
					for _, form := range forms {
						got := entitiesByAttribute(t, view.db, attrs, form.query, form.args...)
						for _, attr := range attrs {
							bound := entitySet(t, view.db, fmt.Sprintf(`[:find ?e :where [?e %s "x"]]`, attr))
							require.Equal(t, bound, got[attr],
								"%s, %s: [?e ?a \"x\"] answers for %s what [?e %s \"x\"] answers", view.name, form.name, attr, attr)
						}
						for attr, entities := range view.want {
							want := make(map[datalog.Identity]bool, len(entities))
							for _, e := range entities {
								want[e] = true
							}
							require.Equal(t, want, got[attr], "%s, %s: the entities whose %s holds \"x\"", view.name, form.name, attr)
						}
					}
				}
			})
		}
	}
}

// entitiesByAttribute runs a query finding ?e ?a and answers, for each of
// attrs, the set of entities it found with that attribute.
func entitiesByAttribute(t *testing.T, db *Database, attrs []datalog.Keyword, queryStr string, args ...interface{}) map[datalog.Keyword]map[datalog.Identity]bool {
	t.Helper()
	found := make(map[datalog.Keyword]map[datalog.Identity]bool, len(attrs))
	for _, attr := range attrs {
		found[attr] = map[datalog.Identity]bool{}
	}
	tuples, err := executor.CollectTuples(db.Query(queryStr, args...))
	require.NoError(t, err)
	for _, tuple := range tuples {
		e, ok := tuple[0].(datalog.Identity)
		require.True(t, ok, "expected an Identity at ?e, got %T", tuple[0])
		a, ok := tuple[1].(datalog.Keyword)
		require.True(t, ok, "expected a Keyword at ?a, got %T", tuple[1])
		if entities, held := found[a]; held {
			entities[e] = true
		}
	}
	return found
}

// entitySet runs a query finding ?e and answers the set of entities it found.
func entitySet(t *testing.T, db *Database, queryStr string) map[datalog.Identity]bool {
	t.Helper()
	tuples, err := executor.CollectTuples(db.Query(queryStr))
	require.NoError(t, err)
	found := make(map[datalog.Identity]bool, len(tuples))
	for _, tuple := range tuples {
		e, ok := tuple[0].(datalog.Identity)
		require.True(t, ok, "expected an Identity at ?e, got %T", tuple[0])
		found[e] = true
	}
	return found
}
