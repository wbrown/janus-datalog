package db_test

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// reviewMemoryGeneratedFacts is a seeded list of scalar facts over five
// entities and three attributes, with the log a store holding them carries:
// each fact at the ElementID of its place, under the op its attribute's
// cardinality writes, OpCRDTAdd where one entity holds two values of the
// attribute and OpNone otherwise.
func reviewMemoryGeneratedFacts(seed int64) (facts []datalog.Datom, log []datalog.Datom, entities []datalog.Identity) {
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < 5; i++ {
		entities = append(entities, datalog.NewIdentity(fmt.Sprintf("generated:%d:%d", seed, i)))
	}
	attributes := []datalog.Keyword{
		datalog.NewKeyword(":generated/a"),
		datalog.NewKeyword(":generated/b"),
		datalog.NewKeyword(":generated/c"),
	}
	values := []interface{}{"x", "y", int64(1), int64(2)}
	n := 3 + r.Intn(10)
	for i := 0; i < n; i++ {
		facts = append(facts, datalog.Datom{
			E: entities[r.Intn(len(entities))],
			A: attributes[r.Intn(len(attributes))],
			V: values[r.Intn(len(values))],
		})
	}
	type entityAttribute struct {
		e datalog.Identity
		a datalog.Keyword
	}
	held := make(map[entityAttribute][]interface{})
	many := make(map[datalog.Keyword]bool)
	for _, f := range facts {
		key := entityAttribute{e: f.E, a: f.A}
		for _, v := range held[key] {
			if !datalog.ValuesEqual(v, f.V) {
				many[f.A] = true
			}
		}
		held[key] = append(held[key], f.V)
	}
	for i, f := range facts {
		op := datalog.OpNone
		if many[f.A] {
			op = datalog.OpCRDTAdd
		}
		log = append(log, datalog.Datom{E: f.E, A: f.A, V: f.V, Tx: datalog.ElementID{Lamport: uint64(i + 1), ReplicaID: 1}, Op: op})
	}
	return facts, log, entities
}

// A database storage.OpenFacts opens answers as the database holding the same
// facts' log answers, the store inferring each undefined attribute's
// cardinality from its ops: a pattern, a pattern whose attribute is a
// variable, and a wildcard pull of each entity, a cardinality-many
// attribute's members compared as a set.
func TestReviewMemory_OpenFactsAnswersAsTheDatabaseOfItsFactsLog(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		{"a pattern on an attribute", `[:find ?e ?v :where [?e :generated/a ?v]]`},
		{"a pattern on another attribute", `[:find ?e ?v :where [?e :generated/b ?v]]`},
		{"a pattern whose attribute is a variable", `[:find ?e ?a ?v :where [?e ?a ?v] (not [?e :db/txInstant _])]`},
	}
	for seed := int64(1); seed <= 40; seed++ {
		facts, log, entities := reviewMemoryGeneratedFacts(seed)
		reference := databaseOfLog(t, log)
		opened, err := storage.OpenFacts(facts, storage.DatabaseOptions{})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, opened.Close()) })
		for _, q := range queries {
			t.Run(fmt.Sprintf("%s/seed %d", q.name, seed), func(t *testing.T) {
				want, err := executor.CollectTuples(reference.Query(q.query))
				require.NoError(t, err)
				got, err := executor.CollectTuples(opened.Query(q.query))
				require.NoError(t, err)
				require.ElementsMatch(t, want, got)
			})
		}
		t.Run(fmt.Sprintf("a wildcard pull of each entity/seed %d", seed), func(t *testing.T) {
			for _, e := range entities {
				want, err := reference.Pull(e, `[*]`)
				require.NoError(t, err)
				got, err := opened.Pull(e, `[*]`)
				require.NoError(t, err)
				// The facts are scalars, so a slice a pull answers holds a
				// cardinality-many attribute's members, a set, which a pull
				// answers in no order. Each set is compared in the engine's
				// order.
				for _, pulled := range []map[string]interface{}{want, got} {
					for _, value := range pulled {
						if members, isSet := value.([]interface{}); isSet {
							slices.SortFunc(members, datalog.CompareValues)
						}
					}
				}
				require.Equal(t, want, got, "the wildcard pull of %v", e)
			}
		})
	}
}
