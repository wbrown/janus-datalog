package db_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func tempDB(t *testing.T, backend storage.Backend, opts ...db.Option) *db.DB {
	t.Helper()
	store, err := backend.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	d, err := db.Open(store, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return d
}

func TestOpenClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "db-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(filepath.Join(tmpDir, "test.db"))
	require.NoError(t, err)
	require.NotNil(t, d)

	err = d.Close()
	require.NoError(t, err)
}

func TestOpenMemory(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, err := db.OpenMemory(db.WithReplicaID(42), db.WithPlannerOptions(mode.plannerOptions()))
			require.NoError(t, err)
			defer d.Close()
			_, ok := d.Store().(*storage.MemoryTreeStore)
			require.True(t, ok)

			entity := datalog.NewIdentity("memory-public")
			attr := datalog.NewKeyword(":memory/name")
			tx := d.NewTransaction()
			require.NoError(t, tx.Set(entity, attr, "portable"))
			_, err = tx.Commit()
			require.NoError(t, err)

			var names []string
			require.NoError(t, d.QueryInto(
				&names,
				`[:find ?name :where [?entity :memory/name ?name]]`,
			))
			require.Equal(t, []string{"portable"}, names)
		})
	}
}

// A store the caller opened is one of Open's sources, and the database it backs
// keeps using that very store rather than a copy or a reopen.
func TestOpenTakesAStore(t *testing.T) {
	store := storage.NewMemoryStore(&storage.BinaryKeyEncoder{})
	database, err := db.Open(store, db.WithReplicaID(43))
	require.NoError(t, err)
	defer database.Close()
	require.Same(t, store, database.Store())
}

func TestQueryRoundTrip(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			txID, err := tx.Commit()
			require.NoError(t, err)
			assert.NotEqual(t, datalog.ElementID{}, txID)

			rel, err := d.Query(`[:find ?name :where [?e :person/name ?name]]`)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()
			require.True(t, iter.Next())
			assert.Equal(t, "Alice", iter.Tuple()[0])
			assert.False(t, iter.Next())
		})
	}
}

func TestQueryWithInputs(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")
			age := datalog.NewKeyword(":person/age")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			require.NoError(t, tx.Add(alice, age, int64(30)))
			_, err := tx.Commit()
			require.NoError(t, err)

			rel, err := d.Query(
				`[:find ?name :in $ ?min-age :where [?e :person/name ?name] [?e :person/age ?age] [(>= ?age ?min-age)]]`,
				int64(25),
			)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()
			require.True(t, iter.Next())
			assert.Equal(t, "Alice", iter.Tuple()[0])
			assert.False(t, iter.Next())
		})
	}
}

func TestQueryInto(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			var names []string
			err = d.QueryInto(&names, `[:find ?name :where [?e :person/name ?name]]`)
			require.NoError(t, err)
			require.Len(t, names, 1)
			assert.Equal(t, "Alice", names[0])
		})
	}
}

func TestQueryOneInto(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			var result string
			found, err := d.QueryOneInto(&result, `[:find ?name :where [?e :person/name ?name]]`)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, "Alice", result)

			// Not found case
			var missing string
			found, err = d.QueryOneInto(&missing, `[:find ?name :where [?e :person/nonexistent ?name]]`)
			require.NoError(t, err)
			assert.False(t, found)
		})
	}
}

func TestGetString(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			val, found, err := d.GetString(alice, name)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, "Alice", val)

			// Missing attribute
			val, found, err = d.GetString(alice, datalog.NewKeyword(":person/missing"))
			require.NoError(t, err)
			assert.False(t, found)
			assert.Equal(t, "", val)
		})
	}
}

func TestGetInt(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			age := datalog.NewKeyword(":person/age")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, age, int64(30)))
			_, err := tx.Commit()
			require.NoError(t, err)

			val, found, err := d.GetInt(alice, age)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, int64(30), val)

			// Missing
			val, found, err = d.GetInt(alice, datalog.NewKeyword(":person/missing"))
			require.NoError(t, err)
			assert.False(t, found)
			assert.Equal(t, int64(0), val)
		})
	}
}

func TestGetFloat(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			score := datalog.NewKeyword(":person/score")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, score, 3.14))
			_, err := tx.Commit()
			require.NoError(t, err)

			val, found, err := d.GetFloat(alice, score)
			require.NoError(t, err)
			assert.True(t, found)
			assert.InDelta(t, 3.14, val, 0.001)

			// Missing
			val, found, err = d.GetFloat(alice, datalog.NewKeyword(":person/missing"))
			require.NoError(t, err)
			assert.False(t, found)
			assert.Equal(t, float64(0), val)
		})
	}
}

func TestGetBool(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			active := datalog.NewKeyword(":person/active")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, active, true))
			_, err := tx.Commit()
			require.NoError(t, err)

			val, found, err := d.GetBool(alice, active)
			require.NoError(t, err)
			assert.True(t, found)
			assert.True(t, val)

			// Missing
			val, found, err = d.GetBool(alice, datalog.NewKeyword(":person/missing"))
			require.NoError(t, err)
			assert.False(t, found)
			assert.False(t, val)
		})
	}
}

func TestGetRef(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			friend := datalog.NewKeyword(":person/friend")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, friend, bob))
			_, err := tx.Commit()
			require.NoError(t, err)

			val, found, err := d.GetRef(alice, friend)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, bob, val)

			// Missing
			val, found, err = d.GetRef(alice, datalog.NewKeyword(":person/missing"))
			require.NoError(t, err)
			assert.False(t, found)
			assert.Nil(t, val)
		})
	}
}

func TestGetStrings(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":person/tag").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			d := tempDB(t, mode.backend, db.WithSchema(s), db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			tag := datalog.NewKeyword(":person/tag")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, tag, "developer"))
			require.NoError(t, tx.Add(alice, tag, "gopher"))
			_, err = tx.Commit()
			require.NoError(t, err)

			vals, err := d.GetStrings(alice, tag)
			require.NoError(t, err)
			assert.Len(t, vals, 2)
			assert.Contains(t, vals, "developer")
			assert.Contains(t, vals, "gopher")

			// Missing — empty slice, no error
			vals, err = d.GetStrings(alice, datalog.NewKeyword(":person/missing"))
			require.NoError(t, err)
			assert.Empty(t, vals)
		})
	}
}

func TestAsOf(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx1 := d.NewTransaction()
			require.NoError(t, tx1.Add(alice, name, "Alice"))
			tx1ID, err := tx1.Commit()
			require.NoError(t, err)

			tx2 := d.NewTransaction()
			require.NoError(t, tx2.Add(alice, name, "Alicia"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Latest should be "Alicia"
			val, found, err := d.GetString(alice, name)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, "Alicia", val)

			// AsOf tx1 should be "Alice"
			asOf := d.AsOf(tx1ID)
			val, found, err = asOf.GetString(alice, name)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, "Alice", val)
		})
	}
}

func TestHistory(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx1 := d.NewTransaction()
			require.NoError(t, tx1.Add(alice, name, "Alice"))
			_, err := tx1.Commit()
			require.NoError(t, err)

			tx2 := d.NewTransaction()
			require.NoError(t, tx2.Add(alice, name, "Alicia"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// History should return both values
			hist := d.History()
			rel, err := hist.Query(`[:find ?name :where [?e :person/name ?name]]`)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()

			var names []string
			for iter.Next() {
				names = append(names, iter.Tuple()[0].(string))
			}
			assert.Len(t, names, 2)
			assert.Contains(t, names, "Alice")
			assert.Contains(t, names, "Alicia")
		})
	}
}

// queryTuples runs q on h with inputs and returns every tuple it answers,
// each copied out of the iterator's reused workspace.
func queryTuples(t *testing.T, h *db.DB, q string, inputs ...interface{}) [][]interface{} {
	t.Helper()
	rel, err := h.Query(q, inputs...)
	require.NoError(t, err)
	iter := rel.Iterator()
	defer iter.Close()
	var tuples [][]interface{}
	for iter.Next() {
		tuples = append(tuples, append([]interface{}(nil), iter.Tuple()...))
	}
	require.NoError(t, iter.Error())
	return tuples
}

// boundValueForm is one spelling of a pattern whose value is bound: written
// into the pattern as a constant, or bound from an input.
type boundValueForm struct {
	name   string
	query  string
	inputs []interface{}
}

// History reads every datom written, including two that differ only in the
// replica that wrote them: the same value written on two replicas at the same
// Lamport is two datoms, and a History query over the attribute reads both,
// with the value written in the pattern and with it bound from an input.
func TestHistoryReadsDatomsDifferingOnlyByReplica(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			e := datalog.NewIdentity("replicated:subject")
			attr := datalog.NewKeyword(":replicated/name")

			writer, err := db.OpenMemory(db.WithReplicaID(1))
			require.NoError(t, err)
			t.Cleanup(func() { _ = writer.Close() })
			tx := writer.NewTransaction()
			require.NoError(t, tx.Set(e, attr, "same"))
			_, err = tx.Commit()
			require.NoError(t, err)
			var dump bytes.Buffer
			require.NoError(t, writer.Export(&dump))

			d := tempDB(t, mode.backend, db.WithReplicaID(2), db.WithPlannerOptions(mode.plannerOptions()))
			tx = d.NewTransaction()
			require.NoError(t, tx.Set(e, attr, "same"))
			_, err = tx.Commit()
			require.NoError(t, err)
			require.NoError(t, d.Import(&dump))

			forms := []boundValueForm{
				{"value in the pattern", `[:find ?tx :where [?e :replicated/name "same" ?tx]]`, nil},
				{"value from an input", `[:find ?tx :in $ ?name :where [?e :replicated/name ?name ?tx]]`, []interface{}{"same"}},
			}
			for _, form := range forms {
				t.Run(form.name, func(t *testing.T) {
					var txs []datalog.ElementID
					for _, tuple := range queryTuples(t, d.History(), form.query, form.inputs...) {
						id, ok := datalog.DerefElementID(tuple[0])
						require.True(t, ok, "?tx binds an ElementID, got %T", tuple[0])
						txs = append(txs, id)
					}
					require.Len(t, txs, 2, "History reads the datom each replica wrote")
					require.Equal(t, txs[0].Lamport, txs[1].Lamport, "the two writes share a Lamport")
					require.ElementsMatch(t, []uint64{1, 2}, []uint64{txs[0].ReplicaID, txs[1].ReplicaID})
				})
			}
		})
	}
}

// History reads a superseded value when the pattern binds it: after "Alice"
// then "Alicia" on one entity, a History query for the entities whose name is
// "Alice" reads the entity, as the same query with the value unbound does,
// with the value written in the pattern and with it bound from an input.
func TestHistoryReadsASupersededValueBoundInThePattern(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))
			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")
			for _, value := range []string{"Alice", "Alicia"} {
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(alice, name, value))
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			var unbound []string
			require.NoError(t, d.History().QueryInto(&unbound, `[:find ?name :where [?e :person/name ?name]]`))
			require.ElementsMatch(t, []string{"Alice", "Alicia"}, unbound)

			forms := []boundValueForm{
				{"value in the pattern", `[:find ?e :where [?e :person/name "Alice"]]`, nil},
				{"value from an input", `[:find ?e :in $ ?name :where [?e :person/name ?name]]`, []interface{}{"Alice"}},
			}
			for _, form := range forms {
				t.Run(form.name, func(t *testing.T) {
					bound := queryTuples(t, d.History(), form.query, form.inputs...)
					require.Equal(t, [][]interface{}{{alice}}, bound, "the superseded value bound in the pattern reads the entity under History")
				})
			}
		})
	}
}

var shelfBooks = datalog.NewKeyword(":shelf/books")

// vectorSchema declares :shelf/books as a vector of strings.
func vectorSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":shelf/books").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	return s
}

// History reads a vector attribute as the element datoms its writer wrote,
// whatever binds the entity: the pattern with the entity written as a
// constant, with it unbound and with it bound from an input read the same
// datoms. A vector literal at the value position names a resolved
// collection, which the current view answers, and under History it reads
// with the entity unbound or from an input what it reads with the entity
// written as a constant.
func TestHistoryReadsAVectorAttributeAsItsElementDatoms(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithSchema(vectorSchema(t)), db.WithPlannerOptions(mode.plannerOptions()))
			shelf := datalog.NewIdentity("shelf:folio")
			tx := d.NewTransaction()
			require.NoError(t, tx.Set(shelf, shelfBooks, []string{"folio", "quarto", "octavo"}))
			_, err := tx.Commit()
			require.NoError(t, err)

			require.Len(t, queryTuples(t, d, `[:find ?tx :where [?e :shelf/books ["folio" "quarto" "octavo"] ?tx]]`), 1,
				"the current view resolves the vector the literal names")

			elements := queryTuples(t, d.History(), `[:find ?v ?tx :where [#id "shelf:folio" :shelf/books ?v ?tx]]`)
			require.Len(t, elements, 3, "History reads one datom per element written")
			require.ElementsMatch(t, elements,
				queryTuples(t, d.History(), `[:find ?v ?tx :where [?e :shelf/books ?v ?tx]]`),
				"the entity unbound reads the element datoms")
			require.ElementsMatch(t, elements,
				queryTuples(t, d.History(), `[:find ?v ?tx :in $ ?e :where [?e :shelf/books ?v ?tx]]`, shelf),
				"the entity bound from an input reads the element datoms")

			literal := queryTuples(t, d.History(), `[:find ?tx :where [#id "shelf:folio" :shelf/books ["folio" "quarto" "octavo"] ?tx]]`)
			require.ElementsMatch(t, literal,
				queryTuples(t, d.History(), `[:find ?tx :where [?e :shelf/books ["folio" "quarto" "octavo"] ?tx]]`),
				"the entity unbound reads for a vector literal what the constant entity reads")
			require.ElementsMatch(t, literal,
				queryTuples(t, d.History(), `[:find ?tx :in $ ?e :where [?e :shelf/books ["folio" "quarto" "octavo"] ?tx]]`, shelf),
				"the entity bound from an input reads for a vector literal what the constant entity reads")
		})
	}
}

// A vector literal at the value position names a resolved collection, which a
// scalar attribute never carries, so it equals no datom of one: with the
// entity written as a constant, with it unbound and with it bound from an
// input, the pattern reads nothing, with the EA cache and without it.
func TestAVectorLiteralEqualsNoDatomOfAScalarAttribute(t *testing.T) {
	for _, mode := range optimizerModes {
		for _, cache := range []struct {
			name string
			opts []db.Option
		}{
			{"cache", nil},
			{"no cache", []db.Option{db.WithoutCache()}},
		} {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				d := tempDB(t, mode.backend, append([]db.Option{db.WithPlannerOptions(mode.plannerOptions())}, cache.opts...)...)
				alice := datalog.NewIdentity("alice")
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice"))
				_, err := tx.Commit()
				require.NoError(t, err)

				require.Len(t, queryTuples(t, d, `[:find ?tx :where [?e :person/name "Alice" ?tx]]`), 1,
					"the scalar value reads the datom")
				require.Empty(t, queryTuples(t, d, `[:find ?tx :where [#id "alice" :person/name ["Alice"] ?tx]]`),
					"the entity written as a constant")
				require.Empty(t, queryTuples(t, d, `[:find ?tx :where [?e :person/name ["Alice"] ?tx]]`),
					"the entity unbound")
				require.Empty(t, queryTuples(t, d, `[:find ?tx :in $ ?e :where [?e :person/name ["Alice"] ?tx]]`, alice),
					"the entity bound from an input")
			})
		}
	}
}

func TestMustParseQuery(t *testing.T) {
	// Valid query — should not panic
	q := db.MustParseQuery(`[:find ?e :where [?e :person/name "Alice"]]`)
	assert.NotNil(t, q)

	// Invalid query — should panic
	assert.Panics(t, func() {
		db.MustParseQuery(`[:find`)
	})
}

func TestMustParseQueryWithQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Pre-parsed query works with Query
			q := db.MustParseQuery(`[:find ?name :where [?e :person/name ?name]]`)
			rel, err := d.Query(q)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()
			require.True(t, iter.Next())
			assert.Equal(t, "Alice", iter.Tuple()[0])
		})
	}
}

func TestAssert(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			err := d.Assert([]datalog.Datom{{E: alice, A: name, V: "Alice"}})
			require.NoError(t, err)

			val, found, err := d.GetString(alice, name)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, "Alice", val)
		})
	}
}

func TestPullInto(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")
			age := datalog.NewKeyword(":person/age")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			require.NoError(t, tx.Add(alice, age, int64(30)))
			_, err := tx.Commit()
			require.NoError(t, err)

			type Person struct {
				Name string `datalog:"name"`
				Age  int64  `datalog:"age"`
			}
			var p Person
			err = d.PullInto(alice, &p)
			require.NoError(t, err)
			assert.Equal(t, "Alice", p.Name)
			assert.Equal(t, int64(30), p.Age)
		})
	}
}

func TestTransactionRollback(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			alice := datalog.NewIdentity("alice")
			name := datalog.NewKeyword(":person/name")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(alice, name, "Alice"))
			require.NoError(t, tx.Rollback())

			// Should not find the data
			rel, err := d.Query(`[:find ?name :where [?e :person/name ?name]]`)
			require.NoError(t, err)
			iter := rel.Iterator()
			defer iter.Close()
			assert.False(t, iter.Next())
		})
	}
}

func TestSaveStruct(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := tempDB(t, mode.backend, db.WithPlannerOptions(mode.plannerOptions()))

			type Person struct {
				ID   datalog.Identity `datalog:"-,id"`
				Name string           `datalog:"name"`
			}

			p := &Person{Name: "Alice"}
			tx := d.NewTransaction()
			id, err := tx.SaveStruct(p)
			require.NoError(t, err)
			assert.NotNil(t, id)
			_, err = tx.Commit()
			require.NoError(t, err)

			val, found, err := d.GetString(id, datalog.NewKeyword(":person/name"))
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, "Alice", val)
		})
	}
}

// Verify DB satisfies Querier interface at compile time.
var _ db.Querier = (*db.DB)(nil)
