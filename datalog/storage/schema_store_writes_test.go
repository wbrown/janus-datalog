package storage

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// What reaches the store, on the memory and badger backends: a user's write
// of an engine word by the entity, map and struct writers and by
// Database.Assert is refused as it is by the four datom writers; SetSchema
// calls racing beside imports, queries and pulls leave the database running
// what its store states; a History read leaves the unique rule in force; and
// an import line naming an attribute longer than storage holds is refused
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §8, docs/reference/SCHEMA.md
// "Schema as Facts").

// withDoc maps a field onto the engine's :db/doc word.
type withDoc struct {
	ID  datalog.Identity `datalog:"-,id"`
	Doc string           `datalog:"db/doc"`
}

func backendDB(t *testing.T, backend Backend, opts DatabaseOptions) *Database {
	t.Helper()
	opts.BackendName = backend.Name
	if backend.Persistent {
		opts.Path = filepath.Join(t.TempDir(), "db")
	}
	d, err := NewDatabaseWithOptions(opts)
	require.NoError(t, err)
	return d
}

func TestEngineWord_TheEntityMapStructAndAssertWritersRefuseIt(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	e := datalog.NewIdentity("u")
	doc, declaration := kw(":db/doc"), kw(":db.attribute/declaration")
	writes := []struct {
		name  string
		word  datalog.Keyword
		write func(tx *Transaction) error
	}{
		{"AddEntity", doc, func(tx *Transaction) error {
			return tx.AddEntity(e, map[datalog.Keyword]interface{}{doc: "x"})
		}},
		{"AddMap of db/doc", doc, func(tx *Transaction) error {
			_, err := tx.AddMap(map[string]interface{}{"db/doc": "x"})
			return err
		}},
		{"AddMap of :db.attribute/declaration", declaration, func(tx *Transaction) error {
			_, err := tx.AddMap(map[string]interface{}{":db.attribute/declaration": e})
			return err
		}},
		{"SaveStruct", doc, func(tx *Transaction) error {
			_, err := tx.SaveStruct(&withDoc{ID: e, Doc: "x"})
			return err
		}},
	}
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			d := backendDB(t, backend, DatabaseOptions{Schema: s})
			defer d.Close()
			for _, w := range writes {
				t.Run(w.name, func(t *testing.T) {
					tx := d.NewTransaction()
					require.ErrorContains(t, w.write(tx), w.word.String(), "the engine alone writes its words")
					require.NoError(t, tx.Rollback())
				})
			}
			t.Run("Database.Assert", func(t *testing.T) {
				require.ErrorContains(t, d.Assert([]datalog.Datom{{E: e, A: doc, V: "x"}}), doc.String(), "the engine alone writes its words")
			})
		})
	}
}

func TestSchemaFacts_SetSchemaImportQueryAndPullRunConcurrently(t *testing.T) {
	x := kw(":t/x")
	build := func(many, unique bool) *schema.Schema {
		b := schema.NewBuilder().Attribute(":t/x").Type(schema.TypeString)
		if many {
			b = b.Many()
		}
		if unique {
			b = b.Unique(schema.UniqueValue)
		}
		s, err := b.Add().Attribute(":t/y").Type(schema.TypeLong).Add().Build()
		require.NoError(t, err)
		return s
	}
	schemas := []*schema.Schema{build(false, false), build(true, false), build(false, true)}
	const rounds = 200
	type declaration struct {
		Card datalog.Keyword `datalog:"?card"`
		U    datalog.Keyword `datalog:"?u"`
	}
	requireStoredAgrees := func(t *testing.T, d *Database, running *schema.AttributeDefinition) {
		t.Helper()
		var stored []declaration
		require.NoError(t, d.QueryInto(&stored, `[:find ?card ?u :where [?a :db.attribute/declaration ?d] [?d :db/ident :t/x] [?d :db/cardinality ?card] [(get-else $ ?d :db/unique :none) ?u]]`))
		require.Len(t, stored, 1, "one declaration of :t/x is current")
		require.Same(t, running.Cardinality, stored[0].Card, "the database runs with the cardinality its store states")
		if running.Unique == nil {
			require.Same(t, kw(":none"), stored[0].U, "the database runs with the uniqueness its store states")
		} else {
			require.Same(t, running.Unique, stored[0].U, "the database runs with the uniqueness its store states")
		}
	}
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			opts := DatabaseOptions{BackendName: backend.Name, Schema: schemas[0]}
			if backend.Persistent {
				opts.Path = filepath.Join(t.TempDir(), "db")
			}
			d, err := NewDatabaseWithOptions(opts)
			require.NoError(t, err)
			path := opts.Path
			e := datalog.NewIdentity("e")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e, x, "a"))
			_, err = tx.Commit()
			require.NoError(t, err)

			var wg sync.WaitGroup
			errs := make(chan error, 6*rounds)
			for g := 0; g < 3; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < rounds; i++ {
						if err := d.SetSchema(schemas[(g+i)%3]); err != nil {
							errs <- fmt.Errorf("SetSchema: %w", err)
						}
					}
				}(g)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < rounds; i++ {
					if err := d.Import(strings.NewReader("")); err != nil {
						errs <- fmt.Errorf("Import: %w", err)
					}
				}
			}()
			for g := 0; g < 2; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < rounds; i++ {
						if _, err := executor.CollectTuples(d.Query(`[:find ?v :where [?e :t/x ?v]]`)); err != nil {
							errs <- fmt.Errorf("Query: %w", err)
						}
						if _, err := d.Pull(e, `[:t/x :t/y]`); err != nil {
							errs <- fmt.Errorf("Pull: %w", err)
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			running := d.Schema().GetAttribute(x)
			require.NotNil(t, running)
			requireStoredAgrees(t, d, running)
			require.NoError(t, d.Close())
			if !backend.Persistent {
				return
			}
			reopened, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: backend.Name, Path: path})
			require.NoError(t, err)
			defer reopened.Close()
			again := reopened.Schema().GetAttribute(x)
			require.NotNil(t, again)
			require.Same(t, running.Cardinality, again.Cardinality, "a reopen with no schema runs with what the store states")
			require.Equal(t, running.Unique, again.Unique, "a reopen with no schema runs with what the store states")
			requireStoredAgrees(t, reopened, again)
		})
	}
}

func TestSchemaFacts_AHistoryReadLeavesTheUniqueRuleInForce(t *testing.T) {
	plain, err := schema.NewBuilder().Attribute(":t/u").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	unique, err := schema.NewBuilder().Attribute(":t/u").Type(schema.TypeString).Unique(schema.UniqueValue).Add().Build()
	require.NoError(t, err)
	pattern, err := parser.ParsePullPattern(`[:t/u]`)
	require.NoError(t, err)
	first, second := datalog.NewIdentity("first"), datalog.NewIdentity("second")
	u := kw(":t/u")
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			d := backendDB(t, backend, DatabaseOptions{Schema: plain})
			defer d.Close()
			for _, claimant := range []datalog.Identity{first, second} {
				tx := d.NewTransaction()
				require.NoError(t, tx.Set(claimant, u, "x"))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			pulled, err := d.Pull(first, `[:t/u]`)
			require.NoError(t, err)
			require.Equal(t, "x", pulled["t/u"], "under the plain declaration the first claimant reads its value")
			held := d.Matcher()
			hist := d.History()
			require.NoError(t, d.SetSchema(unique))
			old, err := executor.NewPullExecutor(held, d).Pull(first, pattern)
			require.NoError(t, err)
			require.Equal(t, "x", old["t/u"], "a matcher minted under the plain declaration reads under it")
			pulled, err = d.Pull(first, `[:t/u]`)
			require.NoError(t, err)
			require.Nil(t, pulled["t/u"], "under :db.unique/value the later claimant owns the value")
			history, err := hist.Pull(first, `[:t/u]`)
			require.NoError(t, err)
			require.Equal(t, "x", history["t/u"], "History reads every assertion")
			pulled, err = d.Pull(first, `[:t/u]`)
			require.NoError(t, err)
			require.Nil(t, pulled["t/u"], "the History read leaves the unique rule in force")
			values, err := executor.CollectTuples(d.Query(`[:find ?v :in $ ?e :where [?e :t/u ?v]]`, first))
			require.NoError(t, err)
			require.Empty(t, values, "the bound-entity query reads under the unique rule")
			owners, err := executor.CollectTuples(d.Query(`[:find ?e :where [?e :t/u "x"]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{second}}, owners, "one entity owns the value")
		})
	}
}

// An ident longer than storage holds never reaches a store: the schema parser
// accepts an ident at the limit and refuses one past it, an import line
// naming an attribute at the limit stores its datom under that name, and an
// import line naming an attribute past it is refused and stores nothing
// under the ident's storable prefix.
func TestImport_AnAttributeLongerThanStorableIsRefused(t *testing.T) {
	ident32 := ":t/" + strings.Repeat("x", 29)
	ident33 := ident32 + "y"
	_, err := schema.ParseSchema("{" + ident32 + " {:db/valueType :db.type/string}}")
	require.NoError(t, err, "an ident at the limit is accepted")
	_, err = schema.ParseSchema("{" + ident33 + " {:db/valueType :db.type/string}}")
	require.Error(t, err, "an ident past the limit is refused")

	line := func(entity, attribute, value string) string {
		return fmt.Sprintf("[%s %s %q [1 99] :op/none]\n", FormatValueEDN(datalog.NewIdentity(entity)), attribute, value)
	}
	tuplesOf := func(t *testing.T, d *Database, attribute string) [][]interface{} {
		t.Helper()
		tuples, err := executor.CollectTuples(d.Query(fmt.Sprintf("[:find ?v :where [?e %s ?v]]", attribute)))
		require.NoError(t, err)
		return tuples
	}

	within, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory"})
	require.NoError(t, err)
	defer within.Close()
	require.NoError(t, within.Import(strings.NewReader(line("e1", ident32, "at the limit"))))
	require.Equal(t, [][]interface{}{{"at the limit"}}, tuplesOf(t, within, ident32), "a datom on an attribute at the limit is stored under its name")

	past, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory"})
	require.NoError(t, err)
	defer past.Close()
	require.Error(t, past.Import(strings.NewReader(line("e1", ident33+"zzz", "long")+line("e2", ident33, "short"))), "an import line naming an attribute past the limit is refused")
	require.Empty(t, tuplesOf(t, past, ident32), "nothing is stored under the storable prefix")
	require.Empty(t, tuplesOf(t, past, ident33), "nothing is stored under the refused name")
}
