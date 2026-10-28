package storage

import (
	"bytes"
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

// A database runs with what its store states, and every writer of a
// definition validates it before a fact is asserted: a provider's definition
// the schema would refuse never reaches the store, a refused SetSchema
// records nothing, a branch reads its parent's given and declared schema as
// one state, a stored declaration of an engine word never runs, a user's
// write of an engine word is refused at Add, and a changed definition
// reaches every reader (docs/reference/SCHEMA.md, "Schema as Facts";
// docs/proposals/NESTED_SCOPES_AS_QUERIES.md §8).

// definitionProvider is a SchemaProvider handing out definitions as given,
// the type a caller supplies to DatabaseOptions.Schema and to SetSchema
// (docs/reference/SCHEMA.md, "For custom schema implementations").
type definitionProvider []*schema.AttributeDefinition

func (p definitionProvider) GetAttribute(a datalog.Keyword) *schema.AttributeDefinition {
	for _, def := range p {
		if def.Ident == a {
			return def
		}
	}
	return nil
}

func (definitionProvider) HasSchema() bool { return true }

func (p definitionProvider) IsRef(a datalog.Keyword) bool {
	def := p.GetAttribute(a)
	return def != nil && def.ValueType == schema.TypeRef
}

func (p definitionProvider) IsMany(a datalog.Keyword) bool {
	def := p.GetAttribute(a)
	return def != nil && def.Cardinality == schema.CardinalityMany
}

func (p definitionProvider) Attributes() []*schema.AttributeDefinition { return p }

// validSchema declares :t/x as a string.
func validSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().Attribute(":t/x").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	return s
}

// cardinalitySchema declares :t/x as a string of the cardinality named.
func cardinalitySchema(t *testing.T, many bool) *schema.Schema {
	t.Helper()
	b := schema.NewBuilder().Attribute(":t/x").Type(schema.TypeString)
	if many {
		b = b.Many()
	}
	s, err := b.Add().Build()
	require.NoError(t, err)
	return s
}

// A definition a provider hands out is validated through the one validation
// Schema.Add runs before any fact is asserted: an over-long ident or a word
// outside its vocabulary is refused at open and at SetSchema, the store is
// untouched, and it reopens with no schema and with a valid one.
func TestSchemaFacts_AProvidersDefinitionIsValidatedBeforeItIsAsserted(t *testing.T) {
	cases := []struct {
		name string
		def  *schema.AttributeDefinition
	}{
		{"an ident longer than storable", &schema.AttributeDefinition{Ident: kw(":order/shipping-address-line-two2"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne}},
		{"a cardinality outside the vocabulary", &schema.AttributeDefinition{Ident: kw(":t/y"), ValueType: schema.TypeString, Cardinality: kw(":db.cardinality/bogus")}},
	}
	for _, c := range cases {
		t.Run(c.name+"/at open", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			_, err := NewDatabaseWithOptions(DatabaseOptions{Path: path, Schema: definitionProvider{c.def}})
			require.Error(t, err, "the provider's definition is refused before it is asserted")
			reopened, err := NewDatabaseWithOptions(DatabaseOptions{Path: path})
			require.NoError(t, err, "nothing was asserted, so the store reopens with no schema")
			require.NoError(t, reopened.Close())
			reopened, err = NewDatabaseWithOptions(DatabaseOptions{Path: path, Schema: validSchema(t)})
			require.NoError(t, err, "nothing was asserted, so the store reopens with a valid schema")
			require.NoError(t, reopened.Close())
		})
		t.Run(c.name+"/at SetSchema", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			d, err := NewDatabaseWithOptions(DatabaseOptions{Path: path, Schema: validSchema(t)})
			require.NoError(t, err)
			require.Error(t, d.SetSchema(definitionProvider{c.def}), "the provider's definition is refused before it is asserted")
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/x")), "the database runs on with the schema it had")
			require.NoError(t, d.Close())
			reopened, err := NewDatabaseWithOptions(DatabaseOptions{Path: path})
			require.NoError(t, err, "nothing was asserted, so the store reopens with no schema")
			require.NoError(t, reopened.Close())
		})
	}
}

// heldStore holds the rewind's delete until the test releases it, so a call
// can be made while the rewind is in progress.
type heldStore struct {
	*BadgerEmulatorStore
	held, release chan struct{}
}

func (s *heldStore) DeleteDatoms(datoms []datalog.Datom) (int, error) {
	close(s.held)
	<-s.release
	return s.BadgerEmulatorStore.DeleteDatoms(datoms)
}

// A SetSchema refused while a rewind is in progress records nothing: the
// database runs after the rewind with the declarations that survive it, and
// an import of nothing afterwards declares the schema it ran with, never the
// refused one.
func TestSchemaFacts_ARefusedSetSchemaRecordsNothing(t *testing.T) {
	x := kw(":t/x")
	store := &heldStore{BadgerEmulatorStore: NewBadgerEmulatorStore(&BinaryKeyEncoder{}), held: make(chan struct{}), release: make(chan struct{})}
	d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store, Schema: cardinalitySchema(t, false)})
	require.NoError(t, err)
	_, err = d.Snapshot("s")
	require.NoError(t, err)
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("e"), x, "v"))
	_, err = tx.Commit()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- d.TruncateTo("s") }()
	<-store.held
	require.Error(t, d.SetSchema(cardinalitySchema(t, true)), "a declaration during a rewind is refused")
	close(store.release)
	require.NoError(t, <-done)
	require.Same(t, schema.CardinalityOne, d.Schema().GetAttribute(x).Cardinality, "the database runs with the declaration that survives the rewind")
	require.NoError(t, d.Import(strings.NewReader("")))
	require.Same(t, schema.CardinalityOne, d.Schema().GetAttribute(x).Cardinality, "an import of nothing declares the schema the database ran with, never the refused one")
}

// A branch reads its parent's given schema and declared definitions as one
// state, under the install lock, so a branch forked while the parent's
// schema is being declared runs with what its store states, and an import
// of nothing on the branch changes nothing. The control forks between
// installs.
func TestSchemaFacts_ForkReadsItsParentsSchemaUnderTheInstallLock(t *testing.T) {
	x := kw(":t/x")
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	schemas := []*schema.Schema{cardinalitySchema(t, false), cardinalitySchema(t, true)}
	stored := func(t *testing.T, d *Database) datalog.Keyword {
		t.Helper()
		var cards []datalog.Keyword
		require.NoError(t, d.QueryInto(&cards, `[:find ?card :where [?a :db.attribute/declaration ?d] [?d :db/ident :t/x] [?d :db/cardinality ?card]]`))
		require.Len(t, cards, 1, "one current declaration of :t/x")
		return cards[0]
	}
	agrees := func(t *testing.T, b *Database) {
		t.Helper()
		running := b.Schema().GetAttribute(x).Cardinality
		require.Same(t, stored(t, b), running, "the branch runs with what its store states")
		require.NoError(t, b.Import(strings.NewReader("")))
		require.Same(t, running, b.Schema().GetAttribute(x).Cardinality, "an import of nothing on the branch changes nothing")
	}
	for _, mode := range forking {
		t.Run(mode.name+"/forks between installs", func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemas[0]})
			_, err := d.Snapshot("s")
			require.NoError(t, err)
			for i := 0; i < 20; i++ {
				require.NoError(t, d.SetSchema(schemas[i%2]))
				b, err := d.Fork("s")
				require.NoError(t, err)
				require.Same(t, d.Schema().GetAttribute(x).Cardinality, b.Schema().GetAttribute(x).Cardinality, "the branch runs with the parent's definition")
				agrees(t, b)
			}
		})
		t.Run(mode.name+"/forks during installs", func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemas[0]})
			_, err := d.Snapshot("s")
			require.NoError(t, err)
			var wg sync.WaitGroup
			var mu sync.Mutex
			var branches []*Database
			var errs []error
			fail := func(err error) {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
			for g := 0; g < 2; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 100; i++ {
						if err := d.SetSchema(schemas[(g+i)%2]); err != nil {
							fail(err)
						}
					}
				}(g)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					if err := d.Import(strings.NewReader("")); err != nil {
						fail(err)
					}
				}
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 30; i++ {
					b, err := d.Fork("s")
					if err != nil {
						fail(err)
						continue
					}
					mu.Lock()
					branches = append(branches, b)
					mu.Unlock()
				}
			}()
			wg.Wait()
			require.Empty(t, errs)
			require.Len(t, branches, 30)
			require.Same(t, stored(t, d), d.Schema().GetAttribute(x).Cardinality, "the parent runs with what its store states")
			for _, b := range branches {
				agrees(t, b)
			}
		})
	}
}

// A stored declaration of an engine word never runs: a dump carrying a
// declaration of :db.attribute/declaration as cardinality many, written by
// the engine's own writer, imports into a database opened with no schema and
// into one opened with a given schema, each running the ref as cardinality
// one, after the import and after a SetSchema on the one with a schema, and
// every later declaration of an ordinary attribute takes effect.
func TestSchemaFacts_AStoredDeclarationOfAnEngineWordDoesNotRun(t *testing.T) {
	ref := kw(":db.attribute/declaration")
	x := kw(":t/x")
	many := &schema.AttributeDefinition{Ident: ref, Cardinality: schema.CardinalityMany}
	writer, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger-emulator", Schema: validSchema(t)})
	require.NoError(t, err)
	tx := writer.NewTransaction()
	tx.engine = true
	declaration := declarationEntity(many)
	require.NoError(t, tx.Add(declaration, schema.DBIdent, many.Ident))
	require.NoError(t, tx.Add(declaration, schema.DBCardinality, many.Cardinality))
	require.NoError(t, tx.Add(declaration, schema.DBUniqueElements, false))
	require.NoError(t, tx.Add(declaration, schema.DBNeverZeroValue, false))
	require.NoError(t, tx.Add(attributeEntity(ref), attributeDeclarationAttr, declaration))
	_, err = tx.Commit()
	require.NoError(t, err)
	var dump bytes.Buffer
	require.NoError(t, writer.Export(&dump))

	for _, arm := range []struct {
		name  string
		given *schema.Schema
	}{{"no schema", nil}, {"a given schema", validSchema(t)}} {
		t.Run(arm.name, func(t *testing.T) {
			opts := DatabaseOptions{BackendName: "badger-emulator"}
			if arm.given != nil {
				opts.Schema = arm.given
			}
			d, err := NewDatabaseWithOptions(opts)
			require.NoError(t, err)
			require.NoError(t, d.Import(bytes.NewReader(dump.Bytes())))
			require.Same(t, schema.CardinalityOne, d.Schema().GetAttribute(ref).Cardinality, "the engine's word resolves by the engine's declaration, whatever the store holds")
			if arm.given != nil {
				require.NoError(t, d.SetSchema(arm.given))
				require.Same(t, schema.CardinalityOne, d.Schema().GetAttribute(ref).Cardinality, "the engine's word resolves by the engine's declaration after a SetSchema")
			}
			for _, many := range []bool{true, false, true, false} {
				require.NoError(t, d.SetSchema(cardinalitySchema(t, many)))
				want := schema.CardinalityOne
				if many {
					want = schema.CardinalityMany
				}
				require.Same(t, want, d.Schema().GetAttribute(x).Cardinality, "a declaration of an ordinary attribute takes effect")
			}
		})
	}
}

// A user's write of an engine word is refused at Add, whatever the value:
// the definition's ref with a ref value and with a string, and :db/ident
// with :db/cardinality on an ordinary entity. The store is untouched, so
// SetSchema and a reopen with and without a schema all succeed.
func TestSchemaFacts_AUsersWriteOfAnEngineWordIsRefusedAtAdd(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":person/name").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	cases := []struct {
		name  string
		write func(tx *Transaction) error
	}{
		{"the declaration ref to an ordinary entity", func(tx *Transaction) error {
			return tx.Add(datalog.NewIdentity("doc-1"), kw(":db.attribute/declaration"), datalog.NewIdentity("doc-2"))
		}},
		{"the declaration ref with a string value", func(tx *Transaction) error {
			return tx.Add(datalog.NewIdentity("doc-1"), kw(":db.attribute/declaration"), "draft")
		}},
		{":db/ident on an ordinary entity", func(tx *Transaction) error {
			ent := datalog.NewIdentity("my-attr-def")
			if err := tx.Add(ent, kw(":db/ident"), kw(":person/email")); err != nil {
				return err
			}
			return tx.Add(ent, kw(":db/cardinality"), schema.CardinalityOne)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			d, err := NewDatabaseWithOptions(DatabaseOptions{Path: path, Schema: s})
			require.NoError(t, err)
			tx := d.NewTransaction()
			require.Error(t, c.write(tx), "the engine alone writes its words")
			require.NoError(t, tx.Rollback())
			require.NoError(t, d.SetSchema(s))
			require.NoError(t, d.Close())
			reopened, err := NewDatabaseWithOptions(DatabaseOptions{Path: path, Schema: s})
			require.NoError(t, err)
			require.NoError(t, reopened.Close())
			reopened, err = NewDatabaseWithOptions(DatabaseOptions{Path: path})
			require.NoError(t, err)
			require.NoError(t, reopened.Close())
		})
	}
}

// A given schema that redeclares an engine word is refused at open and at
// SetSchema, the database runs on with the schema it had, and an import of
// nothing afterwards changes nothing.
func TestSchemaFacts_RedeclaringAnEngineWordIsRefusedAtOpenAndAtSetSchema(t *testing.T) {
	x := kw(":t/x")
	for _, word := range []string{":db.attribute/declaration", ":db/cardinality", ":db/ident"} {
		t.Run(word, func(t *testing.T) {
			redeclaring, err := schema.NewBuilder().Attribute(":t/x").Type(schema.TypeString).Many().Add().
				Attribute(word).Many().Add().Build()
			require.NoError(t, err)
			_, err = NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger-emulator", Schema: redeclaring})
			require.Error(t, err, "a schema declaring an engine word is refused at open")

			d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger-emulator", Schema: cardinalitySchema(t, false)})
			require.NoError(t, err)
			require.Error(t, d.SetSchema(redeclaring), "a schema declaring an engine word is refused at SetSchema")
			require.Same(t, schema.CardinalityOne, d.Schema().GetAttribute(x).Cardinality, "the refused call leaves the given schema as it was")
			require.NoError(t, d.Import(strings.NewReader("")))
			require.Same(t, schema.CardinalityOne, d.Schema().GetAttribute(x).Cardinality, "an import of nothing changes nothing")
		})
	}
}

// An ident longer than storage holds never reaches a store: the builder and
// the parser refuse it, and Schema.Add panics on it.
func TestSchemaFacts_AnIdentLongerThanStorableNeverReachesTheStore(t *testing.T) {
	const long = ":order/shipping-address-line-two2"
	require.Greater(t, len(long), datalog.MaxAttributeBytes)
	_, err := schema.NewBuilder().Attribute(long).Type(schema.TypeString).Add().Build()
	require.Error(t, err)
	_, err = schema.ParseSchema(`{` + long + ` {:db/valueType :db.type/string}}`)
	require.Error(t, err)
	require.Panics(t, func() {
		schema.NewSchema().Add(&schema.AttributeDefinition{Ident: kw(long), ValueType: schema.TypeString})
	})
}

// The definitions a store states read back through ordinary patterns: after
// a declaration of :t/x as many and then as one, the database runs it one,
// the definition's current declaration is the one, no declaration of many
// is current, and a dump of the store imported into a database with no
// schema reads the same, the engine's words declared in its running schema.
func TestSchemaFacts_StoredDefinitionsReadBackThroughPatterns(t *testing.T) {
	x := kw(":t/x")
	for _, backend := range []string{"badger-emulator", "memory-trees"} {
		for _, given := range []bool{true, false} {
			name := backend + "/set"
			if given {
				name = backend + "/given"
			}
			t.Run(name, func(t *testing.T) {
				opts := DatabaseOptions{BackendName: backend}
				if given {
					opts.Schema = cardinalitySchema(t, true)
				}
				d, err := NewDatabaseWithOptions(opts)
				require.NoError(t, err)
				if !given {
					require.NoError(t, d.SetSchema(cardinalitySchema(t, true)))
				}
				require.NoError(t, d.SetSchema(cardinalitySchema(t, false)))
				require.Same(t, schema.CardinalityOne, d.Schema().GetAttribute(x).Cardinality)
				tuples, err := executor.CollectTuples(d.Query(`[:find ?ident ?type ?card :where [?a :db.attribute/declaration ?d] [?d :db/ident :t/x] [?d :db/ident ?ident] [?d :db/valueType ?type] [?d :db/cardinality ?card]]`))
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{x, schema.TypeString, schema.CardinalityOne}}, tuples, "the definition's current declaration is the last one")
				manyOnes, err := executor.CollectTuples(d.Query(`[:find ?ident :where [?a :db.attribute/declaration ?d] [?d :db/cardinality :db.cardinality/many] [?d :db/ident ?ident]]`))
				require.NoError(t, err)
				require.Empty(t, manyOnes, "the superseded declaration is current for no definition")

				var dump bytes.Buffer
				require.NoError(t, d.Export(&dump))
				n, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: backend})
				require.NoError(t, err)
				require.NoError(t, n.Import(bytes.NewReader(dump.Bytes())))
				require.NotNil(t, n.Schema().GetAttribute(kw(":db.attribute/declaration")), "the engine's word is declared in every running schema")
				cards, err := executor.CollectTuples(n.Query(`[:find ?card :where [?a :db.attribute/declaration ?d] [?d :db/ident :t/x] [?d :db/cardinality ?card]]`))
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{schema.CardinalityOne}}, cards, "the imported store states the one current declaration")
			})
		}
	}
}

// A branch runs with its parent's declared definitions: one the parent
// declared after the snapshot reaches the branch, is written and read on it,
// and is carried by the branch's dump, which imports into a fresh database
// as a declaration; and a parent rewound past a declaration forks a branch
// running with the declarations that survived.
func TestSchemaFacts_ABranchRunsWithItsParentsDeclarations(t *testing.T) {
	x, y := kw(":t/x"), kw(":t/y")
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	onlyX := validSchema(t)
	withY, err := schema.NewBuilder().Attribute(":t/x").Type(schema.TypeString).Add().
		Attribute(":t/y").Type(schema.TypeString).Many().Add().Build()
	require.NoError(t, err)
	for _, mode := range forking {
		t.Run(mode.name+"/a declaration after the snapshot reaches the branch", func(t *testing.T) {
			p := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: onlyX, RefuseUnknownAttributes: true})
			_, err := p.Snapshot("s")
			require.NoError(t, err)
			require.NoError(t, p.SetSchema(withY))
			require.NoError(t, p.SetSchema(onlyX))
			require.Same(t, schema.CardinalityMany, p.Schema().GetAttribute(y).Cardinality, "a declaration a later schema does not name stands")
			b, err := p.Fork("s")
			require.NoError(t, err)
			require.Same(t, schema.CardinalityMany, b.Schema().GetAttribute(y).Cardinality, "the parent's declaration reaches the branch")
			tx := b.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("e"), y, "v"))
			_, err = tx.Commit()
			require.NoError(t, err)
			values, err := executor.CollectTuples(b.Query(`[:find ?v :where [?e :t/y ?v]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"v"}}, values)
			var dump bytes.Buffer
			require.NoError(t, b.Export(&dump))
			r := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: onlyX, RefuseUnknownAttributes: true})
			require.NoError(t, r.Import(bytes.NewReader(dump.Bytes())))
			require.Same(t, schema.CardinalityMany, r.Schema().GetAttribute(y).Cardinality, "the branch's dump describes itself")
		})
		t.Run(mode.name+"/a rewound parent forks what survived", func(t *testing.T) {
			q := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: cardinalitySchema(t, true)})
			_, err := q.Snapshot("s0")
			require.NoError(t, err)
			require.NoError(t, q.SetSchema(cardinalitySchema(t, false)))
			require.NoError(t, q.TruncateTo("s0"))
			require.Same(t, schema.CardinalityMany, q.Schema().GetAttribute(x).Cardinality, "the parent runs with the declaration that survived the rewind")
			_, err = q.Snapshot("s1")
			require.NoError(t, err)
			b, err := q.Fork("s1")
			require.NoError(t, err)
			require.Same(t, schema.CardinalityMany, b.Schema().GetAttribute(x).Cardinality, "the branch runs with what its store states")
		})
	}
}

// emailWrites asserts :t/email "a1" then "shared" on A, then "shared" on B,
// each in its own transaction, into a database on s with the cache as asked.
func emailWrites(t *testing.T, s *schema.Schema, disableCache bool) *Database {
	t.Helper()
	d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger-emulator", Schema: s, DisableCache: disableCache})
	require.NoError(t, err)
	email := kw(":t/email")
	for _, w := range []struct{ e, v string }{{"A", "a1"}, {"A", "shared"}, {"B", "shared"}} {
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(datalog.NewIdentity(w.e), email, w.v))
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	return d
}

// A changed uniqueness constraint reaches every reader: under :db.unique/value
// B, the last to assert "shared", owns it and A reads "a1", and under a
// plain declaration A reads "shared". A fresh pull and a bound-entity query
// after SetSchema read under the new rule, and a matcher minted before the
// change reads under the rule it was minted under, whose cache entry carries
// it.
func TestSchemaFacts_ChangedUniquenessReachesEveryReader(t *testing.T) {
	plain, err := schema.NewBuilder().Attribute(":t/email").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	unique, err := schema.NewBuilder().Attribute(":t/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().Build()
	require.NoError(t, err)
	pattern, err := parser.ParsePullPattern(`[:t/email]`)
	require.NoError(t, err)
	a := datalog.NewIdentity("A")
	directions := []struct {
		name      string
		from, to  *schema.Schema
		fresh     string
		underFrom string
	}{
		{"unique to plain", unique, plain, "shared", "a1"},
		{"plain to unique", plain, unique, "a1", "shared"},
	}
	for _, dir := range directions {
		t.Run(dir.name, func(t *testing.T) {
			for _, disable := range []bool{true, false} {
				d := emailWrites(t, unique, disable)
				require.NoError(t, d.SetSchema(dir.from))
				held := d.Matcher()
				require.NoError(t, d.SetSchema(dir.to))
				old, err := executor.NewPullExecutor(held, d).Pull(a, pattern)
				require.NoError(t, err)
				require.Equal(t, dir.underFrom, old["t/email"], "a matcher minted before the change reads under the rule it was minted under (cache disabled %v)", disable)
				pulled, err := d.Pull(a, `[:t/email]`)
				require.NoError(t, err)
				require.Equal(t, dir.fresh, pulled["t/email"], "a fresh pull reads under the new rule (cache disabled %v)", disable)
				var values []string
				require.NoError(t, d.QueryInto(&values, `[:find ?v :in $ ?e :where [?e :t/email ?v]]`, a))
				require.Equal(t, []string{dir.fresh}, values, "a bound-entity query reads under the new rule (cache disabled %v)", disable)
			}
		})
	}
}

// A unique attribute resolves alike with the cache on and off: under
// :db.unique/value from open, B owns "shared" and A reads "a1" through a pull
// and through a bound-entity query, whichever way the database caches.
func TestSchemaFacts_AUniqueAttributeResolvesAlikeWithTheCacheOff(t *testing.T) {
	unique, err := schema.NewBuilder().Attribute(":t/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().Build()
	require.NoError(t, err)
	a := datalog.NewIdentity("A")
	for _, disable := range []bool{false, true} {
		d := emailWrites(t, unique, disable)
		pulled, err := d.Pull(a, `[:t/email]`)
		require.NoError(t, err)
		require.Equal(t, "a1", pulled["t/email"], "B owns the shared value, so A reads its own (cache disabled %v)", disable)
		var values []string
		require.NoError(t, d.QueryInto(&values, `[:find ?v :in $ ?e :where [?e :t/email ?v]]`, a))
		require.Equal(t, []string{"a1"}, values, "the bound-entity query reads what the pull reads (cache disabled %v)", disable)
	}
}

// Two SetSchema calls racing from the same start leave the database running
// the definition its store states, and an import of nothing afterwards
// changes nothing, over many trials.
func TestSchemaFacts_ConcurrentSetSchemaCallsAgreeWithTheStore(t *testing.T) {
	x := kw(":t/x")
	schemas := []*schema.Schema{cardinalitySchema(t, true), cardinalitySchema(t, false)}
	for trial := 0; trial < 500; trial++ {
		d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger-emulator"})
		require.NoError(t, err)
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, len(schemas))
		for _, s := range schemas {
			wg.Add(1)
			go func(s *schema.Schema) {
				defer wg.Done()
				<-start
				errs <- d.SetSchema(s)
			}(s)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		running := d.Schema().GetAttribute(x).Cardinality
		var stored []datalog.Keyword
		require.NoError(t, d.QueryInto(&stored, `[:find ?c :in $ ?def :where [?def :db.attribute/declaration ?a] [?a :db/cardinality ?c]]`, attributeEntity(x)))
		require.Equal(t, []datalog.Keyword{running}, stored, "trial %d: the database runs with what its store states", trial)
		require.NoError(t, d.Import(strings.NewReader("")))
		require.Same(t, running, d.Schema().GetAttribute(x).Cardinality, "trial %d: an import of nothing changes nothing", trial)
		require.NoError(t, d.Close())
	}
}
