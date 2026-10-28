package storage

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A schema given to a database is asserted into its store as facts: one
// entity per attribute, content-addressed from the ident, carrying the
// definition's seven words. A store opened with no schema reads those facts
// back, and a dump carries them.

// schemaFactsEntity is the entity a stored definition of ident lives on.
func schemaFactsEntity(ident datalog.Keyword) datalog.Identity {
	return datalog.NewIdentity("db.attribute/" + ident.String())
}

// schemaFactsFixture declares one attribute per value type, a set, an ordered
// set, a unique attribute, a never-zero attribute and a documented one, so every
// word a definition can carry is exercised.
func schemaFactsFixture(t testing.TB) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":t/string").Type(schema.TypeString).Add().
		Attribute(":t/long").Type(schema.TypeLong).Add().
		Attribute(":t/double").Type(schema.TypeDouble).Add().
		Attribute(":t/boolean").Type(schema.TypeBoolean).Add().
		Attribute(":t/instant").Type(schema.TypeInstant).Add().
		Attribute(":t/bytes").Type(schema.TypeBytes).Add().
		Attribute(":t/ref").Type(schema.TypeRef).Add().
		Attribute(":t/keyword").Type(schema.TypeKeyword).Add().
		Attribute(":t/symbol").Type(schema.TypeSymbol).Add().
		Attribute(":t/tx").Type(schema.TypeTx).Add().
		Attribute(":t/many").Type(schema.TypeString).Many().Doc("members").Add().
		Attribute(":t/ordered").Type(schema.TypeString).OrderedSet().Add().
		Attribute(":t/unique").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Attribute(":t/nonzero").Type(schema.TypeLong).NeverZeroValue().Add().
		Attribute(":t/documented").Type(schema.TypeString).Doc("documented").Add().
		Build()
	require.NoError(t, err)
	return s
}

// declaredDefinition is one tuple of the query the proposal's §4 states: a
// definition entity, and the words of the declaration it refers to.
type declaredDefinition struct {
	Entity datalog.Identity `datalog:"?a"`
	Ident  datalog.Keyword  `datalog:"?ident"`
	Type   datalog.Keyword  `datalog:"?type"`
	Card   datalog.Keyword  `datalog:"?card"`
}

// declaredByQuery reads every stored definition through the §4 query, keyed
// by ident.
func declaredByQuery(t *testing.T, d *Database) map[datalog.Keyword]declaredDefinition {
	t.Helper()
	var tuples []declaredDefinition
	require.NoError(t, d.QueryInto(&tuples, `[:find ?a ?ident ?type ?card
		:where [?a :db.attribute/declaration ?d]
		       [?d :db/ident ?ident]
		       [?d :db/valueType ?type]
		       [?d :db/cardinality ?card]]`))
	byIdent := make(map[datalog.Keyword]declaredDefinition, len(tuples))
	for _, tuple := range tuples {
		byIdent[tuple.Ident] = tuple
	}
	return byIdent
}

// attributeFacts pulls the seven words off the declaration the definition
// entity of ident refers to. A word the declaration does not carry is absent
// from the result, and an ident with no definition entity has nil.
func attributeFacts(t *testing.T, d *Database, ident datalog.Keyword) map[string]interface{} {
	t.Helper()
	var declarations []datalog.Identity
	require.NoError(t, d.QueryInto(&declarations,
		`[:find ?d :in $ ?a :where [?a :db.attribute/declaration ?d]]`, schemaFactsEntity(ident)))
	if len(declarations) == 0 {
		return nil
	}
	require.Len(t, declarations, 1, "%s refers to one declaration", ident)
	facts, err := d.Pull(declarations[0],
		`[:db/ident :db/valueType :db/cardinality :db/unique :db/uniqueElements :db/neverZeroValue :db/doc]`)
	require.NoError(t, err)
	return facts
}

// requireDefinitionFacts asserts that the stored facts of def's ident state def
// word for word.
func requireDefinitionFacts(t *testing.T, d *Database, def *schema.AttributeDefinition) {
	t.Helper()
	facts := attributeFacts(t, d, def.Ident)
	require.NotNil(t, facts, "%s has no definition entity", def.Ident)
	require.Same(t, def.Ident, facts["db/ident"])
	require.Same(t, def.ValueType, facts["db/valueType"])
	require.Same(t, def.Cardinality, facts["db/cardinality"])
	if def.Unique == nil {
		_, carried := facts["db/unique"]
		require.False(t, carried, "%s declares no uniqueness constraint", def.Ident)
	} else {
		require.Same(t, def.Unique, facts["db/unique"])
	}
	require.Equal(t, def.UniqueElements, facts["db/uniqueElements"])
	require.Equal(t, def.NeverZeroValue, facts["db/neverZeroValue"])
	if def.Doc == "" {
		_, carried := facts["db/doc"]
		require.False(t, carried, "%s carries no doc", def.Ident)
	} else {
		require.Equal(t, def.Doc, facts["db/doc"])
	}
}

// reopenLegs is the reopen cases crossed with both optimizer paths, fresh cases
// per leg so no store is shared across legs.
func reopenLegs(t *testing.T, run func(t *testing.T, open func(DatabaseOptions) *Database)) {
	t.Helper()
	for _, algebra := range []struct {
		name string
		on   bool
	}{{"algebra_on", true}, {"algebra_off", false}} {
		for _, rc := range reopenBackendCases() {
			t.Run(rc.name+"/"+algebra.name, func(t *testing.T) {
				open := func(opts DatabaseOptions) *Database {
					popts := DefaultPlannerOptions()
					popts.EnableAlgebraOptimizer = algebra.on
					opts.PlannerOptions = &popts
					return rc.open(t, opts)
				}
				run(t, open)
			})
		}
	}
}

func TestSchemaFacts_AssertedAtOpenReadBackByQuery(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			s := schemaFactsFixture(t)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})

			declared := declaredByQuery(t, d)
			require.Len(t, declared, s.Count())
			for _, def := range s.Attributes() {
				tuple, read := declared[def.Ident]
				require.True(t, read, "%s is not read back by the §4 query", def.Ident)
				require.Equal(t, schemaFactsEntity(def.Ident), tuple.Entity)
				require.Same(t, def.ValueType, tuple.Type)
				require.Same(t, def.Cardinality, tuple.Card)
				requireDefinitionFacts(t, d, def)
			}
		})
	}
}

func TestSchemaFacts_ReopenWithUnchangedSchemaWritesNothing(t *testing.T) {
	reopenLegs(t, func(t *testing.T, open func(DatabaseOptions) *Database) {
		s := schemaFactsFixture(t)
		first := open(DatabaseOptions{Schema: s})
		before, err := first.Store().MaxElementID()
		require.NoError(t, err)

		second := open(DatabaseOptions{Schema: s})
		after, err := second.Store().MaxElementID()
		require.NoError(t, err)
		require.Equal(t, before, after, "reopening with an unchanged schema wrote a datom")
	})
}

func TestSchemaFacts_ReassertedDefinitionConvergesToOneEntity(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			first, err := schema.NewBuilder().
				Attribute(":t/documented").Type(schema.TypeString).Doc("first").Add().
				Build()
			require.NoError(t, err)
			second, err := schema.NewBuilder().
				Attribute(":t/documented").Type(schema.TypeString).Doc("second").Add().
				Build()
			require.NoError(t, err)

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: first})
			require.NoError(t, d.SetSchema(second))

			var entities []datalog.Identity
			require.NoError(t, d.QueryInto(&entities,
				`[:find ?a :where [?a :db.attribute/declaration ?d] [?d :db/ident :t/documented]]`))
			require.Len(t, entities, 1, "a re-asserted definition converges to one entity")
			require.Equal(t, "second", attributeFacts(t, d, kw(":t/documented"))["db/doc"])

			var docs []string
			require.NoError(t, d.History().QueryInto(&docs,
				`[:find ?doc :in $ ?a :where [?a :db.attribute/declaration ?d] [?d :db/doc ?doc]]`,
				schemaFactsEntity(kw(":t/documented"))))
			require.ElementsMatch(t, []string{"first", "second"}, docs, "the ref's history is the definition's history")
		})
	}
}

func TestSchemaFacts_DefinitionConvergesAcrossReplicas(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			s := schemaFactsFixture(t)
			a := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, ReplicaID: 1})
			b := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, ReplicaID: 2})

			var dumpA, dumpB bytes.Buffer
			require.NoError(t, a.Export(&dumpA))
			require.NoError(t, b.Export(&dumpB))

			c := createOptimizerModeDB(t, mode, DatabaseOptions{})
			require.NoError(t, c.Import(strings.NewReader(dumpA.String())))
			require.NoError(t, c.Import(strings.NewReader(dumpB.String())))

			var entities []datalog.Identity
			require.NoError(t, c.QueryInto(&entities, `[:find ?a :where [?a :db/ident :t/many]]`))
			require.Len(t, entities, 1, "two replicas' declarations of one ident converge to one entity")

			many := c.Schema().GetAttribute(kw(":t/many"))
			require.NotNil(t, many, "an import ends by reading the definitions it carried")
			require.Same(t, schema.CardinalityMany, many.Cardinality)
			require.Equal(t, "members", many.Doc)
		})
	}
}

// Two replicas' declarations of one attribute merge to a definition one of
// them made.
func TestSchemaFacts_ReplicaDeclarationsMergeToADeclaredDefinition(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			n := kw(":t/n")
			base, err := schema.NewBuilder().Attribute(":t/n").Type(schema.TypeLong).Add().Build()
			require.NoError(t, err)
			nonzero, err := schema.NewBuilder().Attribute(":t/n").Type(schema.TypeLong).NeverZeroValue().Add().Build()
			require.NoError(t, err)
			untyped, err := schema.NewBuilder().Attribute(":t/n").Add().Build()
			require.NoError(t, err)

			origin := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: base, ReplicaID: 1})
			var dumpOrigin bytes.Buffer
			require.NoError(t, origin.Export(&dumpOrigin))

			a := createOptimizerModeDB(t, mode, DatabaseOptions{ReplicaID: 2})
			require.NoError(t, a.Import(strings.NewReader(dumpOrigin.String())))
			require.NoError(t, a.SetSchema(nonzero))
			b := createOptimizerModeDB(t, mode, DatabaseOptions{ReplicaID: 3})
			require.NoError(t, b.Import(strings.NewReader(dumpOrigin.String())))
			require.NoError(t, b.SetSchema(untyped))

			var dumpB bytes.Buffer
			require.NoError(t, b.Export(&dumpB))
			require.NoError(t, a.Import(strings.NewReader(dumpB.String())), "two replicas' declarations of one attribute merge")

			merged := a.Schema().GetAttribute(n)
			require.NotNil(t, merged)
			declared := []schema.AttributeDefinition{*base.GetAttribute(n), *nonzero.GetAttribute(n), *untyped.GetAttribute(n)}
			require.Contains(t, declared, *merged, "the merged definition is one some declaration made")

			var dumpA bytes.Buffer
			require.NoError(t, a.Export(&dumpA))
			c := createOptimizerModeDB(t, mode, DatabaseOptions{})
			require.NoError(t, c.Import(strings.NewReader(dumpA.String())), "the merged dump opens")
		})
	}
}

func TestSchemaFacts_WithdrawnDocResolvesAbsent(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			documented, err := schema.NewBuilder().
				Attribute(":t/documented").Type(schema.TypeString).Doc("documented").Add().
				Build()
			require.NoError(t, err)
			undocumented, err := schema.NewBuilder().
				Attribute(":t/documented").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: documented})
			entity := schemaFactsEntity(kw(":t/documented"))
			docOf := func() []string {
				var docs []string
				require.NoError(t, d.QueryInto(&docs,
					`[:find ?doc :in $ ?a :where [?a :db.attribute/declaration ?d] [?d :db/doc ?doc]]`, entity))
				return docs
			}
			require.Equal(t, []string{"documented"}, docOf())

			require.NoError(t, d.SetSchema(undocumented))
			require.Empty(t, docOf(), "a withdrawn doc no longer resolves")
			require.Equal(t, "", d.Schema().GetAttribute(kw(":t/documented")).Doc)

			var history []string
			require.NoError(t, d.History().QueryInto(&history,
				`[:find ?doc :in $ ?a :where [?a :db.attribute/declaration ?d] [?d :db/doc ?doc]]`, entity))
			require.Contains(t, history, "documented", "History keeps the withdrawn doc")

			require.NoError(t, d.SetSchema(nil))
			require.Equal(t, "", d.Schema().GetAttribute(kw(":t/documented")).Doc,
				"with no given schema the database runs with the stored declaration, which carries no doc")
			require.Empty(t, docOf())
		})
	}
}

func TestSchemaFacts_WithdrawnWordResolvesAbsent(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			constrained, err := schema.NewBuilder().
				Attribute(":t/unique").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
				Build()
			require.NoError(t, err)
			unconstrained, err := schema.NewBuilder().
				Attribute(":t/unique").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: constrained})
			entity := schemaFactsEntity(kw(":t/unique"))
			uniqueOf := func() []datalog.Keyword {
				var constraints []datalog.Keyword
				require.NoError(t, d.QueryInto(&constraints,
					`[:find ?u :in $ ?a :where [?a :db.attribute/declaration ?d] [?d :db/unique ?u]]`, entity))
				return constraints
			}
			require.Equal(t, []datalog.Keyword{schema.UniqueValue}, uniqueOf())

			require.NoError(t, d.SetSchema(unconstrained))
			require.Empty(t, uniqueOf(), "a withdrawn uniqueness constraint no longer resolves")
			require.Nil(t, d.Schema().GetAttribute(kw(":t/unique")).Unique)

			var history []datalog.Keyword
			require.NoError(t, d.History().QueryInto(&history,
				`[:find ?u :in $ ?a :where [?a :db.attribute/declaration ?d] [?d :db/unique ?u]]`, entity))
			require.Contains(t, history, schema.UniqueValue, "History keeps the withdrawn value")
		})
	}
}

func TestSchemaFacts_OpenWithNoSchemaReadsStoredFacts(t *testing.T) {
	reopenLegs(t, func(t *testing.T, open func(DatabaseOptions) *Database) {
		s := schemaFactsFixture(t)
		first := open(DatabaseOptions{Schema: s})
		entity := datalog.NewIdentity("holder")
		tx := first.NewTransaction()
		for _, member := range []string{"x", "y", "z"} {
			require.NoError(t, tx.Add(entity, kw(":t/many"), member))
		}
		_, err := tx.Commit()
		require.NoError(t, err)

		second := open(DatabaseOptions{})
		var members []string
		require.NoError(t, second.QueryInto(&members, `[:find ?v :where [?e :t/many ?v]]`))
		require.ElementsMatch(t, []string{"x", "y", "z"}, members)

		for _, def := range s.Attributes() {
			got := second.Schema().GetAttribute(def.Ident)
			require.NotNil(t, got, "%s is not declared after a reopen with no schema", def.Ident)
			require.Equal(t, *def, *got, "%s differs from its declaration", def.Ident)
		}
	})
}

// A given schema supersedes the stored definitions for the idents it names,
// and the stored definitions of the other idents stay in the running schema:
// a store declaring two attributes, reopened with a schema naming one of them
// under another doc, runs with the given definition of that one and the
// stored definition of the other, and the store states both.
func TestSchemaFacts_GivenSchemaNamingFewerIdentsKeepsTheOthers(t *testing.T) {
	reopenLegs(t, func(t *testing.T, open func(DatabaseOptions) *Database) {
		both, err := schema.NewBuilder().
			Attribute(":t/named").Type(schema.TypeString).Doc("first").Add().
			Attribute(":t/kept").Type(schema.TypeLong).Many().Add().
			Build()
		require.NoError(t, err)
		first := open(DatabaseOptions{Schema: both})
		requireDefinitionFacts(t, first, both.GetAttribute(kw(":t/kept")))

		one, err := schema.NewBuilder().
			Attribute(":t/named").Type(schema.TypeString).Doc("second").Add().
			Build()
		require.NoError(t, err)
		second := open(DatabaseOptions{Schema: one})

		named := second.Schema().GetAttribute(kw(":t/named"))
		require.NotNil(t, named)
		require.Equal(t, *one.GetAttribute(kw(":t/named")), *named,
			"the given schema supersedes the stored definition of the ident it names")
		kept := second.Schema().GetAttribute(kw(":t/kept"))
		require.NotNil(t, kept, "the stored definition of an ident the given schema does not name stays in the running schema")
		require.Equal(t, *both.GetAttribute(kw(":t/kept")), *kept)
		requireDefinitionFacts(t, second, named)
		requireDefinitionFacts(t, second, kept)
	})
}

func TestSchemaFacts_UndeclaredAttributeWithDatomsIsInferred(t *testing.T) {
	reopenLegs(t, func(t *testing.T, open func(DatabaseOptions) *Database) {
		s, err := schema.NewBuilder().
			Attribute(":t/declared").Type(schema.TypeString).Doc("declared").Add().
			Build()
		require.NoError(t, err)
		first := open(DatabaseOptions{Schema: s})
		entity := datalog.NewIdentity("holder")
		tx := first.NewTransaction()
		require.NoError(t, tx.Add(entity, kw(":t/declared"), "a"))
		require.NoError(t, tx.Add(entity, kw(":t/loose"), "b"))
		_, err = tx.Commit()
		require.NoError(t, err)

		second := open(DatabaseOptions{})
		declared := second.Schema().GetAttribute(kw(":t/declared"))
		require.NotNil(t, declared)
		require.Equal(t, "declared", declared.Doc, "the declared attribute comes from its facts")

		loose := second.Schema().GetAttribute(kw(":t/loose"))
		require.NotNil(t, loose, "the undeclared attribute with datoms is inferred")
		require.Same(t, schema.TypeString, loose.ValueType)
		require.Same(t, schema.CardinalityOne, loose.Cardinality)
	})
}

// A definition lives on its ident's own entity. An entity elsewhere carrying
// :db/ident and :db/cardinality, which a replay alone writes since the words
// are the engine's, is a fact of the application's, not a definition.
func TestSchemaFacts_IdentOnAnotherEntityIsNotADefinition(t *testing.T) {
	reopenLegs(t, func(t *testing.T, open func(DatabaseOptions) *Database) {
		s, err := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Build()
		require.NoError(t, err)
		first := open(DatabaseOptions{Schema: s})
		enum := datalog.NewIdentity("enum/email")
		require.NoError(t, first.Store().Assert([]datalog.Datom{
			{E: enum, A: schema.DBIdent, V: kw(":person/email"), Tx: first.clock.Next()},
			{E: enum, A: schema.DBCardinality, V: schema.CardinalityOne, Tx: first.clock.Next()},
		}))
		require.NoError(t, first.SetSchema(s), "a schema is declared over a store holding an ident elsewhere")

		second := open(DatabaseOptions{Schema: s})
		require.Nil(t, second.declaredSchema().GetAttribute(kw(":person/email")), "the ident elsewhere declares nothing")
		third := open(DatabaseOptions{})
		require.NotNil(t, third.Schema().GetAttribute(kw(":person/name")), "the stored definition is read")
		require.Nil(t, third.declaredSchema().GetAttribute(kw(":person/email")))
	})
}

func TestSchemaFacts_MalformedFactRefusesOpen(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			path := ""
			if mode.backend.Persistent {
				path = t.TempDir()
			}
			store, err := mode.backend.Open(path, &BinaryKeyEncoder{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })

			ident := kw(":t/bogus")
			declaration := datalog.NewIdentity("db.declaration/bogus")
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: declaration, A: schema.DBIdent, V: ident, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 7}},
				{E: declaration, A: schema.DBCardinality, V: kw(":db.cardinality/bogus"), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 7}},
				{E: schemaFactsEntity(ident), A: attributeDeclarationAttr, V: declaration, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 7}},
			}))

			popts := mode.plannerOptions()
			_, err = NewDatabaseWithOptions(DatabaseOptions{Store: store, PlannerOptions: &popts})
			require.Error(t, err, "a definition carrying a cardinality outside the vocabulary refuses the open")
			require.Contains(t, err.Error(), ":t/bogus")
			require.Contains(t, err.Error(), ":db.cardinality/bogus")
		})
	}
}

func TestSchemaFacts_SetSchemaRefusedOnTemporalHandle(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			s := schemaFactsFixture(t)
			require.Error(t, d.AsOf(datalog.ElementID{Lamport: 1, ReplicaID: 1}).SetSchema(s))
			require.Error(t, d.History().SetSchema(s))
		})
	}
}

func TestSchemaFacts_ForkAssertsParentDeclarations(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			before, err := schema.NewBuilder().
				Attribute(":t/early").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)
			after, err := schema.NewBuilder().
				Attribute(":t/early").Type(schema.TypeString).Add().
				Attribute(":t/later").Type(schema.TypeLong).Many().Add().
				Build()
			require.NoError(t, err)

			parent := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: before})
			_, err = parent.Snapshot("s")
			require.NoError(t, err)
			require.NoError(t, parent.SetSchema(after))

			branch := branchOf(t, parent, "s")
			declared := declaredByQuery(t, branch)
			later, read := declared[kw(":t/later")]
			require.True(t, read, "the branch carries the declaration its parent made after the snapshot")
			require.Same(t, schema.CardinalityMany, later.Card)
		})
	}
}

// A branch declares what its parent declares and nothing its parent only
// inferred.
func TestSchemaFacts_ForkCarriesDeclarationsAndNotInferences(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			inferring := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := inferring.NewTransaction()
			require.NoError(t, tx.Add(datalog.NewIdentity("holder"), kw(":t/plain"), "v"))
			_, err := tx.Commit()
			require.NoError(t, err)
			require.NoError(t, inferring.Import(strings.NewReader("")), "an import ends by installing the schema, inferring from the datoms")
			require.NotNil(t, inferring.Schema(), "the parent infers a schema from its datoms")
			require.NotNil(t, inferring.Schema().GetAttribute(kw(":t/plain")), "the parent infers the attribute from its datoms")
			_, err = inferring.Snapshot("s")
			require.NoError(t, err)
			branch := branchOf(t, inferring, "s")
			require.NoError(t, branch.Import(strings.NewReader("")))
			require.Empty(t, declaredByQuery(t, branch), "an inferred definition is never asserted, in the branch as in the parent")
			require.NoError(t, inferring.Import(strings.NewReader("")))
			require.Empty(t, declaredByQuery(t, inferring))

			declared, err := schema.NewBuilder().
				Attribute(":t/plain").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)
			declaring := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declared})
			_, err = declaring.Snapshot("s")
			require.NoError(t, err)
			branch = branchOf(t, declaring, "s")
			_, read := declaredByQuery(t, branch)[kw(":t/plain")]
			require.True(t, read, "the branch carries its parent's declaration")
		})
	}
}

// A branch runs with the schema its own store states: what the parent's
// declarations and its store's ops say as of the snapshot, whatever the
// parent published at its last install.
func TestSchemaFacts_ForkDerivesItsSchemaFromItsStore(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			holder := datalog.NewIdentity("holder")
			tags := kw(":t/tags")
			require.NoError(t, parent.Store().Assert([]datalog.Datom{
				{E: holder, A: tags, V: "x", Tx: parent.clock.Next(), Op: datalog.OpCRDTAdd},
				{E: holder, A: tags, V: "y", Tx: parent.clock.Next(), Op: datalog.OpCRDTAdd},
			}))
			_, err := parent.Snapshot("s")
			require.NoError(t, err)

			branch := branchOf(t, parent, "s")
			members, err := branch.GetStrings(holder, tags)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"x", "y"}, members, "the branch resolves the set its store's ops state")
		})
	}
}

func TestSchemaFacts_ForkWithUnchangedSchemaWritesNothing(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			parent := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			ceiling, err := parent.snapshotMarkerMax("s")
			require.NoError(t, err)

			branch := branchOf(t, parent, "s")
			top, err := branch.Store().MaxElementID()
			require.NoError(t, err)
			require.Equal(t, ceiling, top, "a fork of an unchanged schema wrote a datom")
		})
	}
}

func TestSchemaFacts_TruncateToRewindsDeclarations(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			before, err := schema.NewBuilder().
				Attribute(":t/early").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)
			after, err := schema.NewBuilder().
				Attribute(":t/early").Type(schema.TypeString).Add().
				Attribute(":t/later").Type(schema.TypeLong).Add().
				Build()
			require.NoError(t, err)

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: before})
			_, err = d.Snapshot("s")
			require.NoError(t, err)
			require.NoError(t, d.SetSchema(after))
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/later")))

			require.NoError(t, d.TruncateTo("s"))
			_, read := declaredByQuery(t, d)[kw(":t/later")]
			require.False(t, read, "a definition declared past the snapshot is gone from the facts after the rewind")
			require.Nil(t, d.Schema().GetAttribute(kw(":t/later")), "the database runs with the declarations that survive the rewind")
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/early")))

			require.NoError(t, d.SetSchema(after))
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/later")), "a schema declared again after the rewind is in force")
			_, read = declaredByQuery(t, d)[kw(":t/later")]
			require.True(t, read, "the declaration made again after the rewind is in the facts")
		})
	}
}

// A pull after SetSchema reads under the installed schema.
func TestSchemaFacts_ChangedCardinalityHoldsAgainstAMatcherMintedBefore(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			asSet, err := schema.NewBuilder().
				Attribute(":t/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			asOne, err := schema.NewBuilder().
				Attribute(":t/tags").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: asSet})
			entity := datalog.NewIdentity("holder")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(entity, kw(":t/tags"), "x"))
			require.NoError(t, tx.Add(entity, kw(":t/tags"), "y"))
			_, err = tx.Commit()
			require.NoError(t, err)

			held := d.Matcher()
			heldExecutor := d.NewExecutor()
			require.NoError(t, d.SetSchema(asOne))
			pattern, err := parser.ParsePullPattern(`[:t/tags]`)
			require.NoError(t, err)
			_, err = executor.NewPullExecutor(held, d).Pull(entity, pattern)
			require.NoError(t, err)
			q, err := parser.ParseQuery(`[:find ?t :where [?e :t/tags ?t]]`)
			require.NoError(t, err)
			_, err = executor.CollectTuples(heldExecutor.Execute(q))
			require.NoError(t, err, "an executor minted before SetSchema still runs")

			pulled, err := d.Pull(entity, `[:t/tags]`)
			require.NoError(t, err)
			require.Equal(t, "y", pulled["t/tags"], "under cardinality one the latest member is the value, whatever matcher read the pair before")
			tuples, err := executor.CollectTuples(d.Query(`[:find ?t :where [?e :t/tags ?t]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"y"}}, tuples, "a fresh query reads the one value, whatever executor read the pair before")
		})
	}
}

// A pull in flight across SetSchema leaves the next pull reading under the
// installed schema.
func TestSchemaFacts_ChangedCardinalityHoldsAgainstAPullInFlight(t *testing.T) {
	for _, algebra := range []struct {
		name string
		on   bool
	}{{"algebra_on", true}, {"algebra_off", false}} {
		t.Run(algebra.name, func(t *testing.T) {
			asSet, err := schema.NewBuilder().
				Attribute(":t/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			asOne, err := schema.NewBuilder().
				Attribute(":t/tags").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)

			opts := DefaultPlannerOptions()
			opts.EnableAlgebraOptimizer = algebra.on
			store := &scanSignalStore{Store: NewBadgerEmulatorStore(&BinaryKeyEncoder{})}
			d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store, Schema: asSet, PlannerOptions: &opts})
			require.NoError(t, err)
			defer d.Close()
			entity := datalog.NewIdentity("holder")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(entity, kw(":t/tags"), "x"))
			require.NoError(t, tx.Add(entity, kw(":t/tags"), "y"))
			_, err = tx.Commit()
			require.NoError(t, err)

			held := make(chan struct{})
			release := make(chan struct{})
			store.arm(func() {
				close(held)
				<-release
			})
			inFlight := make(chan error, 1)
			go func() {
				_, err := d.Pull(entity, `[:t/tags]`)
				inFlight <- err
			}()
			<-held
			require.NoError(t, d.SetSchema(asOne))
			close(release)
			require.NoError(t, <-inFlight)

			pulled, err := d.Pull(entity, `[:t/tags]`)
			require.NoError(t, err)
			require.Equal(t, "y", pulled["t/tags"], "under cardinality one the latest member is the value, whatever pull was in flight when it was declared")
		})
	}
}

// Two SetSchema calls that overlap leave the database running the definition
// its store states, and installs are serialized: the first call is held at
// its first scan, under the install's lock; the second call reaches the lock
// while the first is held and takes it only once the first has been released,
// which is read at the moment it takes the lock; so the second declares after
// the first and the second's definition is the one in force.
func TestSchemaFacts_OverlappingSetSchemaCallsAgree(t *testing.T) {
	n := kw(":t/n")
	base, err := schema.NewBuilder().Attribute(":t/n").Type(schema.TypeLong).Doc("one").Add().Build()
	require.NoError(t, err)
	redocumented, err := schema.NewBuilder().Attribute(":t/n").Type(schema.TypeLong).Doc("two").Add().Build()
	require.NoError(t, err)
	widened, err := schema.NewBuilder().Attribute(":t/n").Type(schema.TypeLong).Many().Doc("one").Add().Build()
	require.NoError(t, err)
	for _, algebra := range []struct {
		name string
		on   bool
	}{{"algebra_on", true}, {"algebra_off", false}} {
		t.Run(algebra.name, func(t *testing.T) {
			opts := DefaultPlannerOptions()
			opts.EnableAlgebraOptimizer = algebra.on
			store := &scanSignalStore{Store: NewBadgerEmulatorStore(&BinaryKeyEncoder{})}
			d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store, Schema: base, PlannerOptions: &opts})
			require.NoError(t, err)
			defer d.Close()

			held := make(chan struct{})
			release := make(chan struct{})
			store.arm(func() {
				close(held)
				<-release
			})
			first := make(chan error, 1)
			go func() { first <- d.SetSchema(redocumented) }()
			<-held
			arrived := make(chan struct{}, 1)
			d.onInstallLock = func() {
				select {
				case arrived <- struct{}{}:
				default:
				}
			}
			lockedAfterRelease := make(chan bool, 1)
			d.onInstallLocked = func() {
				select {
				case <-release:
					lockedAfterRelease <- true
				default:
					lockedAfterRelease <- false
				}
			}
			second := make(chan error, 1)
			go func() { second <- d.SetSchema(widened) }()
			<-arrived
			close(release)
			require.NoError(t, <-first)
			require.NoError(t, <-second)
			require.True(t, <-lockedAfterRelease, "the second call takes the install lock only once the first has been released")

			running := d.Schema().GetAttribute(n)
			require.NotNil(t, running)
			requireDefinitionFacts(t, d, running)
			require.Equal(t, *widened.GetAttribute(n), *running, "the call held under the lock declares first and the other declares last")
			require.NoError(t, d.Import(strings.NewReader("")))
			require.Equal(t, *running, *d.Schema().GetAttribute(n), "an import of nothing changes nothing")
		})
	}
}

func TestSchemaFacts_ChangedCardinalityInvalidatesCache(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			asSet, err := schema.NewBuilder().
				Attribute(":t/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			asOne, err := schema.NewBuilder().
				Attribute(":t/tags").Type(schema.TypeString).Add().
				Build()
			require.NoError(t, err)

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: asSet})
			entity := datalog.NewIdentity("holder")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(entity, kw(":t/tags"), "x"))
			require.NoError(t, tx.Add(entity, kw(":t/tags"), "y"))
			_, err = tx.Commit()
			require.NoError(t, err)

			pulled, err := d.Pull(entity, `[:t/tags]`)
			require.NoError(t, err)
			require.ElementsMatch(t, []interface{}{"x", "y"}, pulled["t/tags"])

			require.NoError(t, d.SetSchema(asOne))
			pulled, err = d.Pull(entity, `[:t/tags]`)
			require.NoError(t, err)
			require.Equal(t, "y", pulled["t/tags"], "under cardinality one the latest member is the value")
		})
	}
}

// engineWords is the engine's vocabulary with the type of each word: the
// commit mark, the entity id a pull pattern names, the snapshot marker's
// words, the branch record's words, the definition entity's ref to its
// declaration, and the seven words of a declaration.
var engineWords = []struct {
	word      datalog.Keyword
	valueType datalog.Keyword
}{
	{kw(":db/txInstant"), schema.TypeInstant},
	{kw(":db/id"), schema.TypeRef},
	{kw(":db.snapshot/name"), schema.TypeString},
	{kw(":db.snapshot/at-lamport"), schema.TypeLong},
	{kw(":db.snapshot/at-replica"), schema.TypeLong},
	{kw(":db.snapshot/created"), schema.TypeInstant},
	{kw(":db.branch/snapshot"), schema.TypeString},
	{kw(":db.branch/at-lamport"), schema.TypeLong},
	{kw(":db.branch/at-replica"), schema.TypeLong},
	{kw(":db.branch/fork-lamport"), schema.TypeLong},
	{kw(":db.branch/fork-replica"), schema.TypeLong},
	{kw(":db.branch/replica"), schema.TypeLong},
	{kw(":db.attribute/declaration"), schema.TypeRef},
	{kw(":db/ident"), schema.TypeKeyword},
	{kw(":db/valueType"), schema.TypeKeyword},
	{kw(":db/cardinality"), schema.TypeKeyword},
	{kw(":db/unique"), schema.TypeKeyword},
	{kw(":db/uniqueElements"), schema.TypeBoolean},
	{kw(":db/neverZeroValue"), schema.TypeBoolean},
	{kw(":db/doc"), schema.TypeString},
}

// The engine's words are declared into every running schema: a database
// declaring nothing runs with them, each of cardinality one with its type,
// and with no definition of a user's attribute.
func TestSchemaFacts_EngineWordsAreDeclaredInEveryRunningSchema(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			running := d.Schema()
			require.NotNil(t, running, "a database declaring nothing runs with the engine's schema")
			for _, w := range engineWords {
				def := running.GetAttribute(w.word)
				require.NotNil(t, def, "%s is declared", w.word)
				require.Same(t, schema.CardinalityOne, def.Cardinality, "%s resolves as one value", w.word)
				require.Same(t, w.valueType, def.ValueType, "%s carries its type", w.word)
			}
			require.Nil(t, running.GetAttribute(kw(":t/string")), "the database declares nothing of a user's")
		})
	}
}

// A database opened with a schema answers, through the declaration ref, the
// declaration in force alone once another supersedes it: the ref resolves by
// its declaration, never by the ops it carries.
func TestSchemaFacts_SupersededDeclarationDoesNotResolve(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			first, err := schema.NewBuilder().
				Attribute(":t/documented").Type(schema.TypeString).Doc("first").Add().
				Build()
			require.NoError(t, err)
			second, err := schema.NewBuilder().
				Attribute(":t/documented").Type(schema.TypeString).Doc("second").Add().
				Build()
			require.NoError(t, err)

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: first})
			require.NoError(t, d.SetSchema(second))

			var docs []string
			require.NoError(t, d.QueryInto(&docs,
				`[:find ?doc :where [?a :db.attribute/declaration ?d] [?d :db/ident :t/documented] [?d :db/doc ?doc]]`))
			require.Equal(t, []string{"second"}, docs, "the ref resolves to the declaration in force")
		})
	}
}

// The engine's words are the engine's to write: a user's write of one, by any
// of the four writers, is refused naming the word, and the engine's own
// writes pass: a schema's facts at open, a snapshot's marker, its deletion,
// and a branch's record.
func TestSchemaFacts_EngineWordsAreTheEnginesToWrite(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			require.NotEmpty(t, declaredByQuery(t, d), "the open declares the schema's facts")
			entity := datalog.NewIdentity("user")
			for _, w := range engineWords {
				tx := d.NewTransaction()
				require.ErrorContains(t, tx.Add(entity, w.word, "v"), w.word.String(), "Add of %s", w.word)
				require.ErrorContains(t, tx.Set(entity, w.word, "v"), w.word.String(), "Set of %s", w.word)
				require.ErrorContains(t, tx.Remove(entity, w.word, "v"), w.word.String(), "Remove of %s", w.word)
				require.ErrorContains(t, tx.Retract(entity, w.word, "v"), w.word.String(), "Retract of %s", w.word)
				require.NoError(t, tx.Rollback())
			}
			_, err := d.Snapshot("s")
			require.NoError(t, err, "the engine writes a snapshot's marker")
			require.NoError(t, d.DeleteSnapshot("s"), "the engine retracts a snapshot's marker")
		})
	}
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name+"/fork", func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			_, err := d.Snapshot("s")
			require.NoError(t, err)
			branchOf(t, d, "s")
			branches, err := d.branchesForkedAfter(datalog.ElementID{})
			require.NoError(t, err)
			require.Equal(t, []string{"s"}, branches, "the engine writes the branch's record")
		})
	}
}

// A given schema that redeclares an engine word is refused where a schema is
// given: at open, and at SetSchema, which leaves the given schema as it was.
func TestSchemaFacts_RedeclaringAnEngineWordIsRefused(t *testing.T) {
	redeclaring, err := schema.NewBuilder().
		Attribute(":t/new").Type(schema.TypeString).Add().
		Attribute(":db/ident").Type(schema.TypeKeyword).Many().Add().
		Build()
	require.NoError(t, err)
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			_, err := NewDatabaseWithOptions(backendOptions(t, mode.backend, DatabaseOptions{Schema: redeclaring, PlannerOptions: &popts}))
			require.ErrorContains(t, err, ":db/ident", "an open with a schema redeclaring an engine word is refused")

			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t)})
			require.ErrorContains(t, d.SetSchema(redeclaring), ":db/ident")
			require.Nil(t, d.Schema().GetAttribute(kw(":t/new")), "a refused schema declares nothing")
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/string")), "the database runs with the schema given before")
			require.NoError(t, d.Import(strings.NewReader("")), "the given schema is the one given before, so the import declares it again")
			require.Nil(t, d.Schema().GetAttribute(kw(":t/new")))
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/string")))
		})
	}
}

// A branch runs with the declarations it adopts: a definition its parent
// holds as a fact that the branch's store lacked at open is in force on the
// branch once adopted, so members written under a set the parent declared
// read back as the set.
func TestSchemaFacts_ForkRunsWithTheDeclarationsItAdopts(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			declaring, err := schema.NewBuilder().
				Attribute(":t/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			source := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: declaring})
			var dump bytes.Buffer
			require.NoError(t, source.Export(&dump))

			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, err = parent.Snapshot("s")
			require.NoError(t, err)
			require.NoError(t, parent.Import(strings.NewReader(dump.String())), "the parent holds the declaration as a fact after the snapshot")
			require.NotNil(t, parent.declaredSchema().GetAttribute(kw(":t/tags")))

			branch := branchOf(t, parent, "s")
			holder := datalog.NewIdentity("holder")
			tags := kw(":t/tags")
			tx := branch.NewTransaction()
			require.NoError(t, tx.Add(holder, tags, "x"))
			require.NoError(t, tx.Add(holder, tags, "y"))
			_, err = tx.Commit()
			require.NoError(t, err)
			members, err := branch.GetStrings(holder, tags)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"x", "y"}, members, "the branch runs with the set its parent declared")
		})
	}
}

// uniqueTakeover opens a database on mode declaring :t/u unique, with one
// entity claiming "x" and another claiming "x" after it, so the first's
// claim is superseded under the unique rule and current under the plain one.
func uniqueTakeover(t *testing.T, mode optimizerMode) (*Database, datalog.Identity) {
	t.Helper()
	unique, err := schema.NewBuilder().
		Attribute(":t/u").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)
	d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: unique})
	first, second := datalog.NewIdentity("first"), datalog.NewIdentity("second")
	tx := d.NewTransaction()
	require.NoError(t, tx.Set(first, kw(":t/u"), "x"))
	_, err = tx.Commit()
	require.NoError(t, err)
	tx = d.NewTransaction()
	require.NoError(t, tx.Set(second, kw(":t/u"), "x"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d, first
}

// A cache entry carries the rule it was resolved under, uniqueness included:
// a matcher minted before a SetSchema that makes a unique attribute plain
// resolves a superseded claimant under the unique rule and stores that, and a
// fresh pull under the plain rule reads the claimant's value, never the entry.
func TestSchemaFacts_ChangedUniquenessHoldsAgainstAMatcherMintedBefore(t *testing.T) {
	plain, err := schema.NewBuilder().
		Attribute(":t/u").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			d, first := uniqueTakeover(t, mode)
			held := d.Matcher()
			require.NoError(t, d.SetSchema(plain))

			pattern, err := parser.ParsePullPattern(`[:t/u]`)
			require.NoError(t, err)
			superseded, err := executor.NewPullExecutor(held, d).Pull(first, pattern)
			require.NoError(t, err)
			require.Nil(t, superseded["t/u"], "the matcher minted under the unique rule resolves the superseded claim to no value")

			pulled, err := d.Pull(first, `[:t/u]`)
			require.NoError(t, err)
			require.Equal(t, "x", pulled["t/u"], "under the plain rule the claimant's value is its latest assertion, whatever matcher read it before")
		})
	}
}

// An entry a prefetch builds is resolved under the plain rule and carries
// it, so a reader under the unique rule resolves for itself: a superseded
// claimant prefetched reads as superseded.
func TestPrefetchedEntryIsNotServedUnderTheUniqueRule(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			d, first := uniqueTakeover(t, mode)
			require.NoError(t, d.Matcher().(*PatternMatcher).PrefetchEntities([]datalog.Identity{first}))

			pulled, err := d.Pull(first, `[:t/u]`)
			require.NoError(t, err)
			require.Nil(t, pulled["t/u"], "the superseded claim resolves to no value under the unique rule, whatever the prefetch stored")
		})
	}
}
