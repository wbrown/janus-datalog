package storage

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
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

// declaredDefinition is one tuple of the query the proposal's §4 states.
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
		:where [?a :db/ident ?ident]
		       [?a :db/valueType ?type]
		       [?a :db/cardinality ?card]]`))
	byIdent := make(map[datalog.Keyword]declaredDefinition, len(tuples))
	for _, tuple := range tuples {
		byIdent[tuple.Ident] = tuple
	}
	return byIdent
}

// attributeFacts pulls the seven words off the definition entity of ident. A
// word the definition does not carry is absent from the result.
func attributeFacts(t *testing.T, d *Database, ident datalog.Keyword) map[string]interface{} {
	t.Helper()
	facts, err := d.Pull(schemaFactsEntity(ident),
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
	for _, mode := range optimizerModes {
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
	for _, mode := range optimizerModes {
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
			require.NoError(t, d.QueryInto(&entities, `[:find ?a :where [?a :db/ident :t/documented]]`))
			require.Len(t, entities, 1, "a re-asserted definition converges to one entity")
			require.Equal(t, "second", attributeFacts(t, d, kw(":t/documented"))["db/doc"])

			var docs []string
			require.NoError(t, d.History().QueryInto(&docs,
				`[:find ?doc :in $ ?a :where [?a :db/doc ?doc]]`, schemaFactsEntity(kw(":t/documented"))))
			require.ElementsMatch(t, []string{"first", "second"}, docs)
		})
	}
}

func TestSchemaFacts_DefinitionConvergesAcrossReplicas(t *testing.T) {
	for _, mode := range optimizerModes {
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

func TestSchemaFacts_WithdrawnDocResolvesAbsent(t *testing.T) {
	for _, mode := range optimizerModes {
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
					`[:find ?doc :in $ ?a :where [?a :db/doc ?doc]]`, entity))
				return docs
			}
			require.Equal(t, []string{"documented"}, docOf())

			require.NoError(t, d.SetSchema(undocumented))
			require.Empty(t, docOf(), "a withdrawn doc no longer resolves")
			require.Equal(t, "", d.Schema().GetAttribute(kw(":t/documented")).Doc)

			var history []string
			require.NoError(t, d.History().QueryInto(&history,
				`[:find ?doc :in $ ?a :where [?a :db/doc ?doc]]`, entity))
			require.Contains(t, history, "documented", "History keeps the withdrawn doc")
		})
	}
}

func TestSchemaFacts_WithdrawnWordResolvesAbsent(t *testing.T) {
	for _, mode := range optimizerModes {
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
					`[:find ?u :in $ ?a :where [?a :db/unique ?u]]`, entity))
				return constraints
			}
			require.Equal(t, []datalog.Keyword{schema.UniqueValue}, uniqueOf())

			require.NoError(t, d.SetSchema(unconstrained))
			require.Empty(t, uniqueOf(), "a withdrawn uniqueness constraint no longer resolves")
			require.Nil(t, d.Schema().GetAttribute(kw(":t/unique")).Unique)

			var history []datalog.Keyword
			require.NoError(t, d.History().QueryInto(&history,
				`[:find ?u :in $ ?a :where [?a :db/unique ?u]]`, entity))
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

func TestSchemaFacts_MalformedFactRefusesOpen(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			path := ""
			if mode.backend.Persistent {
				path = t.TempDir()
			}
			store, err := mode.backend.Open(path, &BinaryKeyEncoder{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })

			ident := kw(":t/bogus")
			entity := schemaFactsEntity(ident)
			require.NoError(t, store.Assert([]datalog.Datom{
				{E: entity, A: schema.DBIdent, V: ident, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 7}},
				{E: entity, A: schema.DBCardinality, V: kw(":db.cardinality/bogus"), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 7}},
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
	for _, mode := range optimizerModes {
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
	for _, mode := range optimizerModes {
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
			require.False(t, read, "a definition asserted past the snapshot is gone from the facts after the rewind")
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/later")), "the given schema stays in force over the facts that survive")
			require.NotNil(t, d.Schema().GetAttribute(kw(":t/early")))
		})
	}
}

func TestSchemaFacts_ChangedCardinalityInvalidatesCache(t *testing.T) {
	for _, mode := range optimizerModes {
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
