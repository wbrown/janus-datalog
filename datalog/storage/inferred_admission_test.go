package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An attribute no declaration covers runs with the definition the install
// infers from its ops. That definition's value type is a reading of the
// attribute's latest datom, not a declaration, so a written value of another
// type is admitted there, and the inferred cardinality decides the op the
// write takes. A declared value type refuses a value of another type under
// the same install.
func TestAnInferredDefinitionRefusesNoValueAndDecidesTheOp(t *testing.T) {
	one, many, vector := kw(":inferred/one"), kw(":inferred/many"), kw(":inferred/vector")
	declared := kw(":inferred/declared")
	holder := datalog.NewIdentity("inferred:holder")
	writer := datalog.NewIdentity("inferred:writer")
	declaring, err := schema.NewBuilder().
		Attribute(":inferred/declared").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	// String facts the install infers one, many and vector from: one value at
	// one, two values of one entity at many, and a vector at vector.
	facts := []datalog.Datom{
		{E: holder, A: one, V: "a"},
		{E: holder, A: many, V: "a"},
		{E: holder, A: many, V: "b"},
		{E: holder, A: vector, V: []interface{}{"a", "b"}},
	}
	openFacts := func(t *testing.T, opts DatabaseOptions) *Database {
		d, err := OpenFacts(facts, opts)
		require.NoError(t, err)
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Errorf("close the facts database: %v", err)
			}
		})
		return d
	}
	installs := []struct {
		name     string
		open     func(t *testing.T) *Database
		declares bool
	}{
		{"OpenFacts with no schema", func(t *testing.T) *Database {
			return openFacts(t, DatabaseOptions{})
		}, false},
		{"OpenFacts under a given schema", func(t *testing.T) *Database {
			return openFacts(t, DatabaseOptions{Schema: declaring})
		}, true},
		{"SetSchema after OpenFacts", func(t *testing.T) *Database {
			d := openFacts(t, DatabaseOptions{})
			require.NoError(t, d.SetSchema(declaring))
			return d
		}, true},
	}
	inferred := []struct {
		attr        datalog.Keyword
		cardinality datalog.Keyword
		op          datalog.CRDTOp
		set         interface{}
	}{
		{one, schema.CardinalityOne, datalog.OpNone, int64(2)},
		{many, schema.CardinalityMany, datalog.OpCRDTAdd, []int64{1, 2}},
		{vector, schema.CardinalityVector, datalog.OpRGAInsert, []int64{1, 2}},
	}
	for _, install := range installs {
		t.Run(install.name, func(t *testing.T) {
			d := install.open(t)
			for _, c := range inferred {
				definition := d.Schema().GetAttribute(c.attr)
				require.NotNil(t, definition, "the install infers %s from its facts", c.attr)
				require.Same(t, c.cardinality, definition.Cardinality, "%s's inferred cardinality", c.attr)
				require.Same(t, schema.TypeString, definition.ValueType, "%s's inferred value type, read off its string facts", c.attr)
			}

			tx := d.NewTransaction()
			for _, c := range inferred {
				require.NoError(t, tx.Add(writer, c.attr, int64(1)), "an inferred value type refuses no value at %s", c.attr)
				written := tx.datoms[len(tx.datoms)-1]
				require.Equal(t, int64(1), written.V, "the value Add writes at %s", c.attr)
				require.Equal(t, c.op, written.Op, "%s's inferred cardinality decides the op Add writes", c.attr)
				require.NoError(t, tx.Set(holder, c.attr, c.set), "an inferred value type refuses no value Set writes at %s", c.attr)
			}
			if install.declares {
				require.ErrorContains(t, tx.Add(writer, declared, int64(1)), "schema validation failed for :inferred/declared",
					"the declared value type refuses another type through Add")
				require.ErrorContains(t, tx.Set(writer, declared, int64(1)), "schema validation failed for :inferred/declared",
					"the declared value type refuses another type through Set")
			}
			_, err := tx.Commit()
			require.NoError(t, err)
		})
	}

	t.Run("a reopen with no schema", func(t *testing.T) {
		reopenLegs(t, func(t *testing.T, open func(DatabaseOptions) *Database) {
			first := open(DatabaseOptions{Schema: declaring})
			tx := first.NewTransaction()
			require.NoError(t, tx.Add(holder, declared, "a"))
			require.NoError(t, tx.Add(holder, one, "a"))
			_, err := tx.Commit()
			require.NoError(t, err)

			second := open(DatabaseOptions{})
			definition := second.Schema().GetAttribute(one)
			require.NotNil(t, definition, "the reopen infers %s from its datom", one)
			require.Same(t, schema.TypeString, definition.ValueType, "%s's inferred value type, read off its string datom", one)

			tx = second.NewTransaction()
			require.NoError(t, tx.Add(writer, one, int64(1)), "an inferred value type refuses no value at %s", one)
			require.NoError(t, tx.Set(holder, one, int64(2)), "an inferred value type refuses no value Set writes at %s", one)
			require.ErrorContains(t, tx.Add(writer, declared, int64(1)), "schema validation failed for :inferred/declared",
				"the stored declaration's value type refuses another type through Add")
			_, err = tx.Commit()
			require.NoError(t, err)
		})
	})
}
