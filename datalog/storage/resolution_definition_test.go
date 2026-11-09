package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Each resolved group, and each value-bound candidate, resolves under its own
// attribute's definition, whatever attribute the group or candidate before it
// had, with the EA cache on and off.

// thingSchema declares :thing/name cardinality-one, :thing/tags
// cardinality-many and :thing/code cardinality-one with a unique value.
func thingSchema() *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":thing/name"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":thing/tags"), ValueType: schema.TypeString, Cardinality: schema.CardinalityMany})
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":thing/code"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueValue})
	return s
}

// A value-bound match over every attribute validates each candidate under its
// own attribute: a cardinality-one value since superseded does not match, and
// a cardinality-many member does.
func TestAValueBoundMatchOverEveryAttributeResolvesEachCandidateUnderItsAttribute(t *testing.T) {
	name := datalog.NewKeyword(":thing/name")
	tags := datalog.NewKeyword(":thing/tags")
	superseded, member, held := datalog.NewIdentity("thing:superseded"), datalog.NewIdentity("thing:member"), datalog.NewIdentity("thing:held")
	apply := func(db *Database) {
		tx := db.NewTransaction()
		require.NoError(t, tx.Add(superseded, name, "room"))
		require.NoError(t, tx.Add(member, tags, "room"))
		require.NoError(t, tx.Add(member, tags, "hall"))
		require.NoError(t, tx.Add(held, name, "room"))
		_, err := tx.Commit()
		require.NoError(t, err)
		tx2 := db.NewTransaction()
		require.NoError(t, tx2.Add(superseded, name, "cave"))
		_, err = tx2.Commit()
		require.NoError(t, err)
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			assertVBoundEquivalent(t, mode, thingSchema(), apply, expectedSet(member, held), true,
				`[:find ?e :where [?e ?a "room"]]`)
		})
	}
}

// A resolved scan whose groups change attribute, and one whose groups share
// one, each answer every group under its attribute's definition: the latest
// value of a cardinality-one attribute, the members a cardinality-many
// attribute holds, and the value of a unique attribute.
func TestEachResolvedGroupResolvesUnderItsOwnAttributesDefinition(t *testing.T) {
	name := datalog.NewKeyword(":thing/name")
	tags := datalog.NewKeyword(":thing/tags")
	code := datalog.NewKeyword(":thing/code")
	first, second := datalog.NewIdentity("thing:first"), datalog.NewIdentity("thing:second")
	build := func(t *testing.T, db *Database) {
		tx := db.NewTransaction()
		require.NoError(t, tx.Add(first, name, "old"))
		require.NoError(t, tx.Add(first, tags, "kept"))
		require.NoError(t, tx.Add(first, tags, "dropped"))
		require.NoError(t, tx.Add(first, code, "c1"))
		require.NoError(t, tx.Add(second, name, "other"))
		require.NoError(t, tx.Add(second, tags, "only"))
		require.NoError(t, tx.Add(second, code, "c2"))
		_, err := tx.Commit()
		require.NoError(t, err)
		tx2 := db.NewTransaction()
		require.NoError(t, tx2.Add(first, name, "new"))
		require.NoError(t, tx2.Remove(first, tags, "dropped"))
		_, err = tx2.Commit()
		require.NoError(t, err)
	}
	cases := []struct {
		name  string
		query string
		args  []interface{}
		want  [][]interface{}
	}{
		{
			name:  "groups change attribute",
			query: `[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`,
			args:  []interface{}{first},
			want:  [][]interface{}{{name, "new"}, {tags, "kept"}, {code, "c1"}},
		},
		{
			name:  "groups share a cardinality-one attribute",
			query: `[:find ?e ?v :where [?e :thing/name ?v]]`,
			want:  [][]interface{}{{first, "new"}, {second, "other"}},
		},
		{
			name:  "groups share a cardinality-many attribute",
			query: `[:find ?e ?v :where [?e :thing/tags ?v]]`,
			want:  [][]interface{}{{first, "kept"}, {second, "only"}},
		},
		{
			name:  "groups share a unique attribute",
			query: `[:find ?e ?v :where [?e :thing/code ?v]]`,
			want:  [][]interface{}{{first, "c1"}, {second, "c2"}},
		},
	}
	for _, mode := range optimizerModes {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: thingSchema(), DisableCache: cacheMode.disableCache})
				build(t, db)
				for _, c := range cases {
					got, err := executor.CollectTuples(db.Query(c.query, c.args...))
					require.NoError(t, err, c.name)
					require.ElementsMatch(t, c.want, got, c.name)
				}
			})
		}
	}
}
