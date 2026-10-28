package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// spacedField carries attribute text that is no keyword in its struct tag.
type spacedField struct {
	Name string `datalog:"t/first name"`
}

// Attribute text that is no keyword, given to an entry that answers an error,
// is refused with an error and panics nowhere: the struct writer's Write with
// a tag holding a space, SchemaFromStruct with that tag, and AddMap with an
// empty key or a key holding a tab.
func TestAnAttributeTextThatIsNoKeywordIsRefusedAtTheWriterTheSchemaAndAnEmptyKey(t *testing.T) {
	answer := func(run func() error) (err error, panicked interface{}) {
		defer func() { panicked = recover() }()
		return run(), nil
	}
	cases := []struct {
		name string
		run  func(t *testing.T, d *db.DB) error
	}{
		{"StructWriter.Write, a tag holding a space", func(t *testing.T, d *db.DB) error {
			w, err := dlreflect.NewStructWriter(&spacedField{}, d.Schema())
			require.NoError(t, err)
			return w.Write(d.NewTransaction(), datalog.NewIdentity("spaced-write"), &spacedField{Name: "alice"})
		}},
		{"SchemaFromStruct, a tag holding a space", func(*testing.T, *db.DB) error {
			_, err := dlreflect.SchemaFromStruct(spacedField{})
			return err
		}},
		{"AddMap, an empty key", func(_ *testing.T, d *db.DB) error {
			_, err := d.NewTransaction().AddMap(map[string]interface{}{"": "alice"})
			return err
		}},
		{"AddMap, a key holding a tab", func(_ *testing.T, d *db.DB) error {
			_, err := d.NewTransaction().AddMap(map[string]interface{}{"t/first\tname": "alice"})
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := db.OpenMemory()
			require.NoError(t, err)
			defer d.Close()
			err, panicked := answer(func() error { return c.run(t, d) })
			require.Nil(t, panicked, "the entry panics: %v", panicked)
			require.Error(t, err, "%s accepts attribute text that is no keyword", c.name)
		})
	}
}

// Keyword text given to an entry makes the keyword NewKeyword makes from the
// same text: a map key AddMap writes is read back under the keyword a query
// names, and an ident the builder declares is the keyword the schema answers.
func TestKeywordTextAtAnEntryMakesTheKeywordNewKeywordMakes(t *testing.T) {
	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()

	tx := d.NewTransaction()
	e, err := tx.AddMap(map[string]interface{}{"t/name": "alice"})
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(d.Query(`[:find ?e :where [?e :t/name "alice"]]`))
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{e}}, tuples)

	s, err := schema.NewBuilder().Attribute("t/name").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	require.NotNil(t, s.GetAttribute(datalog.NewKeyword(":t/name")), "the builder declares the keyword NewKeyword makes from the same text")
}
