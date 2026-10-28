package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// spacedTag carries attribute text that is no keyword in its struct tag.
type spacedTag struct {
	Name string `datalog:"t/first name"`
}

// Attribute text that is no keyword, given to an entry that answers an error,
// is refused with an error, as Query refuses it in get-else: a string key of
// AddMap, a struct tag SaveStruct reads, and the ident the schema builder
// records for Build.
func TestAnAttributeTextThatIsNoKeywordIsRefusedWithAnErrorAtEveryEntryThatAnswersOne(t *testing.T) {
	answer := func(run func() error) (err error, panicked interface{}) {
		defer func() { panicked = recover() }()
		return run(), nil
	}
	cases := []struct {
		name string
		run  func(d *db.DB) error
	}{
		{"AddMap, a key holding a space", func(d *db.DB) error {
			_, err := d.NewTransaction().AddMap(map[string]interface{}{"t/first name": "alice"})
			return err
		}},
		{"AddMap, a key with no name after the colon", func(d *db.DB) error {
			_, err := d.NewTransaction().AddMap(map[string]interface{}{":": "alice"})
			return err
		}},
		{"SaveStruct, a tag holding a space", func(d *db.DB) error {
			_, err := d.NewTransaction().SaveStruct(&spacedTag{Name: "alice"})
			return err
		}},
		{"the schema builder, an ident holding a space", func(*db.DB) error {
			_, err := schema.NewBuilder().Attribute("t/first name").Type(schema.TypeString).Add().Build()
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := db.OpenMemory()
			require.NoError(t, err)
			defer d.Close()
			err, panicked := answer(func() error { return c.run(d) })
			require.Nil(t, panicked, "the entry panics: %v", panicked)
			require.Error(t, err, fmt.Sprintf("%s accepts attribute text that is no keyword", c.name))
		})
	}
}
