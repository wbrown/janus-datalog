package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// writerName is a struct SaveStruct writes through a :w/name tag.
type writerName struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"w/name"`
}

// writerTags is a struct SaveStruct writes through a :w/tags tag.
type writerTags struct {
	ID   datalog.Identity `datalog:"-,id"`
	Tags []string         `datalog:"w/tags"`
}

// A transaction's writes are admitted and written under the schema in force
// when the transaction opened, and an install while it is open does not reach
// them. Each case opens a database under the schema it names opened, commits
// the facts it reads, opens a transaction, installs the schema it names
// installed with SetSchema, and writes. It reads the write's error and the
// datoms the transaction holds before its commit.
func TestATransactionWritesUnderTheSchemaInForceWhenItOpened(t *testing.T) {
	writer := datalog.NewIdentity("writer")
	name, tags, path := kw(":w/name"), kw(":w/tags"), kw(":w/path")
	cases := []struct {
		name      string
		opened    *schema.Schema
		installed *schema.Schema
		committed func(tx *Transaction) error
		write     func(tx *Transaction) error
		check     func(t *testing.T, err error, written []datalog.Datom)
	}{
		{
			name:      "Add admits a value the opened schema admits and the installed one refuses",
			opened:    schema.NewBuilder().Attribute(":w/name").Type(schema.TypeString).One().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/name").Type(schema.TypeLong).One().Add().MustBuild(),
			write:     func(tx *Transaction) error { return tx.Add(writer, name, "x") },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err, "the opened schema admits a string at :w/name")
				require.Len(t, written, 1)
				require.Equal(t, "x", written[0].V)
				require.Equal(t, datalog.OpNone, written[0].Op, "the opened schema declares :w/name cardinality one")
			},
		},
		{
			name:      "Add refuses a value the opened schema refuses and the installed one admits",
			opened:    schema.NewBuilder().Attribute(":w/name").Type(schema.TypeLong).One().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/name").Type(schema.TypeString).One().Add().MustBuild(),
			write:     func(tx *Transaction) error { return tx.Add(writer, name, "x") },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.ErrorContains(t, err, "schema validation failed for :w/name", "the opened schema declares :w/name a long")
				require.Empty(t, written)
			},
		},
		{
			name:      "Add writes the op of the opened schema's cardinality",
			opened:    schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeString).Many().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeString).One().Add().MustBuild(),
			write:     func(tx *Transaction) error { return tx.Add(writer, tags, "a") },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err)
				require.Len(t, written, 1)
				require.Equal(t, datalog.OpCRDTAdd, written[0].Op, "the opened schema declares :w/tags cardinality many")
			},
		},
		{
			name:      "Add writes an ordered set's repeated element once",
			opened:    schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).OrderedSet().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).Vector().Add().MustBuild(),
			write: func(tx *Transaction) error {
				if err := tx.Add(writer, path, "a"); err != nil {
					return err
				}
				return tx.Add(writer, path, "a")
			},
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err)
				require.Len(t, written, 1, "the opened schema declares :w/path an ordered set, which holds a once")
				require.Equal(t, datalog.OpRGAInsert, written[0].Op)
			},
		},
		{
			name:      "Add finds an element the ordered set holds committed",
			opened:    schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).OrderedSet().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).Many().Add().MustBuild(),
			committed: func(tx *Transaction) error { return tx.Add(writer, path, "a") },
			write:     func(tx *Transaction) error { return tx.Add(writer, path, "a") },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err)
				require.Empty(t, written, "the ordered set the opened schema declares holds a committed, so the write adds no element")
			},
		},
		{
			name:      "Set admits each element the opened schema admits at a many attribute",
			opened:    schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeString).Many().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeLong).Many().Add().MustBuild(),
			write:     func(tx *Transaction) error { return tx.Set(writer, tags, []string{"a"}) },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err, "the opened schema admits strings at :w/tags")
				require.Len(t, written, 1)
				require.Equal(t, "a", written[0].V)
				require.Equal(t, datalog.OpCRDTAdd, written[0].Op)
			},
		},
		{
			name:      "Set takes the arm of the opened schema's cardinality",
			opened:    schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeString).Many().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeString).One().Add().MustBuild(),
			write:     func(tx *Transaction) error { return tx.Set(writer, tags, []string{"a", "b"}) },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err, "the opened schema declares :w/tags cardinality many, which takes a set")
				require.Len(t, written, 2)
				for _, datom := range written {
					require.Equal(t, datalog.OpCRDTAdd, datom.Op)
				}
			},
		},
		{
			name:      "Set admits a value the opened schema admits at a cardinality-one attribute",
			opened:    schema.NewBuilder().Attribute(":w/name").Type(schema.TypeString).One().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/name").Type(schema.TypeLong).One().Add().MustBuild(),
			write:     func(tx *Transaction) error { return tx.Set(writer, name, "x") },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err, "the opened schema admits a string at :w/name")
				require.Len(t, written, 1)
				require.Equal(t, "x", written[0].V)
			},
		},
		{
			name:      "Set admits each element the opened schema admits at a vector attribute",
			opened:    schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).Vector().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/path").Type(schema.TypeLong).Vector().Add().MustBuild(),
			write:     func(tx *Transaction) error { return tx.Set(writer, path, []string{"x", "y"}) },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err, "the opened schema admits strings at :w/path")
				require.Len(t, written, 2)
				for _, datom := range written {
					require.Equal(t, datalog.OpRGAInsert, datom.Op)
				}
			},
		},
		{
			name:      "Set reads the committed vector under the opened schema's cardinality",
			opened:    schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).Vector().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).Many().Add().MustBuild(),
			committed: func(tx *Transaction) error { return tx.Set(writer, path, []string{"x"}) },
			write:     func(tx *Transaction) error { return tx.Set(writer, path, []string{"x", "y"}) },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err)
				require.Len(t, written, 1, "the committed vector holds x, so only the element past the common prefix is written")
				require.Equal(t, "y", written[0].V)
				require.Equal(t, datalog.OpRGAInsert, written[0].Op)
			},
		},
		{
			name:      "Remove writes the withdrawal of the opened schema's cardinality",
			opened:    schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).Vector().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/path").Type(schema.TypeString).One().Add().MustBuild(),
			committed: func(tx *Transaction) error { return tx.Add(writer, path, "a") },
			write:     func(tx *Transaction) error { return tx.Remove(writer, path, "a") },
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err)
				require.Len(t, written, 1)
				require.Equal(t, datalog.OpRGATombstone, written[0].Op, "the opened schema declares :w/path a vector")
			},
		},
		{
			name:      "SaveStruct writes a field the opened schema admits",
			opened:    schema.NewBuilder().Attribute(":w/name").Type(schema.TypeString).One().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/name").Type(schema.TypeLong).One().Add().MustBuild(),
			write: func(tx *Transaction) error {
				_, err := tx.SaveStruct(&writerName{ID: writer, Name: "n"})
				return err
			},
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err, "the opened schema admits a string at :w/name")
				require.Len(t, written, 1)
				require.Equal(t, "n", written[0].V)
			},
		},
		{
			name:      "SaveStruct diffs a many field against the committed members under the opened cardinality",
			opened:    schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeString).Many().Add().MustBuild(),
			installed: schema.NewBuilder().Attribute(":w/tags").Type(schema.TypeString).One().Add().MustBuild(),
			committed: func(tx *Transaction) error { return tx.Set(writer, tags, []string{"a", "b"}) },
			write: func(tx *Transaction) error {
				_, err := tx.SaveStruct(&writerTags{ID: writer, Tags: []string{"a", "c"}})
				return err
			},
			check: func(t *testing.T, err error, written []datalog.Datom) {
				require.NoError(t, err, "the opened schema declares :w/tags cardinality many, which takes a set")
				require.Len(t, written, 2, "the committed set holds a and b, so b is withdrawn and c added")
				for _, datom := range written {
					switch datom.V {
					case "b":
						require.Equal(t, datalog.OpCRDTRemove, datom.Op, "b is withdrawn from the set")
					case "c":
						require.Equal(t, datalog.OpCRDTAdd, datom.Op, "c is added to the set")
					default:
						t.Fatalf("the write holds %v, which is neither b nor c", datom.V)
					}
				}
			},
		},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: c.opened})
				if c.committed != nil {
					committing := d.NewTransaction()
					require.NoError(t, c.committed(committing))
					_, err := committing.Commit()
					require.NoError(t, err)
				}
				tx := d.NewTransaction()
				require.NoError(t, d.SetSchema(c.installed))
				err := c.write(tx)
				var written []datalog.Datom
				for _, datom := range tx.datoms {
					if datom.E == writer {
						written = append(written, datom)
					}
				}
				c.check(t, err, written)
				require.NoError(t, tx.Rollback())
			})
		}
	}
}
