package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Tests for docs/bugs/BUG_ATTRIBUTE_KEY_TRUNCATION_COLLISION.md (storage side).
//
// Policy: reject. The storage Attribute is a fixed [32]byte; rather than
// truncating (and silently aliasing) longer names, writes and schema
// definitions of over-length attributes fail with a clear error.
// (The interning half is covered by datalog.TestKeywordInterning_LongNamesDoNotCollide.)

// longAttrName is well past the 32-byte storage limit.
func longAttrName() string {
	return ":collision/" + strings.Repeat("x", 30) // 41 bytes
}

// TestStorage_LongAttributeNameRejectedOnWrite: every write entry point rejects
// an over-length attribute instead of truncating it.
func TestStorage_LongAttributeNameRejectedOnWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	e := datalog.NewIdentity("entity-1")
	a := datalog.NewKeyword(longAttrName())
	require.Greater(t, len(a.String()), datalog.MaxAttributeBytes)

	tx := db.NewTransaction()
	require.Error(t, tx.Set(e, a, "v"), "Set must reject over-length attribute")
	require.Error(t, tx.Add(e, a, "v"), "Add must reject over-length attribute")
	require.Error(t, tx.Remove(e, a, "v"), "Remove must reject over-length attribute")
	require.Error(t, tx.Retract(e, a, "v"), "Retract must reject over-length attribute")
}

// TestSchema_LongAttributeDefinitionRejected: schema construction rejects an
// over-length attribute ident.
func TestSchema_LongAttributeDefinitionRejected(t *testing.T) {
	_, err := schema.NewBuilder().
		Attribute(longAttrName()).Type(schema.TypeString).One().Add().
		Build()
	require.Error(t, err, "Build must reject over-length attribute definition")
}

// TestStorage_MaxLengthAttributeNameAccepted: a name exactly at the limit is
// accepted and round-trips (guards against an off-by-one that would reject
// valid names).
func TestStorage_MaxLengthAttributeNameAccepted(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	// ":boundary/" is 10 bytes; pad to exactly MaxAttributeBytes.
	name := ":boundary/" + strings.Repeat("a", datalog.MaxAttributeBytes-len(":boundary/"))
	require.Len(t, name, datalog.MaxAttributeBytes)

	e := datalog.NewIdentity("entity-boundary")
	a := datalog.NewKeyword(name)

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, "value-at-limit"))
	_, err = tx.Commit()
	require.NoError(t, err)

	full, err := db.Pull(e, `[*]`)
	require.NoError(t, err)
	require.Equal(t, "value-at-limit", full[strings.TrimPrefix(name, ":")])
}
