//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAsOfVectorResolution verifies that AsOf correctly filters
// cardinality-vector (RGA) attributes to show only elements that existed
// at the requested point in causal time.
//
// This is a regression test for a bug where loadRGAElements did not
// apply temporal filtering, causing AsOf queries on vector attributes
// to always return the current (latest) content instead of the
// historical content at the requested ElementID.
func TestAsOfVectorResolution(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Schema: a vector attribute (like task/content)
	s, err := schema.NewBuilder().
		Attribute(":doc/content").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	contentAttr := datalog.NewKeyword(":doc/content")
	entity := datalog.NewIdentity("doc1")

	// Transaction 1: Write original content as a vector
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(entity, contentAttr, "Original line one."))
	require.NoError(t, tx1.Add(entity, contentAttr, "Original line two."))
	tx1ID, err := tx1.Commit()
	require.NoError(t, err)
	require.NotEqual(t, datalog.ElementID{}, tx1ID)

	// Verify current content is the original
	matcher1 := db.Matcher().(*BadgerMatcher)
	val1, found := requireAttributeLookup(t, matcher1, entity, contentAttr)
	require.True(t, found)
	require.Equal(t, []string{"Original line one.", "Original line two."}, val1)

	// Transaction 2: Replace the content with Set (tombstones old + inserts new)
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(entity, contentAttr, []interface{}{"Updated content after re-run."}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify current content is updated
	matcher2 := db.Matcher().(*BadgerMatcher)
	val2, found := requireAttributeLookup(t, matcher2, entity, contentAttr)
	require.True(t, found)
	require.Len(t, val2, 1, "current content should have 1 element")
	assert.Equal(t, "Updated content after re-run.", val2.([]string)[0],
		"current content should be the updated version")

	// AsOf tx1: should see the ORIGINAL content, not the update
	asOfMatcher := db.AsOf(tx1ID).Matcher().(*BadgerMatcher)
	asOfVal, found := requireAttributeLookup(t, asOfMatcher, entity, contentAttr)
	require.True(t, found, "entity should have content as-of tx1")
	assert.Equal(t, []string{"Original line one.", "Original line two."}, asOfVal,
		"as-of query should return historical vector content, not current")
}

// TestAsOfVectorResolution_AddOnly verifies AsOf works when elements
// are only appended (no tombstoning). Elements added after the cutoff
// should not appear.
func TestAsOfVectorResolution_AddOnly(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	s, err := schema.NewBuilder().
		Attribute(":log/entries").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	logAttr := datalog.NewKeyword(":log/entries")
	entity := datalog.NewIdentity("log1")

	// Transaction 1: Two entries
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(entity, logAttr, "Entry A"))
	require.NoError(t, tx1.Add(entity, logAttr, "Entry B"))
	tx1ID, err := tx1.Commit()
	require.NoError(t, err)

	// Transaction 2: Append a third entry
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(entity, logAttr, "Entry C"))
	tx2ID, err := tx2.Commit()
	require.NoError(t, err)

	// Transaction 3: Append a fourth entry
	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Add(entity, logAttr, "Entry D"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Current: all four entries
	matcher := db.Matcher().(*BadgerMatcher)
	val, found := requireAttributeLookup(t, matcher, entity, logAttr)
	require.True(t, found)
	assert.Equal(t, []string{"Entry A", "Entry B", "Entry C", "Entry D"}, val)

	// AsOf tx1: only the first two entries
	asOf1 := db.AsOf(tx1ID).Matcher().(*BadgerMatcher)
	val1, found := requireAttributeLookup(t, asOf1, entity, logAttr)
	require.True(t, found)
	assert.Equal(t, []string{"Entry A", "Entry B"}, val1,
		"as-of tx1 should show only entries from tx1")

	// AsOf tx2: first three entries
	asOf2 := db.AsOf(tx2ID).Matcher().(*BadgerMatcher)
	val2, found := requireAttributeLookup(t, asOf2, entity, logAttr)
	require.True(t, found)
	assert.Equal(t, []string{"Entry A", "Entry B", "Entry C"}, val2,
		"as-of tx2 should show entries from tx1 and tx2")
}

// TestAsOfVectorResolution_PullInto verifies that PullInto on an AsOf
// database correctly returns historical vector content.
func TestAsOfVectorResolution_PullInto(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	s, err := schema.NewBuilder().
		Attribute(":doc/content").Type(schema.TypeString).Vector().Add().
		Attribute(":doc/title").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	contentAttr := datalog.NewKeyword(":doc/content")
	titleAttr := datalog.NewKeyword(":doc/title")
	entity := datalog.NewIdentity("doc1")

	// Transaction 1: Original content
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Set(entity, titleAttr, "My Document"))
	require.NoError(t, tx1.Add(entity, contentAttr, "First paragraph."))
	require.NoError(t, tx1.Add(entity, contentAttr, "Second paragraph."))
	tx1ID, err := tx1.Commit()
	require.NoError(t, err)

	// Transaction 2: Replace content
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(entity, contentAttr, []interface{}{"Rewritten paragraph."}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// PullInto on current database
	type Doc struct {
		Title   string   `datalog:"doc/title"`
		Content []string `datalog:"doc/content"`
	}

	var current Doc
	err = db.PullInto(entity, &current)
	require.NoError(t, err)
	assert.Equal(t, "My Document", current.Title)
	assert.Equal(t, []string{"Rewritten paragraph."}, current.Content)

	// PullInto on AsOf database — should show original content
	asOfDB := db.AsOf(tx1ID)
	var historical Doc
	err = asOfDB.PullInto(entity, &historical)
	require.NoError(t, err)
	assert.Equal(t, "My Document", historical.Title)
	assert.Equal(t, []string{"First paragraph.", "Second paragraph."}, historical.Content,
		"PullInto on AsOf database should return historical vector content")
}
