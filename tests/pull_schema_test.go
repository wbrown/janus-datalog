package tests

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func TestPullResolvedCardinalityMany(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-schema-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with cardinality-many attribute
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	db, err := storage.NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")
	tags := datalog.NewKeyword(":person/tags")

	// Add person with multiple tags
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, tags, "developer"))
	require.NoError(t, tx.Add(alice, tags, "team-lead"))
	require.NoError(t, tx.Add(alice, tags, "mentor"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Create pull pattern
	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: name},
			&query.PullAttribute{Attr: tags},
		},
	}

	// Resolve pattern with schema
	resolved := schema.ResolvePullPattern(pattern, s)

	// Execute pull with resolved pattern
	matcher := storage.NewBadgerMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher)
	result, err := puller.PullResolved(alice, resolved)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Name should be single value
	assert.Equal(t, "Alice", result["person/name"])

	// Tags should be array (cardinality-many)
	tagsVal, ok := result["person/tags"].([]interface{})
	require.True(t, ok, "tags should be []interface{}")
	assert.Len(t, tagsVal, 3)

	// Check all tags are present (order may vary)
	tagStrings := make([]string, 0, len(tagsVal))
	for _, v := range tagsVal {
		tagStrings = append(tagStrings, v.(string))
	}
	assert.Contains(t, tagStrings, "developer")
	assert.Contains(t, tagStrings, "team-lead")
	assert.Contains(t, tagStrings, "mentor")
}

func TestPullResolvedCardinalityManyRefs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-refs-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with cardinality-many refs
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/friends").Type(schema.TypeRef).Many().Add().
		Build()
	require.NoError(t, err)

	db, err := storage.NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	carol := datalog.NewIdentity("carol")
	name := datalog.NewKeyword(":person/name")
	friends := datalog.NewKeyword(":person/friends")

	// Add people and friendships
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(bob, name, "Bob"))
	require.NoError(t, tx.Add(carol, name, "Carol"))
	require.NoError(t, tx.Add(alice, friends, bob))
	require.NoError(t, tx.Add(alice, friends, carol))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Create pull pattern with nested ref
	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: name},
			&query.PullMapSpec{
				Attr: friends,
				Pattern: &query.PullPattern{
					Specs: []query.PullAttrSpec{
						&query.PullAttribute{Attr: name},
					},
				},
			},
		},
	}

	// Resolve pattern with schema
	resolved := schema.ResolvePullPattern(pattern, s)

	// Execute pull with resolved pattern
	matcher := storage.NewBadgerMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher)
	result, err := puller.PullResolved(alice, resolved)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Name should be single value
	assert.Equal(t, "Alice", result["person/name"])

	// Friends should be array of nested objects (cardinality-many refs)
	friendsVal, ok := result["person/friends"].([]interface{})
	require.True(t, ok, "friends should be []interface{}")
	assert.Len(t, friendsVal, 2)

	// Check friend names
	friendNames := make([]string, 0)
	for _, f := range friendsVal {
		friendMap := f.(map[string]interface{})
		if name, ok := friendMap["person/name"].(string); ok {
			friendNames = append(friendNames, name)
		}
	}
	assert.Contains(t, friendNames, "Bob")
	assert.Contains(t, friendNames, "Carol")
}

func TestPullResolvedLimit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-limit-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with cardinality-many attribute
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	db, err := storage.NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	tags := datalog.NewKeyword(":person/tags")

	// Add person with many tags
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, tags, "tag1"))
	require.NoError(t, tx.Add(alice, tags, "tag2"))
	require.NoError(t, tx.Add(alice, tags, "tag3"))
	require.NoError(t, tx.Add(alice, tags, "tag4"))
	require.NoError(t, tx.Add(alice, tags, "tag5"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Create pull pattern with limit
	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullLimitExpr{Attr: tags, Limit: 2},
		},
	}

	// Resolve pattern with schema
	resolved := schema.ResolvePullPattern(pattern, s)

	// Execute pull with resolved pattern
	matcher := storage.NewBadgerMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher)
	result, err := puller.PullResolved(alice, resolved)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Tags should be limited to 2
	tagsVal, ok := result["person/tags"].([]interface{})
	require.True(t, ok, "tags should be []interface{}")
	assert.Len(t, tagsVal, 2, "should be limited to 2 tags")
}

func TestPullResolvedDefault(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-default-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/status").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	db, err := storage.NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")
	status := datalog.NewKeyword(":person/status")

	// Add person without status
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Create pull pattern with default
	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: name},
			&query.PullDefaultExpr{Attr: status, Default: "unknown"},
		},
	}

	// Resolve pattern with schema
	resolved := schema.ResolvePullPattern(pattern, s)

	// Execute pull with resolved pattern
	matcher := storage.NewBadgerMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher)
	result, err := puller.PullResolved(alice, resolved)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Name should be present
	assert.Equal(t, "Alice", result["person/name"])

	// Status should have default value
	assert.Equal(t, "unknown", result["person/status"])
}

func TestPullResolvedWithoutSchema(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pull-no-schema-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create database without schema
	db, err := storage.NewDatabase(tmpDir)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")
	tags := datalog.NewKeyword(":person/tags")

	// Add person with multiple tags (without schema, each is just another datom)
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, tags, "developer"))
	require.NoError(t, tx.Add(alice, tags, "team-lead"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Create pull pattern
	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: name},
			&query.PullAttribute{Attr: tags},
		},
	}

	// Resolve pattern without schema - all attributes default to cardinality-one
	resolved := schema.ResolvePullPattern(pattern, nil)

	// Execute pull with resolved pattern
	matcher := storage.NewBadgerMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher)
	result, err := puller.PullResolved(alice, resolved)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Both should be single values (default cardinality-one)
	assert.Equal(t, "Alice", result["person/name"])

	// Tags will only return one value (whichever is returned first)
	// This is the expected behavior without schema
	_, ok := result["person/tags"]
	assert.True(t, ok, "tags should have some value")
}
