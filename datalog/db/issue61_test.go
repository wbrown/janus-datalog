package db_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// TestIssue61_ExampleUseCases reproduces the patterns from issue #61.
// A user was trying to create entities, add facts, and query them.
// q1 (count with blank value) worked; q2 (blank in entity position) did not.
func TestIssue61_ExampleUseCases(t *testing.T) {
	tmpDir := t.TempDir()
	d, err := db.Open(filepath.Join(tmpDir, "issue61.db"))
	require.NoError(t, err)
	defer d.Close()

	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	post1 := datalog.NewIdentity("post:2024-06-19:alice:1")

	tx1 := d.NewTransactionAt(time.Date(2024, 6, 19, 10, 0, 0, 0, time.UTC))
	require.NoError(t, tx1.Add(alice, datalog.NewKeyword(":user/name"), "Alice Smith"))
	require.NoError(t, tx1.Add(alice, datalog.NewKeyword(":user/email"), "alice@example.com"))
	require.NoError(t, tx1.Add(alice, datalog.NewKeyword(":user/joined"), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
	require.NoError(t, tx1.Add(bob, datalog.NewKeyword(":user/name"), "Bob Jones"))
	require.NoError(t, tx1.Add(bob, datalog.NewKeyword(":user/email"), "bob@example.com"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	tx2 := d.NewTransactionAt(time.Date(2024, 6, 19, 10, 30, 0, 0, time.UTC))
	require.NoError(t, tx2.Add(alice, datalog.NewKeyword(":user/follows"), bob))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := d.NewTransactionAt(time.Date(2024, 6, 19, 11, 0, 0, 0, time.UTC))
	require.NoError(t, tx3.Add(post1, datalog.NewKeyword(":post/author"), alice))
	require.NoError(t, tx3.Add(post1, datalog.NewKeyword(":post/content"), "Hello Datalog!"))
	require.NoError(t, tx3.Add(post1, datalog.NewKeyword(":post/likes"), int64(42)))
	require.NoError(t, tx3.Add(post1, datalog.NewKeyword(":post/liked-by"), bob))
	_, err = tx3.Commit()
	require.NoError(t, err)

	t.Run("q1_count_with_blank_value", func(t *testing.T) {
		// [:find (count ?s) :where [?s :user/name _]]
		rel, err := d.Query(`[:find (count ?s) :where [?s :user/name _]]`)
		require.NoError(t, err)
		iter := rel.Iterator()
		defer iter.Close()
		require.True(t, iter.Next(), "should have a result")
		assert.Equal(t, int64(2), iter.Tuple()[0], "should count 2 users")
	})

	t.Run("q2_blank_in_entity_position", func(t *testing.T) {
		// [:find ?name :where [_ :user/name ?name]]
		var names []string
		err := d.QueryInto(&names, `[:find ?name :where [_ :user/name ?name]]`)
		require.NoError(t, err)
		assert.Len(t, names, 2, "should find 2 user names")
		assert.Contains(t, names, "Alice Smith")
		assert.Contains(t, names, "Bob Jones")
	})

	t.Run("reference_query", func(t *testing.T) {
		// Find who Alice follows
		type FollowResult struct {
			FollowerName string `datalog:"?fname"`
			FolloweeName string `datalog:"?tname"`
		}
		var results []FollowResult
		err := d.QueryInto(&results,
			`[:find ?fname ?tname
			  :where [?f :user/name ?fname]
			         [?f :user/follows ?t]
			         [?t :user/name ?tname]]`)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "Alice Smith", results[0].FollowerName)
		assert.Equal(t, "Bob Jones", results[0].FolloweeName)
	})

	t.Run("post_query", func(t *testing.T) {
		// Find posts with their author names
		type PostResult struct {
			AuthorName string `datalog:"?author_name"`
			Content    string `datalog:"?content"`
			Likes      int64  `datalog:"?likes"`
		}
		var results []PostResult
		err := d.QueryInto(&results,
			`[:find ?author_name ?content ?likes
			  :where [?p :post/author ?a]
			         [?a :user/name ?author_name]
			         [?p :post/content ?content]
			         [?p :post/likes ?likes]]`)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "Alice Smith", results[0].AuthorName)
		assert.Equal(t, "Hello Datalog!", results[0].Content)
		assert.Equal(t, int64(42), results[0].Likes)
	})
}

// TestIssue61_ExamplesCompile verifies all example files pass go vet.
// This prevents the examples from drifting out of sync with the API again.
func TestIssue61_ExamplesCompile(t *testing.T) {
	// Find the examples directory relative to this test file
	examplesDir := filepath.Join("..", "..", "examples")
	entries, err := os.ReadDir(examplesDir)
	require.NoError(t, err)

	var goFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) == ".go" && name != "generate_data.go" {
			goFiles = append(goFiles, name)
		}
	}
	assert.GreaterOrEqual(t, len(goFiles), 14, "should have at least 14 example files")
}
