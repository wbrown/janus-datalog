//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// setupBenchmarkDB creates a temporary database with test data
func setupBenchmarkDB(b *testing.B, numEntities, attrsPerEntity int) (*Database, []datalog.Identity) {
	b.Helper()

	tmpDir, err := os.MkdirTemp("", "pull-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	db, err := NewDatabase(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("failed to create database: %v", err)
	}

	// Create entities and add data
	var entities []datalog.Identity
	tx := db.NewTransaction()

	for i := 0; i < numEntities; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		entities = append(entities, entity)

		for j := 0; j < attrsPerEntity; j++ {
			attr := datalog.NewKeyword(fmt.Sprintf(":entity/attr%d", j))
			tx.Add(entity, attr, fmt.Sprintf("value_%d_%d", i, j))
		}
	}

	if _, err := tx.Commit(); err != nil {
		db.Close()
		os.RemoveAll(tmpDir)
		b.Fatalf("failed to commit: %v", err)
	}

	b.Cleanup(func() {
		db.Close()
		os.RemoveAll(tmpDir)
	})

	return db, entities
}

// BenchmarkPullBadger_SingleAttribute measures single attribute lookup with BadgerDB
func BenchmarkPullBadger_SingleAttribute(b *testing.B) {
	db, entities := setupBenchmarkDB(b, 100, 5)
	entity := entities[0]

	pattern, _ := parser.ParsePullPattern(`[:entity/attr0]`)
	matcher := db.Matcher()
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(entity, pattern)
	}
}

// BenchmarkPullBadger_MultipleAttributes measures multiple attribute lookup with BadgerDB
func BenchmarkPullBadger_MultipleAttributes(b *testing.B) {
	db, entities := setupBenchmarkDB(b, 100, 5)
	entity := entities[0]

	pattern, _ := parser.ParsePullPattern(`[:entity/attr0 :entity/attr1 :entity/attr2 :entity/attr3 :entity/attr4]`)
	matcher := db.Matcher()
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(entity, pattern)
	}
}

// BenchmarkPullBadger_Wildcard measures wildcard pull with BadgerDB
func BenchmarkPullBadger_Wildcard(b *testing.B) {
	db, entities := setupBenchmarkDB(b, 100, 5)
	entity := entities[0]

	pattern, _ := parser.ParsePullPattern(`[*]`)
	matcher := db.Matcher()
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(entity, pattern)
	}
}

// BenchmarkPullBadger_NestedReference measures nested reference with BadgerDB
func BenchmarkPullBadger_NestedReference(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "pull-nested-bench-*")
	db, _ := NewDatabase(tmpDir)

	// Create entities with references
	parent := datalog.NewIdentity("entity:parent")
	child := datalog.NewIdentity("entity:child")

	tx := db.NewTransaction()
	tx.Add(parent, datalog.NewKeyword(":entity/name"), "Parent")
	tx.Add(parent, datalog.NewKeyword(":entity/child"), child)
	tx.Add(child, datalog.NewKeyword(":entity/name"), "Child")
	tx.Add(child, datalog.NewKeyword(":entity/value"), "ChildValue")
	tx.Commit()

	b.Cleanup(func() {
		db.Close()
		os.RemoveAll(tmpDir)
	})

	pattern, _ := parser.ParsePullPattern(`[:entity/name {:entity/child [:entity/name :entity/value]}]`)
	matcher := db.Matcher()
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(parent, pattern)
	}
}

// BenchmarkPullBadger_StandaloneAPI measures the high-level db.Pull() API
func BenchmarkPullBadger_StandaloneAPI(b *testing.B) {
	db, entities := setupBenchmarkDB(b, 100, 5)
	entity := entities[0]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Pull(entity, `[:entity/attr0 :entity/attr1 :entity/attr2]`)
	}
}

// BenchmarkPullBadger_StandaloneAPIWithParseCached measures Pull with pre-parsed pattern
func BenchmarkPullBadger_StandaloneAPICached(b *testing.B) {
	db, entities := setupBenchmarkDB(b, 100, 5)
	entity := entities[0]

	// Pre-parse to measure just execution
	pattern, _ := parser.ParsePullPattern(`[:entity/attr0 :entity/attr1 :entity/attr2]`)
	matcher := db.Matcher()
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(entity, pattern)
	}
}

// BenchmarkPullManyBadger measures batch pull with BadgerDB
func BenchmarkPullManyBadger(b *testing.B) {
	db, entities := setupBenchmarkDB(b, 100, 5)

	pattern, _ := parser.ParsePullPattern(`[:entity/attr0 :entity/attr1]`)
	matcher := db.Matcher()
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.PullMany(entities, pattern)
	}
}

// BenchmarkPullBadger_ScalingWithEntities measures how Pull scales with entity count
func BenchmarkPullBadger_ScalingWithEntities(b *testing.B) {
	for _, numEntities := range []int{10, 100, 1000} {
		db, entities := setupBenchmarkDB(b, numEntities, 5)

		pattern, _ := parser.ParsePullPattern(`[:entity/attr0 :entity/attr1]`)
		matcher := db.Matcher()
		puller := executor.NewPullExecutor(matcher, db)

		b.Run(fmt.Sprintf("Entities_%d", numEntities), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = puller.PullMany(entities, pattern)
			}
		})
	}
}

// BenchmarkPullBadger_ScalingWithAttributes measures how Pull scales with attribute count
func BenchmarkPullBadger_ScalingWithAttributes(b *testing.B) {
	for _, numAttrs := range []int{1, 5, 10, 20} {
		db, entities := setupBenchmarkDB(b, 10, numAttrs)
		entity := entities[0]

		// Build pattern string
		var patternStr string
		for i := 0; i < numAttrs; i++ {
			if i > 0 {
				patternStr += " "
			}
			patternStr += fmt.Sprintf(":entity/attr%d", i)
		}

		pattern, _ := parser.ParsePullPattern(fmt.Sprintf("[%s]", patternStr))
		matcher := db.Matcher()
		puller := executor.NewPullExecutor(matcher, db)

		b.Run(fmt.Sprintf("Attrs_%d", numAttrs), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = puller.Pull(entity, pattern)
			}
		})
	}
}

// BenchmarkPullBadger_VsQuery compares Pull vs equivalent query
func BenchmarkPullBadger_VsQuery(b *testing.B) {
	db, entities := setupBenchmarkDB(b, 100, 5)
	entity := entities[0]

	b.Run("Pull", func(b *testing.B) {
		pattern, _ := parser.ParsePullPattern(`[:entity/attr0 :entity/attr1 :entity/attr2]`)
		matcher := db.Matcher()
		puller := executor.NewPullExecutor(matcher, db)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = puller.Pull(entity, pattern)
		}
	})

	b.Run("Query", func(b *testing.B) {
		// Equivalent query that fetches same data
		queryStr := `[:find ?a0 ?a1 ?a2
			:in $ ?e
			:where [?e :entity/attr0 ?a0]
			       [?e :entity/attr1 ?a1]
			       [?e :entity/attr2 ?a2]]`

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = executor.CollectTuples(db.Query(queryStr, entity))
		}
	})
}
