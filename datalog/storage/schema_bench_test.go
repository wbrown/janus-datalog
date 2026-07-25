package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// BenchmarkAddWithoutSchema measures Add() performance without schema validation
func BenchmarkAddWithoutSchema(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-no-schema")
	defer os.RemoveAll(tmpDir)

	db, _ := NewDatabase(tmpDir)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.NewTransaction()
		e := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx.Add(e, name, "Alice")
		tx.Commit()
	}
}

// BenchmarkAddWithSchema measures Add() performance with type validation
func BenchmarkAddWithSchema(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-with-schema")
	defer os.RemoveAll(tmpDir)

	s, _ := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Build()

	db, _ := NewDatabaseWithSchema(tmpDir, s)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.NewTransaction()
		e := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx.Add(e, name, "Alice")
		tx.Commit()
	}
}

// BenchmarkAddWithSchemaUnique measures Add()+Commit() with uniqueness validation
func BenchmarkAddWithSchemaUnique(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-unique")
	defer os.RemoveAll(tmpDir)

	s, _ := schema.NewBuilder().
		Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()

	db, _ := NewDatabaseWithSchema(tmpDir, s)
	defer db.Close()

	email := datalog.NewKeyword(":person/email")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.NewTransaction()
		e := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx.Add(e, email, fmt.Sprintf("user%d@example.com", i))
		tx.Commit()
	}
}

// BenchmarkBulkAddWithoutSchema measures bulk inserts without schema
func BenchmarkBulkAddWithoutSchema(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-bulk-no-schema")
	defer os.RemoveAll(tmpDir)

	db, _ := NewDatabase(tmpDir)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.NewTransaction()
		for j := 0; j < 100; j++ {
			e := datalog.NewIdentity(fmt.Sprintf("entity-%d-%d", i, j))
			tx.Add(e, name, "Alice")
			tx.Add(e, age, int64(30))
		}
		tx.Commit()
	}
}

// BenchmarkBulkAddWithSchema measures bulk inserts with type validation
func BenchmarkBulkAddWithSchema(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-bulk-with-schema")
	defer os.RemoveAll(tmpDir)

	s, _ := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Build()

	db, _ := NewDatabaseWithSchema(tmpDir, s)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.NewTransaction()
		for j := 0; j < 100; j++ {
			e := datalog.NewIdentity(fmt.Sprintf("entity-%d-%d", i, j))
			tx.Add(e, name, "Alice")
			tx.Add(e, age, int64(30))
		}
		tx.Commit()
	}
}

// BenchmarkBulkAddWithSchemaUnique measures bulk inserts with uniqueness checking
func BenchmarkBulkAddWithSchemaUnique(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-bulk-unique")
	defer os.RemoveAll(tmpDir)

	s, _ := schema.NewBuilder().
		Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()

	db, _ := NewDatabaseWithSchema(tmpDir, s)
	defer db.Close()

	email := datalog.NewKeyword(":person/email")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.NewTransaction()
		for j := 0; j < 100; j++ {
			e := datalog.NewIdentity(fmt.Sprintf("entity-%d-%d", i, j))
			tx.Add(e, email, fmt.Sprintf("user%d-%d@example.com", i, j))
		}
		tx.Commit()
	}
}

// BenchmarkPullCardinalityOne measures Pull for cardinality-one attributes
func BenchmarkPullCardinalityOne(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-pull-one")
	defer os.RemoveAll(tmpDir)

	s, _ := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Build()

	db, _ := NewDatabaseWithSchema(tmpDir, s)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	tx.Add(alice, name, "Alice Smith")
	tx.Commit()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: name},
		},
	}
	resolved := schema.ResolvePullPattern(pattern, s)

	matcher := NewPatternMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		puller.PullResolved(alice, resolved)
	}
}

// BenchmarkPullCardinalityMany measures Pull for cardinality-many attributes
func BenchmarkPullCardinalityMany(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-pull-many")
	defer os.RemoveAll(tmpDir)

	s, _ := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()

	db, _ := NewDatabaseWithSchema(tmpDir, s)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	tags := datalog.NewKeyword(":person/tags")

	tx := db.NewTransaction()
	for i := 0; i < 10; i++ {
		tx.Add(alice, tags, fmt.Sprintf("tag-%d", i))
	}
	tx.Commit()

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: tags},
		},
	}
	resolved := schema.ResolvePullPattern(pattern, s)

	matcher := NewPatternMatcher(db.Store())
	puller := executor.NewPullExecutor(matcher, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		puller.PullResolved(alice, resolved)
	}
}

// BenchmarkSchemaResolution measures the cost of resolving a pull pattern
func BenchmarkSchemaResolution(b *testing.B) {
	s, _ := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Attribute(":person/email").Type(schema.TypeString).Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Attribute(":person/friends").Type(schema.TypeRef).Many().Add().
		Build()

	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")
	email := datalog.NewKeyword(":person/email")
	tags := datalog.NewKeyword(":person/tags")
	friends := datalog.NewKeyword(":person/friends")

	pattern := &query.PullPattern{
		Specs: []query.PullAttrSpec{
			&query.PullAttribute{Attr: name},
			&query.PullAttribute{Attr: age},
			&query.PullAttribute{Attr: email},
			&query.PullAttribute{Attr: tags},
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema.ResolvePullPattern(pattern, s)
	}
}
