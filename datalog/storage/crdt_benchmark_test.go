package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// BenchmarkCRDTWrite measures write performance for different cardinalities
func BenchmarkCRDTWrite(b *testing.B) {
	b.Run("CardinalityOne", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/name")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := db.NewTransaction()
			tx.Add(entity, attr, fmt.Sprintf("Name%d", i))
			tx.Commit()
		}
	})

	b.Run("CardinalityMany/Add", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/tags").Type(schema.TypeString).Many().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/tags")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := db.NewTransaction()
			tx.Add(entity, attr, fmt.Sprintf("tag%d", i))
			tx.Commit()
		}
	})

	b.Run("CardinalityMany/AddRemove", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/tags").Type(schema.TypeString).Many().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/tags")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := db.NewTransaction()
			tx.Add(entity, attr, fmt.Sprintf("tag%d", i))
			tx.Remove(entity, attr, fmt.Sprintf("tag%d", i))
			tx.Commit()
		}
	})

	b.Run("CardinalityVector/Append", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/skills")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := db.NewTransaction()
			tx.Add(entity, attr, fmt.Sprintf("skill%d", i))
			tx.Commit()
		}
	})
}

// BenchmarkCRDTRead measures read performance for different cardinalities
func BenchmarkCRDTRead(b *testing.B) {
	b.Run("CardinalityOne", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Build()
		db.SetSchema(s)

		// Setup: write 100 versions
		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/name")
		for i := 0; i < 100; i++ {
			tx := db.NewTransaction()
			tx.Add(entity, attr, fmt.Sprintf("Name%d", i))
			tx.Commit()
		}

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			if val == nil {
				b.Fatal("Expected value")
			}
		}
	})

	b.Run("CardinalityMany/10members", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/tags").Type(schema.TypeString).Many().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/tags")
		tx := db.NewTransaction()
		for i := 0; i < 10; i++ {
			tx.Add(entity, attr, fmt.Sprintf("tag%d", i))
		}
		tx.Commit()

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			if val == nil {
				b.Fatal("Expected value")
			}
		}
	})

	b.Run("CardinalityMany/100members", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/tags").Type(schema.TypeString).Many().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/tags")
		tx := db.NewTransaction()
		for i := 0; i < 100; i++ {
			tx.Add(entity, attr, fmt.Sprintf("tag%d", i))
		}
		tx.Commit()

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			if val == nil {
				b.Fatal("Expected value")
			}
		}
	})

	b.Run("CardinalityVector/10elements", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/skills")
		tx := db.NewTransaction()
		for i := 0; i < 10; i++ {
			tx.Add(entity, attr, fmt.Sprintf("skill%d", i))
		}
		tx.Commit()

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			if val == nil {
				b.Fatal("Expected value")
			}
		}
	})

	b.Run("CardinalityVector/100elements", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/skills")
		tx := db.NewTransaction()
		for i := 0; i < 100; i++ {
			tx.Add(entity, attr, fmt.Sprintf("skill%d", i))
		}
		tx.Commit()

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			if val == nil {
				b.Fatal("Expected value")
			}
		}
	})
}

// BenchmarkCRDTResolution measures CRDT conflict resolution overhead
func BenchmarkCRDTResolution(b *testing.B) {
	b.Run("AddWins/NoConflict", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/tags").Type(schema.TypeString).Many().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/tags")

		// Add 50 unique tags
		tx := db.NewTransaction()
		for i := 0; i < 50; i++ {
			tx.Add(entity, attr, fmt.Sprintf("tag%d", i))
		}
		tx.Commit()

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			slice := val.([]interface{})
			if len(slice) != 50 {
				b.Fatalf("Expected 50 members, got %d", len(slice))
			}
		}
	})

	b.Run("AddWins/WithTombstones", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/tags").Type(schema.TypeString).Many().Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/tags")

		// Add 100 tags, remove 50
		tx := db.NewTransaction()
		for i := 0; i < 100; i++ {
			tx.Add(entity, attr, fmt.Sprintf("tag%d", i))
		}
		tx.Commit()

		tx = db.NewTransaction()
		for i := 0; i < 50; i++ {
			tx.Remove(entity, attr, fmt.Sprintf("tag%d", i))
		}
		tx.Commit()

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			slice := val.([]interface{})
			if len(slice) != 50 {
				b.Fatalf("Expected 50 members, got %d", len(slice))
			}
		}
	})

	b.Run("LWW/ManyVersions", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Build()
		db.SetSchema(s)

		entity := datalog.NewIdentity("bench-entity")
		attr := datalog.NewKeyword(":person/name")

		// Write 1000 versions
		for i := 0; i < 1000; i++ {
			tx := db.NewTransaction()
			tx.Add(entity, attr, fmt.Sprintf("Name%d", i))
			tx.Commit()
		}

		matcher := NewBadgerMatcher(db.Store())
		matcher.SetSchema(s)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			val, _ := requireAttributeLookup(b, matcher, entity, attr)
			if val != "Name999" {
				b.Fatalf("Expected Name999, got %v", val)
			}
		}
	})
}

// BenchmarkCRDTQuery measures query performance with CRDT storage
func BenchmarkCRDTQuery(b *testing.B) {
	b.Run("SimpleQuery/100entities", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Attribute(":person/age").Type(schema.TypeLong).Add().
			Build()
		db.SetSchema(s)

		// Insert 100 entities
		for i := 0; i < 100; i++ {
			tx := db.NewTransaction()
			entity := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(entity, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(entity, datalog.NewKeyword(":person/age"), int64(20+i))
			tx.Commit()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := executor.CollectTuples(db.Query(`[:find ?e ?name :where [?e :person/name ?name]]`))
			if err != nil {
				b.Fatal(err)
			}
			if len(result) != 100 {
				b.Fatalf("Expected 100 results, got %d", len(result))
			}
		}
	})

	b.Run("JoinQuery/100entities", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Attribute(":person/age").Type(schema.TypeLong).Add().
			Build()
		db.SetSchema(s)

		// Insert 100 entities
		for i := 0; i < 100; i++ {
			tx := db.NewTransaction()
			entity := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(entity, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(entity, datalog.NewKeyword(":person/age"), int64(20+i))
			tx.Commit()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := executor.CollectTuples(db.Query(`[:find ?e ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`))
			if err != nil {
				b.Fatal(err)
			}
			if len(result) != 100 {
				b.Fatalf("Expected 100 results, got %d", len(result))
			}
		}
	})

	b.Run("CardinalityManyQuery/FindByValue", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Attribute(":person/tags").Type(schema.TypeString).Many().Add().
			Build()
		db.SetSchema(s)

		// Insert 100 entities with tags
		for i := 0; i < 100; i++ {
			tx := db.NewTransaction()
			entity := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(entity, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			tx.Add(entity, datalog.NewKeyword(":person/tags"), "common")
			tx.Add(entity, datalog.NewKeyword(":person/tags"), fmt.Sprintf("unique%d", i))
			tx.Commit()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := executor.CollectTuples(db.Query(`[:find ?e :where [?e :person/tags "common"]]`))
			if err != nil {
				b.Fatal(err)
			}
			if len(result) != 100 {
				b.Fatalf("Expected 100 results, got %d", len(result))
			}
		}
	})

	b.Run("VectorQuery/WithJoin", func(b *testing.B) {
		db := setupCRDTBenchDB(b)
		defer db.Close()

		s, _ := schema.NewBuilder().
			Attribute(":person/name").Type(schema.TypeString).Add().
			Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
			Build()
		db.SetSchema(s)

		// Insert 50 entities with skills
		for i := 0; i < 50; i++ {
			tx := db.NewTransaction()
			entity := datalog.NewIdentity(fmt.Sprintf("person%d", i))
			tx.Add(entity, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
			for j := 0; j < 5; j++ {
				tx.Add(entity, datalog.NewKeyword(":person/skills"), fmt.Sprintf("skill%d", j))
			}
			tx.Commit()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := executor.CollectTuples(db.Query(`[:find ?name ?skills :where [?e :person/name ?name] [?e :person/skills ?skills]]`))
			if err != nil {
				b.Fatal(err)
			}
			if len(result) != 50 {
				b.Fatalf("Expected 50 results, got %d", len(result))
			}
		}
	})
}

// BenchmarkElementID measures ElementID operations
func BenchmarkElementID(b *testing.B) {
	b.Run("Encode", func(b *testing.B) {
		id := datalog.ElementID{Lamport: 12345678, ReplicaID: 87654321}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeElementIDForKey(id)
		}
	})

	b.Run("Decode", func(b *testing.B) {
		id := datalog.ElementID{Lamport: 12345678, ReplicaID: 87654321}
		encoded := EncodeElementIDForKey(id)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = DecodeElementID(encoded)
		}
	})

	b.Run("Compare", func(b *testing.B) {
		id1 := datalog.ElementID{Lamport: 12345678, ReplicaID: 87654321}
		id2 := datalog.ElementID{Lamport: 12345679, ReplicaID: 87654321}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = id1.Compare(id2)
		}
	})
}

func setupCRDTBenchDB(b *testing.B) *Database {
	b.Helper()
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	return db
}
