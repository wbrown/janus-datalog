// Benchmarks for unique-attribute walk resolution, comparing against
// non-unique CardinalityOne first-entry reads.
//
// Setup: N entities, each with both a unique attribute (:user/email)
// and a non-unique attribute (:user/name). Measurements cover:
//
//   - Happy path (uncontested): read current value of each attribute.
//     Unique read performs walk (expected ~2× seeks); non-unique read
//     uses first-entry shortcut.
//   - Contested path: additional takeovers create supersession. Unique
//     reads fall back through history.
//   - V-view (LookupByUnique) happy path: one claimant.
//   - Cache impact: all benchmarks run with cache warmed.
//
// Numbers are machine-dependent; the important comparison is the
// ratio between unique and non-unique reads, not the absolute values.

package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// setupUniqueBenchmarkDB creates a database with N entities, each with
// a unique email and non-unique name. Returns the db, the entity slice,
// and a cleanup function.
func setupUniqueBenchmarkDB(b *testing.B, n int) (*Database, []datalog.Identity, func()) {
	dir := b.TempDir()

	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Attribute(":user/name").Type(schema.TypeString).One().Add().
		Build()
	if err != nil {
		b.Fatal(err)
	}

	db, err := NewDatabaseWithSchema(dir, s)
	if err != nil {
		b.Fatal(err)
	}

	email := datalog.NewKeyword(":user/email")
	name := datalog.NewKeyword(":user/name")

	entities := make([]datalog.Identity, n)
	tx := db.NewTransaction()
	for i := 0; i < n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("user:%d", i))
		entities[i] = e
		if err := tx.Set(e, email, fmt.Sprintf("user%d@example.com", i)); err != nil {
			b.Fatal(err)
		}
		if err := tx.Set(e, name, fmt.Sprintf("User %d", i)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Warm cache on each attribute for at least one entity so
	// attribute-level metadata is populated (attrVersions, etc).
	_, _ = db.ResolveEntityAttributes(entities[0], []datalog.Keyword{email, name})

	return db, entities, func() { db.Close() }
}

// BenchmarkUniqueRead_Uncontested measures the cost of reading the
// current value of a unique attribute for a single entity, in the
// common case where no takeover has occurred.
func BenchmarkUniqueRead_Uncontested(b *testing.B) {
	db, entities, cleanup := setupUniqueBenchmarkDB(b, 1000)
	defer cleanup()

	email := datalog.NewKeyword(":user/email")
	attrs := []datalog.Keyword{email}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := entities[i%len(entities)]
		result, err := db.ResolveEntityAttributes(e, attrs)
		if err != nil {
			b.Fatal(err)
		}
		if len(result) != 1 {
			b.Fatalf("expected 1 attribute, got %d", len(result))
		}
	}
}

// BenchmarkNonUniqueRead_Baseline measures the same operation on a
// non-unique CardinalityOne attribute. This is the reference point
// for the overhead ratio.
func BenchmarkNonUniqueRead_Baseline(b *testing.B) {
	db, entities, cleanup := setupUniqueBenchmarkDB(b, 1000)
	defer cleanup()

	name := datalog.NewKeyword(":user/name")
	attrs := []datalog.Keyword{name}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := entities[i%len(entities)]
		result, err := db.ResolveEntityAttributes(e, attrs)
		if err != nil {
			b.Fatal(err)
		}
		if len(result) != 1 {
			b.Fatalf("expected 1 attribute, got %d", len(result))
		}
	}
}

// BenchmarkUniqueRead_ContestedLinear measures reads when each entity
// has had its email claimed by one other entity (one-level takeover).
// Exercises walk fallback in the worst reasonable case.
func BenchmarkUniqueRead_ContestedLinear(b *testing.B) {
	dir := b.TempDir()
	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	if err != nil {
		b.Fatal(err)
	}
	db, err := NewDatabaseWithSchema(dir, s)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	email := datalog.NewKeyword(":user/email")
	const n = 500

	// First pass: every entity claims email.
	entities := make([]datalog.Identity, n)
	contenders := make([]datalog.Identity, n)
	tx := db.NewTransaction()
	for i := 0; i < n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("user:%d", i))
		c := datalog.NewIdentity(fmt.Sprintf("contender:%d", i))
		entities[i] = e
		contenders[i] = c
		if err := tx.Set(e, email, fmt.Sprintf("user%d@example.com", i)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Second pass: contenders take email (higher Tx), forcing fallback.
	// Each user has one prior assertion; walk must skip it and discover
	// there is no fallback value available.
	tx = db.NewTransaction()
	for i := 0; i < n; i++ {
		if err := tx.Set(contenders[i], email, fmt.Sprintf("user%d@example.com", i)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	attrs := []datalog.Keyword{email}

	// Warm cache to stabilize.
	_, _ = db.ResolveEntityAttributes(entities[0], attrs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := entities[i%n]
		_, err := db.ResolveEntityAttributes(e, attrs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUniqueRead_DeepFallback measures worst-case walk cost: each
// entity has K historical assertions, with each one superseded by a
// distinct contender. The walk must examine every entry before either
// finding a fallback (rare) or giving up (common).
//
// This is the worst case for the walk primitive; in practice unique
// attributes see few takeovers, so this is an upper-bound characterization
// rather than a typical-workload number.
func BenchmarkUniqueRead_DeepFallback(b *testing.B) {
	dir := b.TempDir()
	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	if err != nil {
		b.Fatal(err)
	}
	db, err := NewDatabaseWithSchema(dir, s)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	email := datalog.NewKeyword(":user/email")
	const n = 200          // entities
	const historyDepth = 5 // assertions per user (each superseded)

	users := make([]datalog.Identity, n)
	for u := 0; u < n; u++ {
		users[u] = datalog.NewIdentity(fmt.Sprintf("user:%d", u))
	}

	// User K asserts K distinct emails over K transactions.
	for depth := 0; depth < historyDepth; depth++ {
		tx := db.NewTransaction()
		for u := 0; u < n; u++ {
			v := fmt.Sprintf("user%d-v%d@example.com", u, depth)
			if err := tx.Set(users[u], email, v); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}

	// Contenders take each of those values with higher Tx, forcing the
	// walk to examine and reject every historical entry.
	for depth := 0; depth < historyDepth; depth++ {
		tx := db.NewTransaction()
		for u := 0; u < n; u++ {
			contender := datalog.NewIdentity(fmt.Sprintf("contender:%d:%d", u, depth))
			v := fmt.Sprintf("user%d-v%d@example.com", u, depth)
			if err := tx.Set(contender, email, v); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}

	attrs := []datalog.Keyword{email}
	_, _ = db.ResolveEntityAttributes(users[0], attrs) // warm

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := users[i%n]
		_, err := db.ResolveEntityAttributes(u, attrs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLookupByUnique_Uncontested measures V-view lookup in the
// common case (single claimant).
func BenchmarkLookupByUnique_Uncontested(b *testing.B) {
	db, _, cleanup := setupUniqueBenchmarkDB(b, 1000)
	defer cleanup()

	email := datalog.NewKeyword(":user/email")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := fmt.Sprintf("user%d@example.com", i%1000)
		_, err := db.LookupByUnique(email, v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLookupByUnique_ColdCache measures V-view lookup cost when
// the cache is cleared between calls. Approximates worst-case cold
// lookup.
func BenchmarkLookupByUnique_ColdCache(b *testing.B) {
	db, _, cleanup := setupUniqueBenchmarkDB(b, 1000)
	defer cleanup()

	email := datalog.NewKeyword(":user/email")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Cache().Clear()
		v := fmt.Sprintf("user%d@example.com", i%1000)
		_, err := db.LookupByUnique(email, v)
		if err != nil {
			b.Fatal(err)
		}
	}
}
