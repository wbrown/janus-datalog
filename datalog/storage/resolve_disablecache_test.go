// Tests that ResolveEntityAttributes and ResolveAllAttributes work
// correctly under DisableCache: true.
//
// Principle: the cache is an optimization, not a correctness requirement.
// Both methods must produce identical results regardless of cache mode.
// Today they fail under DisableCache:
//
//   - ResolveEntityAttributes panics on nil d.cache deref
//   - ResolveAllAttributes errors out with "ResolveAllAttributes requires cache"
//
// Each test runs against both modes; identical results validate the principle.

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var resolveDisableCacheModes = []struct {
	name         string
	disableCache bool
}{
	{"cache_enabled", false},
	{"cache_disabled", true},
}

// setupResolveDisableCacheDB opens a database on the mode's backend in the
// requested cache mode.
func setupResolveDisableCacheDB(t *testing.T, optMode optimizerMode, disableCache bool) *Database {
	t.Helper()
	return createOptimizerModeDB(t, optMode, DatabaseOptions{DisableCache: disableCache})
}

// TestResolveEntityAttributes_BothCacheModes_CardinalityOne verifies
// ResolveEntityAttributes works for cardinality-one attributes regardless
// of cache mode. Includes an LWW scenario (two writes, latest wins) to
// exercise cache-less CRDT resolution.
func TestResolveEntityAttributes_BothCacheModes_CardinalityOne(t *testing.T) {
	for _, optMode := range optimizerModes {
		t.Run(optMode.name, func(t *testing.T) {
			for _, mode := range resolveDisableCacheModes {
				t.Run(mode.name, func(t *testing.T) {
					db := setupResolveDisableCacheDB(t, optMode, mode.disableCache)

					s, err := schema.NewBuilder().
						Attribute(":person/name").Type(schema.TypeString).One().Add().
						Attribute(":person/age").Type(schema.TypeLong).One().Add().
						Build()
					require.NoError(t, err)
					db.SetSchema(s)

					alice := datalog.NewIdentity("alice")
					nameAttr := datalog.NewKeyword(":person/name")
					ageAttr := datalog.NewKeyword(":person/age")

					// First write
					tx := db.NewTransaction()
					require.NoError(t, tx.Set(alice, nameAttr, "Alice"))
					require.NoError(t, tx.Set(alice, ageAttr, int64(30)))
					_, err = tx.Commit()
					require.NoError(t, err)

					// LWW: overwrite name; latest must win
					tx2 := db.NewTransaction()
					require.NoError(t, tx2.Set(alice, nameAttr, "Alice2"))
					_, err = tx2.Commit()
					require.NoError(t, err)

					result, err := db.ResolveEntityAttributes(alice, []datalog.Keyword{nameAttr, ageAttr})
					require.NoError(t, err)

					assert.Equal(t, "Alice2", result[nameAttr], "LWW winner should be returned")
					assert.Equal(t, int64(30), result[ageAttr])
				})
			}
		})
	}
}

// TestResolveEntityAttributes_BothCacheModes_CardinalityMany verifies
// add-wins resolution returns the full set under both cache modes.
func TestResolveEntityAttributes_BothCacheModes_CardinalityMany(t *testing.T) {
	for _, optMode := range optimizerModes {
		t.Run(optMode.name, func(t *testing.T) {
			for _, mode := range resolveDisableCacheModes {
				t.Run(mode.name, func(t *testing.T) {
					db := setupResolveDisableCacheDB(t, optMode, mode.disableCache)

					s, err := schema.NewBuilder().
						Attribute(":person/tags").Type(schema.TypeString).Many().Add().
						Build()
					require.NoError(t, err)
					db.SetSchema(s)

					alice := datalog.NewIdentity("alice")
					tagAttr := datalog.NewKeyword(":person/tags")

					tx := db.NewTransaction()
					require.NoError(t, tx.Add(alice, tagAttr, "admin"))
					require.NoError(t, tx.Add(alice, tagAttr, "user"))
					require.NoError(t, tx.Add(alice, tagAttr, "editor"))
					_, err = tx.Commit()
					require.NoError(t, err)

					result, err := db.ResolveEntityAttributes(alice, []datalog.Keyword{tagAttr})
					require.NoError(t, err)

					tags, ok := result[tagAttr].([]interface{})
					require.True(t, ok, "expected []interface{}, got %T", result[tagAttr])

					tagSet := make(map[string]bool)
					for _, v := range tags {
						tagSet[v.(string)] = true
					}
					assert.True(t, tagSet["admin"], "missing admin")
					assert.True(t, tagSet["user"], "missing user")
					assert.True(t, tagSet["editor"], "missing editor")
					assert.Len(t, tagSet, 3)
				})
			}
		})
	}
}

// TestResolveEntityAttributes_BothCacheModes_CardinalityVector verifies
// RGA-resolved vector values under both cache modes.
func TestResolveEntityAttributes_BothCacheModes_CardinalityVector(t *testing.T) {
	for _, optMode := range optimizerModes {
		t.Run(optMode.name, func(t *testing.T) {
			for _, mode := range resolveDisableCacheModes {
				t.Run(mode.name, func(t *testing.T) {
					db := setupResolveDisableCacheDB(t, optMode, mode.disableCache)

					s, err := schema.NewBuilder().
						Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
						Build()
					require.NoError(t, err)
					db.SetSchema(s)

					alice := datalog.NewIdentity("alice")
					skillsAttr := datalog.NewKeyword(":person/skills")

					tx := db.NewTransaction()
					require.NoError(t, tx.Add(alice, skillsAttr, "Go"))
					require.NoError(t, tx.Add(alice, skillsAttr, "Rust"))
					require.NoError(t, tx.Add(alice, skillsAttr, "Python"))
					_, err = tx.Commit()
					require.NoError(t, err)

					result, err := db.ResolveEntityAttributes(alice, []datalog.Keyword{skillsAttr})
					require.NoError(t, err)

					skills := result[skillsAttr]
					require.NotNil(t, skills, "skills should be present")

					// Vector returns a slice; element type may be []string or []interface{}
					// depending on schema.ValueType handling. Normalize for assertion.
					var got []string
					switch v := skills.(type) {
					case []string:
						got = v
					case []interface{}:
						for _, x := range v {
							got = append(got, x.(string))
						}
					default:
						t.Fatalf("unexpected vector type %T", skills)
					}
					assert.Equal(t, []string{"Go", "Rust", "Python"}, got)
				})
			}
		})
	}
}

// TestResolveEntityAttributes_BothCacheModes_MissingAttribute verifies
// that missing attributes are simply absent from the result map (not an error).
func TestResolveEntityAttributes_BothCacheModes_MissingAttribute(t *testing.T) {
	for _, optMode := range optimizerModes {
		t.Run(optMode.name, func(t *testing.T) {
			for _, mode := range resolveDisableCacheModes {
				t.Run(mode.name, func(t *testing.T) {
					db := setupResolveDisableCacheDB(t, optMode, mode.disableCache)

					s, err := schema.NewBuilder().
						Attribute(":person/name").Type(schema.TypeString).One().Add().
						Attribute(":person/email").Type(schema.TypeString).One().Add().
						Build()
					require.NoError(t, err)
					db.SetSchema(s)

					alice := datalog.NewIdentity("alice")
					nameAttr := datalog.NewKeyword(":person/name")
					emailAttr := datalog.NewKeyword(":person/email")

					tx := db.NewTransaction()
					require.NoError(t, tx.Set(alice, nameAttr, "Alice"))
					// no email asserted
					_, err = tx.Commit()
					require.NoError(t, err)

					result, err := db.ResolveEntityAttributes(alice, []datalog.Keyword{nameAttr, emailAttr})
					require.NoError(t, err)

					assert.Equal(t, "Alice", result[nameAttr])
					_, hasEmail := result[emailAttr]
					assert.False(t, hasEmail, "missing attribute should not appear in result")
				})
			}
		})
	}
}

// TestResolveAllAttributes_BothCacheModes_WithSchema verifies the
// schema-driven path of ResolveAllAttributes works under both cache modes.
func TestResolveAllAttributes_BothCacheModes_WithSchema(t *testing.T) {
	for _, optMode := range optimizerModes {
		t.Run(optMode.name, func(t *testing.T) {
			for _, mode := range resolveDisableCacheModes {
				t.Run(mode.name, func(t *testing.T) {
					db := setupResolveDisableCacheDB(t, optMode, mode.disableCache)

					s, err := schema.NewBuilder().
						Attribute(":person/name").Type(schema.TypeString).One().Add().
						Attribute(":person/email").Type(schema.TypeString).One().Add().
						Attribute(":person/tags").Type(schema.TypeString).Many().Add().
						Build()
					require.NoError(t, err)
					db.SetSchema(s)

					alice := datalog.NewIdentity("alice")

					tx := db.NewTransaction()
					require.NoError(t, tx.Set(alice, datalog.NewKeyword(":person/name"), "Alice"))
					require.NoError(t, tx.Set(alice, datalog.NewKeyword(":person/email"), "alice@x.y"))
					require.NoError(t, tx.Add(alice, datalog.NewKeyword(":person/tags"), "admin"))
					_, err = tx.Commit()
					require.NoError(t, err)

					result, err := db.ResolveAllAttributes(alice)
					require.NoError(t, err)

					assert.Equal(t, "Alice", result[datalog.NewKeyword(":person/name")])
					assert.Equal(t, "alice@x.y", result[datalog.NewKeyword(":person/email")])

					tags, ok := result[datalog.NewKeyword(":person/tags")].([]interface{})
					require.True(t, ok, "expected []interface{}, got %T", result[datalog.NewKeyword(":person/tags")])
					require.Len(t, tags, 1)
					assert.Equal(t, "admin", tags[0])
				})
			}
		})
	}
}

// TestResolveAllAttributes_BothCacheModes_NoSchema verifies the EAVT-discovery
// path of ResolveAllAttributes works under both cache modes.
func TestResolveAllAttributes_BothCacheModes_NoSchema(t *testing.T) {
	for _, optMode := range optimizerModes {
		t.Run(optMode.name, func(t *testing.T) {
			for _, mode := range resolveDisableCacheModes {
				t.Run(mode.name, func(t *testing.T) {
					db := setupResolveDisableCacheDB(t, optMode, mode.disableCache)

					// No schema; ResolveAllAttributes uses EAVT to discover attributes.
					alice := datalog.NewIdentity("alice")

					tx := db.NewTransaction()
					require.NoError(t, tx.Set(alice, datalog.NewKeyword(":person/name"), "Alice"))
					require.NoError(t, tx.Set(alice, datalog.NewKeyword(":person/email"), "alice@x.y"))
					_, err := tx.Commit()
					require.NoError(t, err)

					result, err := db.ResolveAllAttributes(alice)
					require.NoError(t, err)

					assert.Equal(t, "Alice", result[datalog.NewKeyword(":person/name")])
					assert.Equal(t, "alice@x.y", result[datalog.NewKeyword(":person/email")])
				})
			}
		})
	}
}
