package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestResolveAllAttributesManyPreservesWildcardSemantics(t *testing.T) {
	for _, disableCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable_cache=%t", disableCache), func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/tags").Type(schema.TypeString).Many().Add().
				Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
				Attribute(":person/blobs").Type(schema.TypeBytes).Many().Add().
				Attribute(":person/missing").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:         t.TempDir(),
				Schema:       s,
				DisableCache: disableCache,
			})
			require.NoError(t, err)
			defer db.Close()

			name := datalog.NewKeyword(":person/name")
			tags := datalog.NewKeyword(":person/tags")
			skills := datalog.NewKeyword(":person/skills")
			blobs := datalog.NewKeyword(":person/blobs")
			first := datalog.NewIdentity("batch-resolve-first")
			second := datalog.NewIdentity("batch-resolve-second")
			absent := datalog.NewIdentity("batch-resolve-absent")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(first, name, "First"))
			require.NoError(t, tx.Add(first, tags, "admin"))
			require.NoError(t, tx.Add(first, tags, "author"))
			require.NoError(t, tx.Add(first, skills, "Go"))
			require.NoError(t, tx.Add(first, skills, "Rust"))
			require.NoError(t, tx.Add(first, blobs, []byte("blob")))
			require.NoError(t, tx.Set(second, name, "Second"))
			require.NoError(t, tx.Remove(second, name, "Second"))
			_, err = tx.Commit()
			require.NoError(t, err)

			results, err := db.ResolveAllAttributesMany(
				[]datalog.Identity{second, first, absent, first},
			)
			require.NoError(t, err)
			require.Len(t, results, 4)
			require.Empty(t, results[0], "the latest name operation is a tombstone")
			require.Equal(t, "First", results[1][name])
			require.ElementsMatch(t, []interface{}{"admin", "author"}, results[1][tags])
			require.Equal(t, []string{"Go", "Rust"}, results[1][skills])
			require.Empty(t, results[2])
			require.Equal(t, results[1], results[3])
			results[1][skills].([]string)[0] = "Changed"
			require.Equal(t, []string{"Go", "Rust"}, results[3][skills],
				"duplicate entities must receive independent vector values")
			results[1][blobs].([]interface{})[0].([]byte)[0] = 'X'
			require.Equal(t, []byte("blob"), results[3][blobs].([]interface{})[0],
				"duplicate entities must receive independent byte values")
		})
	}
}

func TestResolveAllAttributesManyHonorsAsOf(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir(), Schema: s})
	require.NoError(t, err)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")
	first := datalog.NewIdentity("batch-asof-first")
	second := datalog.NewIdentity("batch-asof-second")
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(first, name, "First v1"))
	require.NoError(t, tx.Set(second, name, "Second v1"))
	asOfID, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(first, name, "First v2"))
	require.NoError(t, tx.Set(second, name, "Second v2"))
	_, err = tx.Commit()
	require.NoError(t, err)

	results, err := db.AsOf(asOfID).ResolveAllAttributesMany(
		[]datalog.Identity{first, second},
	)
	require.NoError(t, err)
	require.Equal(t, "First v1", results[0][name])
	require.Equal(t, "Second v1", results[1][name])
}

func TestResolveAllAttributesManyPreservesUniqueFallback(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir(), Schema: s})
	require.NoError(t, err)
	defer db.Close()

	email := datalog.NewKeyword(":person/email")
	first := datalog.NewIdentity("batch-unique-first")
	second := datalog.NewIdentity("batch-unique-second")
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(first, email, "first@example.com"))
	_, err = tx.Commit()
	require.NoError(t, err)
	tx = db.NewTransaction()
	require.NoError(t, tx.Set(first, email, "shared@example.com"))
	_, err = tx.Commit()
	require.NoError(t, err)
	tx = db.NewTransaction()
	require.NoError(t, tx.Set(second, email, "shared@example.com"))
	_, err = tx.Commit()
	require.NoError(t, err)

	results, err := db.ResolveAllAttributesMany([]datalog.Identity{first, second})
	require.NoError(t, err)
	require.Equal(t, "first@example.com", results[0][email])
	require.Equal(t, "shared@example.com", results[1][email])
}

func TestResolveAllAttributesManyMatchesDeclaredSchema(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir(), Schema: s})
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("batch-declared-schema")
	name := datalog.NewKeyword(":person/name")
	nickname := datalog.NewKeyword(":person/nickname")
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(entity, name, "Declared"))
	require.NoError(t, tx.Set(entity, nickname, "Stored but undeclared"))
	_, err = tx.Commit()
	require.NoError(t, err)

	perEntity, err := db.ResolveAllAttributes(entity)
	require.NoError(t, err)
	batch, err := db.ResolveAllAttributesMany([]datalog.Identity{entity})
	require.NoError(t, err)
	require.Equal(t, perEntity, batch[0])
	require.NotContains(t, batch[0], nickname)
}

func TestResolveAllAttributesManyKeepsExplicitlyClearedVector(t *testing.T) {
	for _, disableCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable_cache=%t", disableCache), func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
				Build()
			require.NoError(t, err)
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:         t.TempDir(),
				Schema:       s,
				DisableCache: disableCache,
			})
			require.NoError(t, err)
			defer db.Close()

			entity := datalog.NewIdentity("batch-cleared-vector")
			neverSet := datalog.NewIdentity("batch-never-set-vector")
			skills := datalog.NewKeyword(":person/skills")
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(entity, skills, "Go"))
			_, err = tx.Commit()
			require.NoError(t, err)
			tx = db.NewTransaction()
			require.NoError(t, tx.Remove(entity, skills, "Go"))
			_, err = tx.Commit()
			require.NoError(t, err)

			perEntity, err := db.ResolveAllAttributes(entity)
			require.NoError(t, err)
			batch, err := db.ResolveAllAttributesMany([]datalog.Identity{entity})
			require.NoError(t, err)
			require.Equal(t, perEntity, batch[0])
			require.Equal(t, []string{}, batch[0][skills])

			neverSetPerEntity, err := db.ResolveAllAttributes(neverSet)
			require.NoError(t, err)
			neverSetBatch, err := db.ResolveAllAttributesMany([]datalog.Identity{neverSet})
			require.NoError(t, err)
			require.NotContains(t, neverSetPerEntity, skills)
			require.Equal(t, neverSetPerEntity, neverSetBatch[0])
		})
	}
}

func TestResolveAllAttributesManyDifferential(t *testing.T) {
	for _, disableCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable_cache=%t", disableCache), func(t *testing.T) {
			random := rand.New(rand.NewSource(0x381))
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/tags").Type(schema.TypeString).Many().Add().
				Attribute(":person/steps").Type(schema.TypeString).Vector().Add().
				Attribute(":person/missing").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:         t.TempDir(),
				Schema:       s,
				DisableCache: disableCache,
			})
			require.NoError(t, err)
			defer db.Close()

			name := datalog.NewKeyword(":person/name")
			tags := datalog.NewKeyword(":person/tags")
			steps := datalog.NewKeyword(":person/steps")
			undeclared := datalog.NewKeyword(":person/nickname")
			entities := make([]datalog.Identity, 64)
			addedSteps := make(map[[20]byte][]string)
			tx := db.NewTransaction()
			for i := range entities {
				entity := datalog.NewIdentity(fmt.Sprintf("batch-differential-%d", i))
				entities[i] = entity
				if random.Intn(4) != 0 {
					require.NoError(t, tx.Set(entity, name, fmt.Sprintf("Name %d", i)))
				}
				if random.Intn(3) == 0 {
					require.NoError(t, tx.Set(entity, undeclared, fmt.Sprintf("Nick %d", i)))
				}
				for tag := 0; tag < random.Intn(4); tag++ {
					require.NoError(t, tx.Add(entity, tags, fmt.Sprintf("tag-%d", tag)))
				}
				for step := 0; step < random.Intn(4); step++ {
					value := fmt.Sprintf("step-%d", step)
					require.NoError(t, tx.Add(entity, steps, value))
					addedSteps[entity.Hash()] = append(addedSteps[entity.Hash()], value)
				}
			}
			_, err = tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			for i, entity := range entities {
				if i%5 == 0 {
					for _, value := range addedSteps[entity.Hash()] {
						require.NoError(t, tx.Remove(entity, steps, value))
					}
				}
				if i%7 == 0 {
					require.NoError(t, tx.Remove(entity, name, fmt.Sprintf("Name %d", i)))
				}
			}
			_, err = tx.Commit()
			require.NoError(t, err)

			expected := make([]map[datalog.Keyword]interface{}, len(entities))
			for i, entity := range entities {
				expected[i], err = db.ResolveAllAttributes(entity)
				require.NoError(t, err)
			}
			actual, err := db.ResolveAllAttributesMany(entities)
			require.NoError(t, err)
			require.Len(t, actual, len(expected))
			for i := range expected {
				require.Equal(t,
					normalizeWildcardAttributes(expected[i]),
					normalizeWildcardAttributes(actual[i]),
					"entity %d", i,
				)
			}
		})
	}
}

func TestResolveAllAttributesManyHistoryMatchesPerEntity(t *testing.T) {
	db, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("batch-history")
	name := datalog.NewKeyword(":person/name")
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(entity, name, "First"))
	_, err = tx.Commit()
	require.NoError(t, err)
	tx = db.NewTransaction()
	require.NoError(t, tx.Set(entity, name, "Second"))
	_, err = tx.Commit()
	require.NoError(t, err)

	history := db.History()
	perEntity, err := history.ResolveAllAttributes(entity)
	require.NoError(t, err)
	batch, err := history.ResolveAllAttributesMany([]datalog.Identity{entity})
	require.NoError(t, err)
	require.Equal(t, perEntity, batch[0])
}

func TestWildcardPullQueryUsesOneBatch(t *testing.T) {
	var batchBegins, batchCompletes int
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: t.TempDir(),
		AnnotationHandler: func(event annotations.Event) {
			switch event.Name {
			case annotations.PullBatchBegin:
				batchBegins++
				require.Equal(t, 230, event.Data["entity_count"])
			case annotations.PullBatchComplete:
				batchCompletes++
				require.Equal(t, true, event.Data["success"])
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

	entityType := datalog.NewKeyword(":entity/type")
	name := datalog.NewKeyword(":entity/name")
	tx := db.NewTransaction()
	for i := 0; i < 230; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("scenario:%d", i))
		require.NoError(t, tx.Set(entity, entityType, "scenario"))
		require.NoError(t, tx.Set(entity, name, fmt.Sprintf("Scenario %d", i)))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	result, queryErr := db.Query(`[:find (pull ?entity [*])
		:where [?entity :entity/type "scenario"]]`)
	rows, err := executor.CollectTuples(result, queryErr)
	require.NoError(t, err)
	require.Len(t, rows, 230)
	require.Equal(t, 1, batchBegins)
	require.Equal(t, 1, batchCompletes)
	for _, row := range rows {
		pulled, ok := row[0].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, "scenario", pulled["entity/type"])
		require.NotNil(t, pulled[query.DBIDKey])
	}
}

func TestWildcardPullQueryRendersMultiValuedAttributes(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":entity/type").Type(schema.TypeString).One().Add().
		Attribute(":entity/tags").Type(schema.TypeString).Many().Add().
		Attribute(":entity/steps").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir(), Schema: s})
	require.NoError(t, err)
	defer db.Close()

	entityType := datalog.NewKeyword(":entity/type")
	tags := datalog.NewKeyword(":entity/tags")
	steps := datalog.NewKeyword(":entity/steps")
	tx := db.NewTransaction()
	for i := 0; i < 3; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("multi:%d", i))
		require.NoError(t, tx.Set(entity, entityType, "multi"))
		require.NoError(t, tx.Add(entity, tags, "one"))
		require.NoError(t, tx.Add(entity, tags, "two"))
		require.NoError(t, tx.Add(entity, steps, "first"))
		require.NoError(t, tx.Add(entity, steps, "second"))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	result, queryErr := db.Query(`[:find (pull ?entity [*])
		:where [?entity :entity/type "multi"]]`)
	rows, err := executor.CollectTuples(result, queryErr)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	for _, row := range rows {
		pulled := row[0].(map[string]interface{})
		require.ElementsMatch(t, []interface{}{"one", "two"}, pulled["entity/tags"])
		require.Equal(t, []string{"first", "second"}, pulled["entity/steps"])
	}
}

func normalizeWildcardAttributes(
	attrs map[datalog.Keyword]interface{},
) map[datalog.Keyword]interface{} {
	normalized := make(map[datalog.Keyword]interface{}, len(attrs))
	for attr, value := range attrs {
		if values, ok := value.([]interface{}); ok {
			items := make([]string, len(values))
			for i, item := range values {
				items[i] = fmt.Sprintf("%T:%v", item, item)
			}
			sort.Strings(items)
			normalized[attr] = items
		} else {
			normalized[attr] = value
		}
	}
	return normalized
}

func BenchmarkResolveAllAttributesMany(b *testing.B) {
	for _, entityCount := range []int{230, 3_899} {
		b.Run(fmt.Sprintf("entities=%d", entityCount), func(b *testing.B) {
			db, entities := setupBatchWildcardBenchmarkDB(b, entityCount, 5)

			b.Run("per-entity", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					for _, entity := range entities {
						if _, err := db.ResolveAllAttributes(entity); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := db.ResolveAllAttributesMany(entities); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func setupBatchWildcardBenchmarkDB(
	b *testing.B,
	entityCount, attrsPerEntity int,
) (*Database, []datalog.Identity) {
	b.Helper()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         b.TempDir(),
		DisableCache: true,
	})
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, db.Close())
	})

	attrs := make([]datalog.Keyword, attrsPerEntity)
	for i := range attrs {
		attrs[i] = datalog.NewKeyword(fmt.Sprintf(":entity/attr%d", i))
	}
	entities := make([]datalog.Identity, entityCount)
	tx := db.NewTransaction()
	for i := range entities {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("batch-entity:%d", i))
		for attrIndex, attr := range attrs {
			require.NoError(b, tx.Set(
				entities[i],
				attr,
				fmt.Sprintf("value-%d-%d", i, attrIndex),
			))
		}
	}
	_, err = tx.Commit()
	require.NoError(b, err)
	return db, entities
}
