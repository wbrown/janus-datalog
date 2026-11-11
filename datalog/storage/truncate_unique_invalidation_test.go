package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// uniqueItemSchema declares :item/code and :item/tag, each a unique
// cardinality-one string.
func uniqueItemSchema() *schema.Schema {
	s := schema.NewSchema()
	for _, ident := range []string{":item/code", ":item/tag"} {
		s.Add(&schema.AttributeDefinition{
			Ident:       datalog.NewKeyword(ident),
			ValueType:   schema.TypeString,
			Cardinality: schema.CardinalityOne,
			Unique:      schema.UniqueIdentity,
		})
	}
	return s
}

// A unique attribute's entry for one entity is resolved from other entities'
// claims of its value, so a rewind that erases another entity's claim stales
// the holder's entry without touching the holder's own key. Every entry under
// each unique attribute among the erased datoms is dropped: holder asserts a
// code and a tag before the snapshot, two claimants take them after it, holder
// is read by entity, and the rewind erases both claims. Holder then holds both
// again, with the cache on as with it off.
func TestTruncateToDropsEveryEntryOfEachErasedUniqueAttribute(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	tag := datalog.NewKeyword(":item/tag")
	holder := datalog.NewIdentity("item:holder")
	codeClaimant := datalog.NewIdentity("item:code-claimant")
	tagClaimant := datalog.NewIdentity("item:tag-claimant")

	for _, mode := range legsFor(backendModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: uniqueItemSchema(), DisableCache: cacheMode.disableCache})

				tx := d.NewTransaction()
				require.NoError(t, tx.Add(holder, code, "y"))
				require.NoError(t, tx.Add(holder, tag, "z"))
				_, err := tx.Commit()
				require.NoError(t, err)
				_, err = d.Snapshot("before the claims")
				require.NoError(t, err)

				tx = d.NewTransaction()
				require.NoError(t, tx.Add(codeClaimant, code, "y"))
				require.NoError(t, tx.Add(tagClaimant, tag, "z"))
				_, err = tx.Commit()
				require.NoError(t, err)

				// Read by entity, so the cache holds holder's entries resolved
				// with the claims in place.
				var codes, tags []string
				require.NoError(t, d.QueryInto(&codes, `[:find ?v :in $ ?e :where [?e :item/code ?v]]`, holder))
				require.NoError(t, d.QueryInto(&tags, `[:find ?v :in $ ?e :where [?e :item/tag ?v]]`, holder))
				require.Empty(t, codes, "with the claim in place holder holds no code")
				require.Empty(t, tags, "with the claim in place holder holds no tag")

				require.NoError(t, d.TruncateTo("before the claims"))

				codes, tags = nil, nil
				require.NoError(t, d.QueryInto(&codes, `[:find ?v :in $ ?e :where [?e :item/code ?v]]`, holder))
				require.NoError(t, d.QueryInto(&tags, `[:find ?v :in $ ?e :where [?e :item/tag ?v]]`, holder))
				assert.Equal(t, []string{"y"}, codes, "holder's code after the rewind")
				assert.Equal(t, []string{"z"}, tags, "holder's tag after the rewind")
			})
		}
	}
}

// Another entity's withdrawal of a unique value after the snapshot is an entry
// of the value the holder's resolution read, never a claim of it: holder
// asserts a code before the snapshot, another entity withdraws that code after
// it, holder is read by entity, and the rewind erases the withdrawal. Holder
// then holds the code, with the cache on as with it off.
func TestTruncateToDropsAHolderWhoseValueAnotherEntityWithdrewAfterTheSnapshot(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	holder := datalog.NewIdentity("item:holder")
	withdrawer := datalog.NewIdentity("item:withdrawer")

	for _, mode := range legsFor(backendModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: uniqueItemSchema(), DisableCache: cacheMode.disableCache})

				tx := d.NewTransaction()
				require.NoError(t, tx.Add(holder, code, "y"))
				_, err := tx.Commit()
				require.NoError(t, err)
				_, err = d.Snapshot("before the withdrawal")
				require.NoError(t, err)

				tx = d.NewTransaction()
				require.NoError(t, tx.Remove(withdrawer, code, "y"))
				_, err = tx.Commit()
				require.NoError(t, err)

				// Read by entity, so the cache holds holder's entry resolved
				// with the withdrawal in place.
				var codes []string
				require.NoError(t, d.QueryInto(&codes, `[:find ?v :in $ ?e :where [?e :item/code ?v]]`, holder))

				require.NoError(t, d.TruncateTo("before the withdrawal"))

				codes = nil
				require.NoError(t, d.QueryInto(&codes, `[:find ?v :in $ ?e :where [?e :item/code ?v]]`, holder))
				assert.Equal(t, []string{"y"}, codes, "holder's code after the rewind")
			})
		}
	}
}
