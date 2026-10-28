package reflect_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// nestedIDOwner reaches one peer through a ref and several through a
// cardinality-many ref.
type nestedIDOwner struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
	Peer *nestedIDPeer    `datalog:"peer"`
	Refs []*nestedIDPeer  `datalog:"refs"`
}

type nestedIDPeer struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"name"`
}

// A struct read from a pull carries the pulled entity's identity in its id
// field at every depth: the peer reached through a ref and each peer
// reached through a many ref hold the identity of the entity pulled for
// them, under PullInto and PullIntoMany alike, with a schema and without
// one, on every backend and planner path.
func TestANestedStructReadFromAPullCarriesItsEntitysID(t *testing.T) {
	sch, err := dlreflect.SchemaFromStruct(nestedIDOwner{})
	require.NoError(t, err)
	schemas := []struct {
		name   string
		schema *schema.Schema
	}{{"with a schema", sch}, {"without a schema", nil}}
	for _, mode := range optimizerModes {
		for _, s := range schemas {
			t.Run(mode.name+"/"+s.name, func(t *testing.T) {
				popts := mode.plannerOptions()
				db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
					Store:          mustOpenStore(t, mode.backend),
					Schema:         s.schema,
					PlannerOptions: &popts,
				})
				require.NoError(t, err)
				defer db.Close()

				b, c1, c2 := &nestedIDPeer{Name: "b"}, &nestedIDPeer{Name: "c1"}, &nestedIDPeer{Name: "c2"}
				tx := db.NewTransaction()
				for _, peer := range []*nestedIDPeer{b, c1, c2} {
					_, err := tx.SaveStruct(peer)
					require.NoError(t, err)
				}
				ownerID, err := tx.SaveStruct(&nestedIDOwner{Name: "a", Peer: b, Refs: []*nestedIDPeer{c1, c2}})
				require.NoError(t, err)
				_, err = tx.Commit()
				require.NoError(t, err)
				peers := []datalog.Identity{c1.ID, c2.ID}

				var one nestedIDOwner
				require.NoError(t, db.PullInto(ownerID, &one))
				require.Equal(t, ownerID, one.ID)
				require.NotNil(t, one.Peer)
				require.Equal(t, b.ID, one.Peer.ID, "the peer reached through the ref carries the entity pulled for it")
				require.NotEmpty(t, one.Refs)
				for _, ref := range one.Refs {
					require.Contains(t, peers, ref.ID, "a peer reached through the many ref carries the entity pulled for it")
				}

				var many []nestedIDOwner
				require.NoError(t, db.PullIntoMany([]datalog.Identity{ownerID}, &many))
				require.Len(t, many, 1)
				require.Equal(t, ownerID, many[0].ID)
				require.NotNil(t, many[0].Peer)
				require.Equal(t, b.ID, many[0].Peer.ID, "under PullIntoMany the peer carries the entity pulled for it")
			})
		}
	}
}

// The reader fills a nested struct's id field from the nested map's db/id.
func TestReadStructFillsANestedStructsIDFromItsMap(t *testing.T) {
	peer := datalog.NewIdentity("peer")
	var owner nestedIDOwner
	require.NoError(t, dlreflect.ReadStruct(map[string]interface{}{
		"nested-id-owner/name": "a",
		"nested-id-owner/peer": map[string]interface{}{query.DBIDKey: peer, "nested-id-peer/name": "b"},
	}, &owner, nil))
	require.NotNil(t, owner.Peer)
	require.Equal(t, "b", owner.Peer.Name)
	require.Equal(t, peer, owner.Peer.ID, "the nested struct's id is the nested map's db/id")
}
