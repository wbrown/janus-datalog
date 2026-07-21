package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestOrJoinProjectionInheritsOuterBindings reproduces the bug documented in
// docs/bugs/active/ORJOIN_PROJECTION_REQUIRES_REBINDING.md
//
// When or-join projection variables are already bound by outer clauses,
// Datomic makes those bindings available inside each branch. Janus Datalog
// currently does not: it converts or-join to plain or, discarding the
// JoinVars, so branches execute without outer context.
//
// Data model: containers have an owner, location, and items.
// Items or containers can be flagged. Containers can be archived or deleted.
//
// The query: find items in containers matching an owner+location, where
// either the item OR its container is flagged, excluding archived/deleted
// containers.
func TestOrJoinProjectionInheritsOuterBindings(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "orjoin-projection-*")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			// Schema: :container/item is cardinality-many (a container has multiple items)
			s := schema.NewBuilder().
				Attribute(":container/item").Type(schema.TypeRef).Many().Add().
				MustBuild()

			popts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           tmpDir,
				Schema:         s,
				PlannerOptions: &popts,
			})
			require.NoError(t, err)
			defer db.Close()

			// Set up data
			containerA := datalog.NewIdentity("container:A")
			containerB := datalog.NewIdentity("container:B")
			containerC := datalog.NewIdentity("container:C")
			item1 := datalog.NewIdentity("item:1")
			item2 := datalog.NewIdentity("item:2")
			item3 := datalog.NewIdentity("item:3")
			item4 := datalog.NewIdentity("item:4")
			owner := datalog.NewIdentity("owner:alice")

			kw := datalog.NewKeyword

			tx := db.NewTransaction()
			// Container A: owned by alice, in warehouse-1, has items 1 and 2
			tx.Add(containerA, kw(":container/owner"), owner)
			tx.Add(containerA, kw(":container/location"), "warehouse-1")
			tx.Add(containerA, kw(":container/item"), item1)
			tx.Add(containerA, kw(":container/item"), item2)

			// Container B: owned by alice, in warehouse-1, has item 3, is archived
			tx.Add(containerB, kw(":container/owner"), owner)
			tx.Add(containerB, kw(":container/location"), "warehouse-1")
			tx.Add(containerB, kw(":container/item"), item3)
			tx.Add(containerB, kw(":container/archived"), true)

			// Container C: owned by alice, in warehouse-2, has item 4
			tx.Add(containerC, kw(":container/owner"), owner)
			tx.Add(containerC, kw(":container/location"), "warehouse-2")
			tx.Add(containerC, kw(":container/item"), item4)

			// Item 1: flagged (item-level flag)
			tx.Add(item1, kw(":item/flagged"), true)
			// Item 2: not flagged, but container A is flagged (container-level flag)
			tx.Add(containerA, kw(":item/flagged"), true)
			// Item 3: flagged, but container B is archived (should be excluded)
			tx.Add(item3, kw(":item/flagged"), true)
			// Item 4: flagged, but in warehouse-2 (should be excluded by location filter)
			tx.Add(item4, kw(":item/flagged"), true)

			_, err = tx.Commit()
			require.NoError(t, err)

			t.Run("idiomatic_or_join_with_outer_bindings", func(t *testing.T) {
				// This is the idiomatic Datomic pattern from the bug doc.
				// ?container and ?item are bound by outer clauses; the or-join
				// varies which entity carries :item/flagged.
				//
				// Expected: items 1 and 2 from container A (warehouse-1, not archived)
				// Item 1 matches because it is directly flagged.
				// Item 2 matches because its container (A) is flagged.
				// Item 3 excluded: container B is archived.
				// Item 4 excluded: container C is in warehouse-2.
				result, err := executor.CollectTuples(db.Query(
					`[:find ?item
			  :in $ ?owner ?location
			  :where
			  [?container :container/owner ?owner]
			  [?container :container/location ?location]
			  [?container :container/item ?item]
			  (or-join [?container ?item]
			    [?item :item/flagged true]
			    [?container :item/flagged true])
			  (not [?container :container/archived true])]`,
					owner, "warehouse-1"))
				require.NoError(t, err, "or-join with outer-bound projection variables should not error")
				assert.Len(t, result, 2, "should find 2 flagged items in warehouse-1")
			})

			t.Run("workaround_repeated_bindings", func(t *testing.T) {
				// The documented workaround: duplicate binding clauses inside each branch.
				result, err := executor.CollectTuples(db.Query(
					`[:find ?item
			  :in $ ?owner ?location
			  :where
			  (or-join [?container ?item]
			    (and [?container :container/owner ?owner]
			         [?container :container/location ?location]
			         [?container :container/item ?item]
			         [?item :item/flagged true])
			    (and [?container :container/owner ?owner]
			         [?container :container/location ?location]
			         [?container :container/item ?item]
			         [?container :item/flagged true]))
			  (not [?container :container/archived true])]`,
					owner, "warehouse-1"))
				require.NoError(t, err)
				assert.Len(t, result, 2, "workaround should find 2 flagged items")
			})

			t.Run("qb_or_join_with_outer_bindings", func(t *testing.T) {
				// Same query via the qb builder.
				container := qb.NewVar("container")
				item := qb.NewVar("item")
				ownerVar := qb.NewVar("owner")
				locationVar := qb.NewVar("location")

				q := qb.Query().
					Find(item).
					In(qb.DB, qb.Scalar(ownerVar), qb.Scalar(locationVar)).
					Where(
						qb.Pat(container, qb.Kw(":container/owner"), ownerVar),
						qb.Pat(container, qb.Kw(":container/location"), locationVar),
						qb.Pat(container, qb.Kw(":container/item"), item),
						qb.OrJoin(container, item).
							Branch(qb.Pat(item, qb.Kw(":item/flagged"), qb.V(true))).
							Branch(qb.Pat(container, qb.Kw(":item/flagged"), qb.V(true))),
						qb.Not(qb.Pat(container, qb.Kw(":container/archived"), qb.V(true))),
					).
					MustBuild()

				result, err := executor.CollectTuples(db.Query(q, owner, "warehouse-1"))
				require.NoError(t, err, "qb or-join with outer-bound projection variables should not error")
				assert.Len(t, result, 2, "should find 2 flagged items in warehouse-1")
			})

			t.Run("simple_or_join_projects_outer_variable", func(t *testing.T) {
				// Minimal reproduction: or-join lists a variable in its projection
				// that is only bound by an outer clause, not by any branch.
				// The or-join should carry the outer binding through.
				result, err := executor.CollectTuples(db.Query(
					`[:find ?container ?item
			  :where
			  [?container :container/item ?item]
			  (or-join [?container ?item]
			    [?item :item/flagged true]
			    [?container :item/flagged true])]`))
				require.NoError(t, err, "or-join should inherit outer binding for ?container")
				// Items 1 (item flagged), 2 (container A flagged), 3 (item flagged), 4 (item flagged)
				// Container A is flagged, so all its items (1, 2) match via either branch.
				assert.GreaterOrEqual(t, len(result), 3,
					"should find items where either the item or container is flagged")
			})
		})
	}
}
