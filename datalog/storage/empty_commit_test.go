package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestAnEmptyCommitWritesNoMarkAndAnswersTheStoresHighestElementID pins that
// a commit of a transaction holding nothing writes no datom and returns the
// highest ElementID the store holds, at which AsOf answers what the latest
// view answers.
func TestAnEmptyCommitWritesNoMarkAndAnswersTheStoresHighestElementID(t *testing.T) {
	name := datalog.NewKeyword(":empty-commit/name")
	alice := datalog.NewIdentity("empty-commit:alice")
	const q = `[:find ?e ?v :where [?e :empty-commit/name ?v]]`

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)
			before, err := db.Store().MaxElementID()
			require.NoError(t, err)

			point, err := db.NewTransaction().Commit()
			require.NoError(t, err)
			after, err := db.Store().MaxElementID()
			require.NoError(t, err)
			assert.Equal(t, before, after, "an empty commit writes no datom")
			assert.Equal(t, before, point, "an empty commit answers the store's highest ElementID")

			latest, err := executor.CollectTuples(db.Query(q))
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{alice, "Alice"}}, latest)
			asOf, err := executor.CollectTuples(db.AsOf(point).Query(q))
			require.NoError(t, err)
			assert.ElementsMatch(t, latest, asOf, "AsOf the empty commit's point answers what the latest view answers")
		})
	}
}

// TestAnEmptyCommitOnAnEmptyStoreAnswersTheZeroElementID pins that an empty
// commit on a store holding nothing writes nothing and returns the zero
// ElementID, at which AsOf answers nothing.
func TestAnEmptyCommitOnAnEmptyStoreAnswersTheZeroElementID(t *testing.T) {
	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			point, err := db.NewTransaction().Commit()
			require.NoError(t, err)
			assert.Equal(t, datalog.ElementID{}, point, "an empty commit on an empty store answers the zero ElementID")
			highest, err := db.Store().MaxElementID()
			require.NoError(t, err)
			assert.Equal(t, datalog.ElementID{}, highest, "an empty commit writes no datom")

			got, err := executor.CollectTuples(db.AsOf(point).Query(`[:find ?e ?a ?v :where [?e ?a ?v]]`))
			require.NoError(t, err)
			assert.Empty(t, got, "AsOf the zero ElementID answers nothing")
		})
	}
}

// TestCommittingAStructTheViewAlreadyHoldsWritesNothing pins that a
// transaction re-saving the state its view holds commits no datom, so a
// writer asking whether anything was written after the previous commit's
// point is answered no.
func TestCommittingAStructTheViewAlreadyHoldsWritesNothing(t *testing.T) {
	type named struct {
		ID   datalog.Identity `datalog:"-,id"`
		Name string           `datalog:"empty-commit/name"`
	}
	alice := datalog.NewIdentity("empty-commit:saved")

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			_, err := tx.SaveStruct(&named{ID: alice, Name: "Alice"})
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)
			before, err := db.Store().MaxElementID()
			require.NoError(t, err)

			tx = db.NewTransaction()
			_, err = tx.SaveStruct(&named{ID: alice, Name: "Alice"})
			require.NoError(t, err)
			point, err := tx.Commit()
			require.NoError(t, err)
			after, err := db.Store().MaxElementID()
			require.NoError(t, err)
			assert.Equal(t, before, after, "re-saving the state the view holds writes no datom")
			assert.Equal(t, before, point, "the commit answers the store's highest ElementID")
		})
	}
}

// TestAnEmptyCommitClosesItsTransactionAndReleasesItFromARewind pins that an
// empty commit closes its transaction, so a second Commit is refused, and
// releases it from the database's open transactions, so a rewind begun after
// it finds no write in flight.
func TestAnEmptyCommitClosesItsTransactionAndReleasesItFromARewind(t *testing.T) {
	name := datalog.NewKeyword(":empty-commit/name")
	alice := datalog.NewIdentity("empty-commit:rewound")

	for _, mode := range backendModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)
			_, err = db.Snapshot("before-the-empty-commit")
			require.NoError(t, err)

			empty := db.NewTransaction()
			_, err = empty.Commit()
			require.NoError(t, err)
			_, err = empty.Commit()
			require.ErrorContains(t, err, "transaction is closed", "a committed empty transaction is closed")

			waited := make(chan struct{}, 1)
			db.onDrainWait = func() {
				select {
				case waited <- struct{}{}:
				default:
				}
			}
			rewound := make(chan error, 1)
			go func() { rewound <- db.TruncateTo("before-the-empty-commit") }()
			select {
			case err := <-rewound:
				require.NoError(t, err)
			case <-waited:
				t.Fatal("the rewind found the committed empty transaction in flight")
			}
		})
	}
}
