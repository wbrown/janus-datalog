package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A scan that fails mid-stream must surface at every public boundary rather
// than read as an empty or not-found success. That is the property
// BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES names, and it belongs to
// every backend.
//
// Inducing the failure does not. The blob case deletes a Tier-3 blob out from
// under a value, which reproduces the original defect exactly and needs a store
// that keeps blobs; the injected case wraps any store and fails its iterator on
// demand, which needs nothing. Each case therefore names the modes it runs on,
// and running the boundary assertions over both is what puts the property on
// every backend without giving up the reproduction.

var errInjectedScanFault = errors.New("injected scan fault")

// scanFaultStore fails the iterators it hands out, once armed. Setup writes
// through it unarmed, so only the reads under test fail.
type scanFaultStore struct {
	Store
	armed     bool
	failAfter int
}

func (s *scanFaultStore) arm() { s.armed = true }

func (s *scanFaultStore) wrap(it Iterator, err error) (Iterator, error) {
	if err != nil || !s.armed {
		return it, err
	}
	return &scanFaultIterator{Iterator: it, remaining: s.failAfter}, nil
}

func (s *scanFaultStore) Scan(bound ScanBound) (Iterator, error) {
	return s.wrap(s.Store.Scan(bound))
}

func (s *scanFaultStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.wrap(s.Store.ScanKeysOnly(bound))
}

func (s *scanFaultStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if err != nil {
		return nil, err
	}
	return &scanFaultSession{ReadSession: session, store: s}, nil
}

type scanFaultSession struct {
	ReadSession
	store *scanFaultStore
}

func (s *scanFaultSession) Scan(bound ScanBound) (Iterator, error) {
	return s.store.wrap(s.ReadSession.Scan(bound))
}

func (s *scanFaultSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.store.wrap(s.ReadSession.ScanKeysOnly(bound))
}

// scanFaultIterator yields remaining datoms and then fails the way a real
// storage failure does: Next reports false and the error arrives through
// Error(), stickily.
type scanFaultIterator struct {
	Iterator
	remaining int
	failed    bool
}

func (it *scanFaultIterator) Next() bool {
	if it.failed {
		return false
	}
	if it.remaining <= 0 {
		it.failed = true
		return false
	}
	if !it.Iterator.Next() {
		return false
	}
	it.remaining--
	return true
}

func (it *scanFaultIterator) Error() error {
	if it.failed {
		return errInjectedScanFault
	}
	return it.Iterator.Error()
}

// queryBoundaryFaultCase is one way of making a scan fail, plus what the
// resulting error says.
type queryBoundaryFaultCase struct {
	name string
	// modes is the axis this way of inducing a failure can be induced on.
	modes []optimizerMode
	// openFailing returns a database whose every scan of (entity, attr) fails
	// before yielding anything.
	openFailing func(t *testing.T, mode optimizerMode) (*Database, datalog.Identity, datalog.Keyword)
	// openValidThenFailing returns a database whose attribute-wide scan yields
	// one good datom and then fails, which is the truncation boundary.
	openValidThenFailing func(t *testing.T, mode optimizerMode) *Database
	// errText is a substring every boundary must carry through.
	errText string
}

func queryBoundaryFaultCases(t *testing.T) []queryBoundaryFaultCase {
	return appendBlobFaultCase(t, []queryBoundaryFaultCase{{
		name:  "injected",
		modes: optimizerModes,
		openFailing: func(t *testing.T, mode optimizerMode) (*Database, datalog.Identity, datalog.Keyword) {
			return openInjectedFaultDatabase(t, mode, 0)
		},
		openValidThenFailing: func(t *testing.T, mode optimizerMode) *Database {
			db, _, _ := openInjectedFaultDatabase(t, mode, 1)
			return db
		},
		errText: "injected scan fault",
	}})
}

// openInjectedFaultDatabase writes two datoms through an unarmed fault store,
// then arms it so every scan under test fails after failAfter datoms.
func openInjectedFaultDatabase(t *testing.T, mode optimizerMode, failAfter int) (*Database, datalog.Identity, datalog.Keyword) {
	t.Helper()

	base, err := mode.backend.Open(t.TempDir(), &BinaryKeyEncoder{})
	require.NoError(t, err)
	store := &scanFaultStore{Store: base, failAfter: failAfter}

	popts := mode.plannerOptions()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Store:          store,
		DisableCache:   true,
		PlannerOptions: &popts,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = base.Close()
	})

	entity := datalog.NewIdentity("doc-1")
	attr := datalog.NewKeyword(":doc/blob")
	other := datalog.NewIdentity("doc-2")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(entity, attr, []byte("first")))
	require.NoError(t, tx.Set(other, attr, []byte("second")))
	require.NoError(t, tx.Set(entity, datalog.NewKeyword(":doc/name"), "doc-one"))
	_, err = tx.Commit()
	require.NoError(t, err)

	store.arm()
	return db, entity, attr
}

// eachFaultAndMode runs body for every fault-injection case crossed with the
// modes that case can be induced on, which is the grid each boundary assertion
// below is pinned on.
func eachFaultAndMode(t *testing.T, body func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode)) {
	t.Helper()
	for _, fault := range queryBoundaryFaultCases(t) {
		t.Run(fault.name, func(t *testing.T) {
			for _, mode := range fault.modes {
				t.Run(mode.name, func(t *testing.T) {
					body(t, fault, mode)
				})
			}
		})
	}
}

// TestAnalyze_SurfacesScanFault: Analyze fully executes the query (EXPLAIN
// ANALYZE-style), so a deferred failure must surface from Analyze itself rather
// than be deferred past the API boundary into a lazy result.
func TestAnalyze_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := db.Analyze(`[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, entity)
		require.ErrorContains(t, err, fault.errText,
			"Analyze must surface a deferred error, not return a clean lazy result")
	})
}

func TestQueryInto_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		var out [][]byte
		err := db.QueryInto(&out, `[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, entity)
		require.ErrorContains(t, err, fault.errText,
			"a failed scan must not be reported as an empty slice")
	})
}

func TestQueryOneInto_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		var out []byte
		found, err := db.QueryOneInto(&out, `[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, entity)
		require.ErrorContains(t, err, fault.errText,
			"a failed scan must not be reported as found=false,nil")
		require.False(t, found)
	})
}

// TestQueryOrderBy_SurfacesScanFault: order-by materializes (Sort) the failing
// scan; the error must survive that transform to the boundary.
func TestQueryOrderBy_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(
			db.Query(`[:find ?v :in $ ?e :where [?e :doc/blob ?v] :order-by [?v]]`, entity))
		require.ErrorContains(t, err, fault.errText,
			"order-by over a failing scan must surface the error")
	})
}

// TestQueryAggregate_SurfacesScanFault: aggregation consumes the failing scan;
// the error must survive that transform to the boundary.
func TestQueryAggregate_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(
			db.Query(`[:find (count ?v) :in $ ?e :where [?e :doc/blob ?v]]`, entity))
		require.ErrorContains(t, err, fault.errText,
			"aggregate over a failing scan must surface the error")
	})
}

// TestQueryNot_SurfacesScanFault: the (not [?e :doc/blob ?v]) inner scan fails,
// which makes the inner relation look empty — NOT would then wrongly include
// the entity. The error must surface instead of producing a silently-wrong
// result. Both fixtures carry :doc/name so the outer pattern matches.
func TestQueryNot_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(db.Query(
			`[:find ?n :in $ ?e :where [?e :doc/name ?n] (not [?e :doc/blob ?v])]`,
			entity))
		require.ErrorContains(t, err, fault.errText,
			"a failed inner scan must surface, not silently un-exclude the entity")
	})
}

// TestQueryGroupedAggregate_SurfacesScanFault: a group-by var in :find routes
// through the grouped path; the failing scan's error must survive.
func TestQueryGroupedAggregate_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(
			db.Query(`[:find ?e (count ?v) :in $ ?e :where [?e :doc/blob ?v]]`, entity))
		require.ErrorContains(t, err, fault.errText,
			"grouped aggregate over a failing scan must surface the error")
	})
}

// TestQueryRelationInput_SurfacesScanFault: a RelationInput query iterates per
// input tuple and collects the per-tuple results; that collection must
// propagate a failing scan's error rather than drop it.
func TestQueryRelationInput_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(db.Query(
			`[:find ?v :in $ [[?e] ...] :where [?e :doc/blob ?v]]`,
			[][]any{{entity}}))
		require.ErrorContains(t, err, fault.errText,
			"relation-input iteration over a failing scan must surface the error")
	})
}

// TestQuerySubquery_SurfacesScanFault: a subquery whose inner scan fails must
// surface the error through subquery result combination.
func TestQuerySubquery_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(db.Query(
			`[:find ?v :in $ ?e
			  :where [(q [:find ?bv :in $ ?e2 :where [?e2 :doc/blob ?bv]] $ ?e) [[?v]]]]`,
			entity))
		require.ErrorContains(t, err, fault.errText,
			"subquery over a failing scan must surface the error")
	})
}

// TestQueryOr_SurfacesScanFault: an (or ...) branch whose scan fails must
// surface the error through the union of branch results.
func TestQueryOr_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(db.Query(
			`[:find ?v :in $ ?e :where (or [?e :doc/blob ?v] [?e :doc/missing ?v])]`,
			entity))
		require.ErrorContains(t, err, fault.errText,
			"OR branch over a failing scan must surface the error")
	})
}

// TestQueryMultiPhase_SurfacesScanFault: a two-pattern join over the failing
// value must surface the error rather than return empty. It propagates via the
// collapsed/streaming join path; it does NOT force the failing scan into a
// non-last phase's Keep projection in executor.go — that laundering site
// (Site 1) is not yet triggered and remains open.
func TestQueryMultiPhase_SurfacesScanFault(t *testing.T) {
	eachFaultAndMode(t, func(t *testing.T, fault queryBoundaryFaultCase, mode optimizerMode) {
		db, entity, _ := fault.openFailing(t, mode)
		defer db.Close()

		_, err := executor.CollectTuples(db.Query(
			`[:find ?e2 :in $ ?e :where [?e :doc/blob ?v] [?e2 :doc/blob ?v]]`,
			entity))
		require.ErrorContains(t, err, fault.errText,
			"multi-phase join over a failing scan must surface the error")
	})
}

func TestCollectTuples_SurfacesScanFault(t *testing.T) {
	for _, fault := range queryBoundaryFaultCases(t) {
		t.Run(fault.name, func(t *testing.T) {
			for _, mode := range fault.modes {
				t.Run(mode.name, func(t *testing.T) {
					db, entity, _ := fault.openFailing(t, mode)
					defer db.Close()

					_, err := executor.CollectTuples(
						db.Query(`[:find ?v :in $ ?e :where [?e :doc/blob ?v]]`, entity))
					require.ErrorContains(t, err, fault.errText,
						"a failed scan must not be reported as an empty result")
				})
			}
		})
	}
}
