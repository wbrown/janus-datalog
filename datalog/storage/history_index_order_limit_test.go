package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

const (
	historyOrderEntities = 100
	historyOrderVersions = 100
	historyOrderDatoms   = historyOrderEntities * historyOrderVersions
)

type historyOrderScanCapture struct {
	mu      sync.Mutex
	scanned int
	index   string
}

func (c *historyOrderScanCapture) handler(event annotations.Event) {
	if event.Name != annotations.StorageScanComplete {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Intake, not resolution's output: the assertions this capture drives are
	// about how far the scan walked before the limit was satisfied.
	if scanned, ok := event.Data[annotations.KeyDatomsScanned].(int); ok {
		c.scanned += scanned
	}
	// The producer carries the IndexType; rendering here keeps "" meaning no
	// scan was seen, which reset() and the assertions below rely on.
	if index, ok := event.Data[annotations.KeyIndex].(IndexType); ok {
		c.index = index.String()
	}
}

func (c *historyOrderScanCapture) reset() {
	c.mu.Lock()
	c.scanned = 0
	c.index = ""
	c.mu.Unlock()
}

func (c *historyOrderScanCapture) snapshot() (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scanned, c.index
}

// openHistoryOrderDatabase seeds the multi-entity history fixture on the mode's
// backend. capture may be nil.
func openHistoryOrderDatabase(
	tb testing.TB,
	mode optimizerMode,
	capture *historyOrderScanCapture,
) *Database {
	tb.Helper()
	valueAttr := datalog.NewKeyword(":event/value")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       valueAttr,
		ValueType:   schema.TypeLong,
		Cardinality: schema.CardinalityOne,
	})

	options := DatabaseOptions{Schema: s}
	if capture != nil {
		options.AnnotationHandler = capture.handler
	}
	db := createOptimizerModeDB(tb, mode, options)

	entities := make([]datalog.Identity, historyOrderEntities)
	for i := range entities {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("history-order:%d", i))
	}
	for version := 0; version < historyOrderVersions; version++ {
		tx := db.NewTransaction()
		for _, entity := range entities {
			require.NoError(tb, tx.Set(entity, valueAttr, int64(version)))
		}
		_, err := tx.Commit()
		require.NoError(tb, err)
	}
	return db
}

// openHistoryEntityOrderDatabase seeds the single-entity, multi-attribute
// history fixture on the mode's backend. capture may be nil.
func openHistoryEntityOrderDatabase(
	tb testing.TB,
	mode optimizerMode,
	capture *historyOrderScanCapture,
) (*Database, datalog.Identity) {
	tb.Helper()
	attributes := make([]datalog.Keyword, historyOrderEntities)
	s := schema.NewSchema()
	for i := range attributes {
		attributes[i] = datalog.NewKeyword(fmt.Sprintf(":event/value-%03d", i))
		s.Add(&schema.AttributeDefinition{
			Ident:       attributes[i],
			ValueType:   schema.TypeLong,
			Cardinality: schema.CardinalityOne,
		})
	}

	options := DatabaseOptions{Schema: s}
	if capture != nil {
		options.AnnotationHandler = capture.handler
	}
	db := createOptimizerModeDB(tb, mode, options)

	entity := datalog.NewIdentity("history-entity-order")
	for version := 0; version < historyOrderVersions; version++ {
		tx := db.NewTransaction()
		for _, attribute := range attributes {
			require.NoError(tb, tx.Set(entity, attribute, int64(version)))
		}
		_, err := tx.Commit()
		require.NoError(tb, err)
	}
	return db, entity
}

func historyOrderedLimitQuery(limit int) string {
	return fmt.Sprintf(
		`[:find ?e ?v ?tx
		  :where [?e :event/value ?v ?tx]
		  :order-by [[?tx :desc] [?e :asc]]
		  :limit %d]`,
		limit,
	)
}

func historyTransactionOrderedLimitQuery(limit int) string {
	return fmt.Sprintf(
		`[:find ?e ?a ?v ?tx
		  :where [?e ?a ?v ?tx]
		  :order-by [[?tx :desc] [?a :asc] [?e :asc]]
		  :limit %d]`,
		limit,
	)
}

func historyEntityOrderedLimitQuery(limit int) string {
	return fmt.Sprintf(
		`[:find ?e ?v ?tx
		  :where [?e :event/value ?v ?tx]
		  :order-by [[?e :asc] [?tx :desc]]
		  :limit %d]`,
		limit,
	)
}

func historyAttributeOrderedLimitQuery(entity datalog.Identity, limit int) string {
	return fmt.Sprintf(
		`[:find ?a ?v ?tx
		  :where [#identity %q ?a ?v ?tx]
		  :order-by [[?a :asc] [?tx :desc]]
		  :limit %d]`,
		entity.L85(),
		limit,
	)
}

func TestHistoryOrderedLimitUsesATEV(t *testing.T) {
	const limit = 10
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db := openHistoryOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.History().Query(historyOrderedLimitQuery(limit))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, limit)
			for i := 1; i < len(tuples); i++ {
				previousTx, ok := datalog.DerefElementID(tuples[i-1][2])
				require.True(t, ok)
				currentTx, ok := datalog.DerefElementID(tuples[i][2])
				require.True(t, ok)
				require.GreaterOrEqual(t, previousTx.Compare(currentTx), 0)
				if previousTx.Equal(currentTx) {
					previousEntity := tuples[i-1][0].(datalog.Identity)
					currentEntity := tuples[i][0].(datalog.Identity)
					require.LessOrEqual(t, previousEntity.Compare(currentEntity), 0)
				}
			}

			scanned, index := capture.snapshot()
			require.Equal(t, "ATEV", index)
			require.LessOrEqual(t, scanned, limit,
				"history ATEV ordering must stop after the requested tuples")
		})
	}
}

func TestLatestOrderedLimitDeclinesATEV(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db := openHistoryOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.Query(historyOrderedLimitQuery(10))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 10)

			_, index := capture.snapshot()
			require.NotEqual(t, "ATEV", index,
				"latest CRDT resolution must not use Tx-primary ATEV")
		})
	}
}

func TestHistoryTransactionOrderedLimitUsesTAEV(t *testing.T) {
	const limit = 10
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db := openHistoryOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.History().Query(historyTransactionOrderedLimitQuery(limit))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, limit)
			for i := 1; i < len(tuples); i++ {
				previousTx, ok := datalog.DerefElementID(tuples[i-1][3])
				require.True(t, ok)
				currentTx, ok := datalog.DerefElementID(tuples[i][3])
				require.True(t, ok)
				require.GreaterOrEqual(t, previousTx.Compare(currentTx), 0)
				if previousTx.Equal(currentTx) {
					previousAttr := tuples[i-1][1].(datalog.Keyword)
					currentAttr := tuples[i][1].(datalog.Keyword)
					require.LessOrEqual(t, previousAttr.Compare(currentAttr), 0)
					if previousAttr.Compare(currentAttr) == 0 {
						previousEntity := tuples[i-1][0].(datalog.Identity)
						currentEntity := tuples[i][0].(datalog.Identity)
						require.LessOrEqual(t, previousEntity.Compare(currentEntity), 0)
					}
				}
			}

			scanned, index := capture.snapshot()
			require.Equal(t, "TAEV", index)
			require.LessOrEqual(t, scanned, limit,
				"history TAEV ordering must stop after the requested tuples")
		})
	}
}

func TestLatestTransactionOrderedLimitDeclinesTAEV(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db := openHistoryOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.Query(historyTransactionOrderedLimitQuery(10))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 10)

			_, index := capture.snapshot()
			require.NotEqual(t, "TAEV", index,
				"latest CRDT resolution must not use global Tx-primary TAEV")
		})
	}
}

func TestHistoryEntityOrderedLimitUsesAETV(t *testing.T) {
	const limit = 10
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db := openHistoryOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.History().Query(historyEntityOrderedLimitQuery(limit))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, limit)
			for i := 1; i < len(tuples); i++ {
				previousEntity := tuples[i-1][0].(datalog.Identity)
				currentEntity := tuples[i][0].(datalog.Identity)
				require.LessOrEqual(t, previousEntity.Compare(currentEntity), 0)
				if previousEntity.Equal(currentEntity) {
					previousTx, ok := datalog.DerefElementID(tuples[i-1][2])
					require.True(t, ok)
					currentTx, ok := datalog.DerefElementID(tuples[i][2])
					require.True(t, ok)
					require.GreaterOrEqual(t, previousTx.Compare(currentTx), 0)
				}
			}

			scanned, index := capture.snapshot()
			require.Equal(t, "AETV", index)
			require.LessOrEqual(t, scanned, limit,
				"history AETV ordering must stop after the requested tuples")
		})
	}
}

func TestLatestEntityOrderedLimitDoesNotUseHistoryAETVProperty(t *testing.T) {
	const limit = 10
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db := openHistoryOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.Query(historyEntityOrderedLimitQuery(limit))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, limit)

			scanned, index := capture.snapshot()
			require.Equal(t, "AETV", index)
			require.Greater(t, scanned, limit,
				"latest CRDT resolution must not inherit raw-history early termination")
		})
	}
}

func TestHistoryAttributeOrderedLimitUsesEATV(t *testing.T) {
	const limit = 10
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db, entity := openHistoryEntityOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.History().Query(historyAttributeOrderedLimitQuery(entity, limit))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, limit)
			for i := 1; i < len(tuples); i++ {
				previousAttr := tuples[i-1][0].(datalog.Keyword)
				currentAttr := tuples[i][0].(datalog.Keyword)
				require.LessOrEqual(t, previousAttr.Compare(currentAttr), 0)
				if previousAttr.Compare(currentAttr) == 0 {
					previousTx, ok := datalog.DerefElementID(tuples[i-1][2])
					require.True(t, ok)
					currentTx, ok := datalog.DerefElementID(tuples[i][2])
					require.True(t, ok)
					require.GreaterOrEqual(t, previousTx.Compare(currentTx), 0)
				}
			}

			scanned, index := capture.snapshot()
			require.Equal(t, "EATV", index)
			require.LessOrEqual(t, scanned, limit,
				"history EATV ordering must stop after the requested tuples")
		})
	}
}

func TestLatestAttributeOrderedLimitDoesNotUseHistoryEATVProperty(t *testing.T) {
	const limit = 10
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			capture := &historyOrderScanCapture{}
			db, entity := openHistoryEntityOrderDatabase(t, mode, capture)

			capture.reset()
			result, err := db.Query(historyAttributeOrderedLimitQuery(entity, limit))
			require.NoError(t, err)
			tuples, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, limit)

			scanned, index := capture.snapshot()
			require.Equal(t, "EATV", index)
			require.Greater(t, scanned, limit,
				"latest CRDT resolution must not inherit raw-history early termination")
		})
	}
}

func TestHistoryOrderedLimitDifferentialRandomized(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			runHistoryOrderedLimitDifferentialRandomized(t, mode)
		})
	}
}

func runHistoryOrderedLimitDifferentialRandomized(t *testing.T, mode optimizerMode) {
	random := rand.New(rand.NewSource(0x1d85))
	capture := &historyOrderScanCapture{}
	db := openHistoryOrderDatabase(t, mode, capture)
	entityDB, entity := openHistoryEntityOrderDatabase(t, mode, capture)

	type shape struct {
		name      string
		database  *Database
		fullQuery string
		limited   func(int) string
		less      func(left, right []interface{}) bool
	}
	shapes := []shape{
		{
			name:      "ATEV",
			database:  db.History(),
			fullQuery: `[:find ?e ?v ?tx :where [?e :event/value ?v ?tx]]`,
			limited:   historyOrderedLimitQuery,
			less: func(left, right []interface{}) bool {
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				if comparison := leftTx.Compare(rightTx); comparison != 0 {
					return comparison > 0
				}
				return left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)) < 0
			},
		},
		{
			name:      "TAEV",
			database:  db.History(),
			fullQuery: `[:find ?e ?a ?v ?tx :where [?e ?a ?v ?tx]]`,
			limited:   historyTransactionOrderedLimitQuery,
			less: func(left, right []interface{}) bool {
				leftTx, _ := datalog.DerefElementID(left[3])
				rightTx, _ := datalog.DerefElementID(right[3])
				if comparison := leftTx.Compare(rightTx); comparison != 0 {
					return comparison > 0
				}
				if comparison := left[1].(datalog.Keyword).Compare(right[1].(datalog.Keyword)); comparison != 0 {
					return comparison < 0
				}
				return left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)) < 0
			},
		},
		{
			name:      "AETV",
			database:  db.History(),
			fullQuery: `[:find ?e ?v ?tx :where [?e :event/value ?v ?tx]]`,
			limited:   historyEntityOrderedLimitQuery,
			less: func(left, right []interface{}) bool {
				if comparison := left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)); comparison != 0 {
					return comparison < 0
				}
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				return leftTx.Compare(rightTx) > 0
			},
		},
		{
			name:     "EATV",
			database: entityDB.History(),
			fullQuery: fmt.Sprintf(
				`[:find ?a ?v ?tx :where [#identity %q ?a ?v ?tx]]`,
				entity.L85(),
			),
			limited: func(limit int) string {
				return historyAttributeOrderedLimitQuery(entity, limit)
			},
			less: func(left, right []interface{}) bool {
				if comparison := left[0].(datalog.Keyword).Compare(right[0].(datalog.Keyword)); comparison != 0 {
					return comparison < 0
				}
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				return leftTx.Compare(rightTx) > 0
			},
		},
	}

	for _, testShape := range shapes {
		t.Run(testShape.name, func(t *testing.T) {
			reference, err := testShape.database.Query(testShape.fullQuery)
			require.NoError(t, err)
			allTuples, err := executor.CollectTuples(reference, nil)
			require.NoError(t, err)
			sort.Slice(allTuples, func(i, j int) bool {
				return testShape.less(allTuples[i], allTuples[j])
			})

			limits := []int{0, 1, 2, 10, 99, 100, len(allTuples), len(allTuples) + 1}
			for i := 0; i < 20; i++ {
				limits = append(limits, random.Intn(len(allTuples)+2))
			}
			for _, limit := range limits {
				capture.reset()
				result, err := testShape.database.Query(testShape.limited(limit))
				require.NoError(t, err, "limit %d", limit)
				actual, err := executor.CollectTuples(result, nil)
				require.NoError(t, err, "limit %d", limit)
				wantCount := limit
				if wantCount > len(allTuples) {
					wantCount = len(allTuples)
				}
				requireHistoryTuplesEqual(t, allTuples[:wantCount], actual, testShape.name, limit)
				scanned, index := capture.snapshot()
				if limit == 0 {
					require.Zero(t, scanned)
					require.Empty(t, index)
					continue
				}
				require.Equal(t, testShape.name, index, "limit %d", limit)
				require.LessOrEqual(t, scanned, wantCount, "limit %d", limit)
			}
		})
	}
}

func TestHistoryOrderedLimitUsesFullElementIDAcrossReplicas(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			runHistoryOrderedLimitUsesFullElementIDAcrossReplicas(t, mode)
		})
	}
}

func runHistoryOrderedLimitUsesFullElementIDAcrossReplicas(t *testing.T, mode optimizerMode) {
	capture := &historyOrderScanCapture{}
	valueAttr := datalog.NewKeyword(":event/value")
	otherAttr := datalog.NewKeyword(":event/other")
	s := schema.NewSchema()
	for _, attribute := range []datalog.Keyword{valueAttr, otherAttr} {
		s.Add(&schema.AttributeDefinition{
			Ident:       attribute,
			ValueType:   schema.TypeLong,
			Cardinality: schema.CardinalityOne,
		})
	}
	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		Schema:            s,
		AnnotationHandler: capture.handler,
	})

	entities := []datalog.Identity{
		datalog.NewIdentity("replica-history-0"),
		datalog.NewIdentity("replica-history-1"),
		datalog.NewIdentity("replica-history-2"),
	}
	datoms := []datalog.Datom{
		{E: entities[0], A: valueAttr, V: int64(1), Tx: datalog.ElementID{Lamport: 10, ReplicaID: 1}},
		{E: entities[0], A: valueAttr, V: int64(2), Tx: datalog.ElementID{Lamport: 10, ReplicaID: 7}},
		{E: entities[0], A: otherAttr, V: int64(3), Tx: datalog.ElementID{Lamport: 10, ReplicaID: 3}},
		{E: entities[1], A: valueAttr, V: int64(4), Tx: datalog.ElementID{Lamport: 10, ReplicaID: 9}},
		{E: entities[2], A: valueAttr, V: int64(5), Tx: datalog.ElementID{Lamport: 10, ReplicaID: 5}},
		{E: entities[2], A: valueAttr, V: int64(6), Tx: datalog.ElementID{Lamport: 9, ReplicaID: 100}},
	}
	require.NoError(t, db.Store().Assert(datoms))

	type shape struct {
		name      string
		fullQuery string
		limited   string
		less      func(left, right []interface{}) bool
	}
	shapes := []shape{
		{
			name:      "ATEV",
			fullQuery: `[:find ?e ?v ?tx :where [?e :event/value ?v ?tx]]`,
			limited:   historyOrderedLimitQuery(3),
			less: func(left, right []interface{}) bool {
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				if comparison := leftTx.Compare(rightTx); comparison != 0 {
					return comparison > 0
				}
				return left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)) < 0
			},
		},
		{
			name:      "TAEV",
			fullQuery: `[:find ?e ?a ?v ?tx :where [?e ?a ?v ?tx]]`,
			limited:   historyTransactionOrderedLimitQuery(3),
			less: func(left, right []interface{}) bool {
				leftTx, _ := datalog.DerefElementID(left[3])
				rightTx, _ := datalog.DerefElementID(right[3])
				if comparison := leftTx.Compare(rightTx); comparison != 0 {
					return comparison > 0
				}
				if comparison := left[1].(datalog.Keyword).Compare(right[1].(datalog.Keyword)); comparison != 0 {
					return comparison < 0
				}
				return left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)) < 0
			},
		},
		{
			name:      "AETV",
			fullQuery: `[:find ?e ?v ?tx :where [?e :event/value ?v ?tx]]`,
			limited:   historyEntityOrderedLimitQuery(3),
			less: func(left, right []interface{}) bool {
				if comparison := left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)); comparison != 0 {
					return comparison < 0
				}
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				return leftTx.Compare(rightTx) > 0
			},
		},
		{
			name: "EATV",
			fullQuery: fmt.Sprintf(
				`[:find ?a ?v ?tx :where [#identity %q ?a ?v ?tx]]`,
				entities[0].L85(),
			),
			limited: historyAttributeOrderedLimitQuery(entities[0], 3),
			less: func(left, right []interface{}) bool {
				if comparison := left[0].(datalog.Keyword).Compare(right[0].(datalog.Keyword)); comparison != 0 {
					return comparison < 0
				}
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				return leftTx.Compare(rightTx) > 0
			},
		},
	}

	for _, testShape := range shapes {
		t.Run(testShape.name, func(t *testing.T) {
			reference, err := db.History().Query(testShape.fullQuery)
			require.NoError(t, err)
			expected, err := executor.CollectTuples(reference, nil)
			require.NoError(t, err)
			sort.Slice(expected, func(i, j int) bool {
				return testShape.less(expected[i], expected[j])
			})
			expected = expected[:3]

			capture.reset()
			result, err := db.History().Query(testShape.limited)
			require.NoError(t, err)
			actual, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			requireHistoryTuplesEqual(t, expected, actual, testShape.name, 3)
			scanned, index := capture.snapshot()
			require.Equal(t, testShape.name, index)
			require.LessOrEqual(t, scanned, 3)
		})
	}
}

func requireHistoryTuplesEqual(
	t *testing.T,
	expected, actual [][]interface{},
	shape string,
	limit int,
) {
	t.Helper()
	require.Len(t, actual, len(expected), "%s limit %d", shape, limit)
	for tupleIndex := range expected {
		require.Len(t, actual[tupleIndex], len(expected[tupleIndex]), "%s limit %d tuple %d", shape, limit, tupleIndex)
		for tuplePosition := range expected[tupleIndex] {
			require.True(t,
				datalog.ValuesEqual(expected[tupleIndex][tuplePosition], actual[tupleIndex][tuplePosition]),
				"%s limit %d tuple %d tuple position %d: expected %v, got %v",
				shape, limit, tupleIndex, tuplePosition,
				expected[tupleIndex][tuplePosition], actual[tupleIndex][tuplePosition],
			)
		}
	}
}

func TestAsOfOrderedLimitDifferentialAroundTombstone(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			runAsOfOrderedLimitDifferentialAroundTombstone(t, mode)
		})
	}
}

func runAsOfOrderedLimitDifferentialAroundTombstone(t *testing.T, mode optimizerMode) {
	capture := &historyOrderScanCapture{}
	attributeA := datalog.NewKeyword(":boundary/a")
	attributeB := datalog.NewKeyword(":boundary/b")
	s := schema.NewSchema()
	for _, attribute := range []datalog.Keyword{attributeA, attributeB} {
		s.Add(&schema.AttributeDefinition{
			Ident:       attribute,
			ValueType:   schema.TypeLong,
			Cardinality: schema.CardinalityOne,
		})
	}
	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		Schema:            s,
		AnnotationHandler: capture.handler,
	})

	entities := []datalog.Identity{
		datalog.NewIdentity("boundary-0"),
		datalog.NewIdentity("boundary-1"),
		datalog.NewIdentity("boundary-2"),
		datalog.NewIdentity("boundary-3"),
	}
	tx := db.NewTransaction()
	for entityIndex, entity := range entities {
		require.NoError(t, tx.Set(entity, attributeA, int64(entityIndex)))
		require.NoError(t, tx.Set(entity, attributeB, int64(100+entityIndex)))
	}
	first, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	for entityIndex, entity := range entities {
		require.NoError(t, tx.Set(entity, attributeA, int64(10+entityIndex)))
	}
	second, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Remove(entities[0], attributeA, int64(10)))
	require.NoError(t, tx.Remove(entities[0], attributeB, int64(100)))
	third, err := tx.Commit()
	require.NoError(t, err)

	type asOfShape struct {
		name      string
		fullQuery string
		limited   string
		less      func(left, right []interface{}) bool
	}
	shapes := []asOfShape{
		{
			name: "ATEV",
			fullQuery: fmt.Sprintf(
				`[:find ?e ?v ?tx :where [?e %s ?v ?tx]]`,
				attributeA,
			),
			limited: fmt.Sprintf(
				`[:find ?e ?v ?tx :where [?e %s ?v ?tx]
				 :order-by [[?tx :desc] [?e :asc]] :limit 1]`,
				attributeA,
			),
			less: func(left, right []interface{}) bool {
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				if comparison := leftTx.Compare(rightTx); comparison != 0 {
					return comparison > 0
				}
				return left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)) < 0
			},
		},
		{
			name:      "TAEV",
			fullQuery: `[:find ?e ?a ?v ?tx :where [?e ?a ?v ?tx]]`,
			limited: `[:find ?e ?a ?v ?tx :where [?e ?a ?v ?tx]
			          :order-by [[?tx :desc] [?a :asc] [?e :asc]] :limit 1]`,
			less: func(left, right []interface{}) bool {
				leftTx, _ := datalog.DerefElementID(left[3])
				rightTx, _ := datalog.DerefElementID(right[3])
				if comparison := leftTx.Compare(rightTx); comparison != 0 {
					return comparison > 0
				}
				if comparison := left[1].(datalog.Keyword).Compare(right[1].(datalog.Keyword)); comparison != 0 {
					return comparison < 0
				}
				return left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)) < 0
			},
		},
		{
			name: "AETV",
			fullQuery: fmt.Sprintf(
				`[:find ?e ?v ?tx :where [?e %s ?v ?tx]]`,
				attributeA,
			),
			limited: fmt.Sprintf(
				`[:find ?e ?v ?tx :where [?e %s ?v ?tx]
				 :order-by [[?e :asc] [?tx :desc]] :limit 1]`,
				attributeA,
			),
			less: func(left, right []interface{}) bool {
				if comparison := left[0].(datalog.Identity).Compare(right[0].(datalog.Identity)); comparison != 0 {
					return comparison < 0
				}
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				return leftTx.Compare(rightTx) > 0
			},
		},
		{
			name: "EATV",
			fullQuery: fmt.Sprintf(
				`[:find ?a ?v ?tx :where [#identity %q ?a ?v ?tx]]`,
				entities[1].L85(),
			),
			limited: fmt.Sprintf(
				`[:find ?a ?v ?tx :where [#identity %q ?a ?v ?tx]
				 :order-by [[?a :asc] [?tx :desc]] :limit 1]`,
				entities[1].L85(),
			),
			less: func(left, right []interface{}) bool {
				if comparison := left[0].(datalog.Keyword).Compare(right[0].(datalog.Keyword)); comparison != 0 {
					return comparison < 0
				}
				leftTx, _ := datalog.DerefElementID(left[2])
				rightTx, _ := datalog.DerefElementID(right[2])
				return leftTx.Compare(rightTx) > 0
			},
		},
	}

	for snapshotIndex, snapshot := range []datalog.ElementID{first, second, third} {
		view := db.AsOf(snapshot)
		for _, testShape := range shapes {
			t.Run(fmt.Sprintf("snapshot_%d/%s", snapshotIndex, testShape.name), func(t *testing.T) {
				reference, err := view.Query(testShape.fullQuery)
				require.NoError(t, err)
				expected, err := executor.CollectTuples(reference, nil)
				require.NoError(t, err)
				sort.Slice(expected, func(i, j int) bool {
					return testShape.less(expected[i], expected[j])
				})
				if len(expected) > 1 {
					expected = expected[:1]
				}

				capture.reset()
				result, err := view.Query(testShape.limited)
				require.NoError(t, err)
				actual, err := executor.CollectTuples(result, nil)
				require.NoError(t, err)
				requireHistoryTuplesEqual(t, expected, actual, testShape.name, 1)
				scanned, _ := capture.snapshot()
				require.Greater(t, scanned, 1,
					"as-of CRDT resolution must decline raw-history early termination")
			})
		}
	}
}

func TestHistoryATEVPropertiesRequireSafeDatalogShape(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	tx := datalog.NewSymbol("?tx")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":event/value")},
		query.Variable{Name: datalog.NewSymbol("?v")},
		query.Variable{Name: tx},
	}}
	limit := 10
	q := &query.Query{
		Where: []query.Clause{pattern},
		OrderBy: []query.OrderByClause{
			{Variable: tx, Descending: true},
			{Variable: entity, Descending: false},
		},
		Limit: &limit,
	}
	fragmentPattern, err := q.SingleDataPattern()
	require.NoError(t, err)
	require.Same(t, pattern, fragmentPattern)

	properties, ok := historyATEVProperties(q, pattern, true)
	require.True(t, ok)
	require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)

	_, ok = historyATEVProperties(q, pattern, false)
	require.False(t, ok, "latest/as-of modes must decline Tx-primary ATEV")

	q.OrderBy[0].Descending = false
	_, ok = historyATEVProperties(q, pattern, true)
	require.False(t, ok, "forward ATEV cannot satisfy ascending Tx")
}

func TestHistoryTAEVPropertiesRequireSafeDatalogShape(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	attribute := datalog.NewSymbol("?a")
	value := datalog.NewSymbol("?v")
	tx := datalog.NewSymbol("?tx")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Variable{Name: attribute},
		query.Variable{Name: value},
		query.Variable{Name: tx},
	}}
	limit := 10
	q := &query.Query{
		Where: []query.Clause{pattern},
		OrderBy: []query.OrderByClause{
			{Variable: tx, Descending: true},
			{Variable: attribute, Descending: false},
			{Variable: entity, Descending: false},
		},
		Limit: &limit,
	}

	properties, ok := historyTAEVProperties(q, pattern, true)
	require.True(t, ok)
	require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)

	fullOrder := append([]query.OrderByClause(nil), q.OrderBy...)
	for prefixLength := 1; prefixLength <= len(fullOrder); prefixLength++ {
		q.OrderBy = fullOrder[:prefixLength]
		properties, ok = historyTAEVProperties(q, pattern, true)
		require.True(t, ok, "physical TAEV order prefix of length %d should be accepted", prefixLength)
		require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)
	}
	q.OrderBy = fullOrder

	_, ok = historyTAEVProperties(q, pattern, false)
	require.False(t, ok, "latest/as-of modes must decline global Tx-primary TAEV")

	q.Limit = nil
	_, ok = historyTAEVProperties(q, pattern, true)
	require.False(t, ok, "TAEV order is useful for finalization only with a limit")
	q.Limit = &limit

	q.OrderBy[0].Descending = false
	_, ok = historyTAEVProperties(q, pattern, true)
	require.False(t, ok, "forward TAEV cannot satisfy ascending Tx")
	q.OrderBy[0].Descending = true

	pattern.Elements[1] = query.Constant{Value: datalog.NewKeyword(":event/value")}
	_, ok = historyTAEVProperties(q, pattern, true)
	require.False(t, ok, "filtered patterns are outside the exact-N global TAEV shape")
}

func TestHistoryAETVPropertiesRequireSafeDatalogShape(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	value := datalog.NewSymbol("?v")
	tx := datalog.NewSymbol("?tx")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":event/value")},
		query.Variable{Name: value},
		query.Variable{Name: tx},
	}}
	limit := 10
	q := &query.Query{
		Where: []query.Clause{pattern},
		OrderBy: []query.OrderByClause{
			{Variable: entity, Descending: false},
			{Variable: tx, Descending: true},
		},
		Limit: &limit,
	}

	properties, ok := historyAETVProperties(q, pattern, true)
	require.True(t, ok)
	require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)

	_, ok = historyAETVProperties(q, pattern, false)
	require.False(t, ok, "latest/as-of modes must not inherit raw AETV history ordering")

	q.Limit = nil
	_, ok = historyAETVProperties(q, pattern, true)
	require.False(t, ok, "AETV order is useful for finalization only with a limit")
	q.Limit = &limit

	q.OrderBy = q.OrderBy[:1]
	_, ok = historyAETVProperties(q, pattern, true)
	require.False(t, ok, "entity alone is not a total history order across versions")
	q.OrderBy = []query.OrderByClause{
		{Variable: entity, Descending: false},
		{Variable: tx, Descending: false},
	}
	_, ok = historyAETVProperties(q, pattern, true)
	require.False(t, ok, "forward AETV cannot satisfy ascending Tx within an entity")

	pattern.Elements[2] = query.Constant{Value: int64(42)}
	q.OrderBy[1].Descending = true
	_, ok = historyAETVProperties(q, pattern, true)
	require.False(t, ok, "filtered values are outside the exact-N AETV shape")
}

func TestHistoryEATVPropertiesRequireSafeDatalogShape(t *testing.T) {
	attribute := datalog.NewSymbol("?a")
	value := datalog.NewSymbol("?v")
	tx := datalog.NewSymbol("?tx")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Constant{Value: datalog.NewIdentity("history-entity")},
		query.Variable{Name: attribute},
		query.Variable{Name: value},
		query.Variable{Name: tx},
	}}
	limit := 10
	q := &query.Query{
		Where: []query.Clause{pattern},
		OrderBy: []query.OrderByClause{
			{Variable: attribute, Descending: false},
			{Variable: tx, Descending: true},
		},
		Limit: &limit,
	}

	properties, ok := historyEATVProperties(q, pattern, true)
	require.True(t, ok)
	require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)

	_, ok = historyEATVProperties(q, pattern, false)
	require.False(t, ok, "latest/as-of modes must not inherit raw EATV history ordering")

	q.Limit = nil
	_, ok = historyEATVProperties(q, pattern, true)
	require.False(t, ok, "EATV order is useful for finalization only with a limit")
	q.Limit = &limit

	q.OrderBy = q.OrderBy[:1]
	_, ok = historyEATVProperties(q, pattern, true)
	require.False(t, ok, "attribute alone is not a total history order across versions")
	q.OrderBy = []query.OrderByClause{
		{Variable: attribute, Descending: false},
		{Variable: tx, Descending: false},
	}
	_, ok = historyEATVProperties(q, pattern, true)
	require.False(t, ok, "forward EATV cannot satisfy ascending Tx within an attribute")

	pattern.Elements[2] = query.Constant{Value: int64(42)}
	q.OrderBy[1].Descending = true
	_, ok = historyEATVProperties(q, pattern, true)
	require.False(t, ok, "filtered values are outside the exact-N EATV shape")
}
